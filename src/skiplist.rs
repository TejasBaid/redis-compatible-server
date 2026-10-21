//! A skiplist with rank spans: the ordered index behind large sorted sets.
//!
//! This is a faithful port of Redis' `zskiplist` (`t_zset.c`): elements are
//! ordered by `(score, member)`, every forward pointer carries a **span** (the
//! number of level-0 nodes it skips) so `ZRANK` and `ZRANGE` by index are
//! O(log n), and each node has a backward pointer for reverse iteration.
//!
//! Nodes live in an arena (`Vec<Node>`) addressed by `u32` indices instead of
//! raw pointers, which keeps the implementation 100% safe Rust while staying
//! cache-friendly. The first four levels are stored inline in the node; only
//! nodes taller than four levels (0.4%) carry a heap slice for the rest.

use crate::sds::Sds;
use std::cmp::Ordering;

const NIL: u32 = u32::MAX;
const HEAD: u32 = 0;
/// Maximum node height (enough for 2^64 elements at p = 1/4).
pub const MAX_LEVEL: usize = 32;
const P: u64 = 0xFFFF / 4;

#[derive(Clone, Copy, Debug)]
struct Level {
    forward: u32,
    span: u32,
}

const EMPTY_LEVEL: Level = Level {
    forward: NIL,
    span: 0,
};

/// Levels stored inline in every node. With p = 1/4, 99.6% of nodes are at
/// most this tall, so a traversal touches one cache line per node visited
/// instead of chasing a second pointer for the upper levels.
const INLINE_LEVELS: usize = 4;

/// Everything a traversal touches, in exactly one 64-byte cache line. The
/// member lives in a parallel vector and is only read on a score tie.
#[repr(C, align(64))]
struct Node {
    score: f64,
    backward: u32,
    height: u32,
    levels: [Level; INLINE_LEVELS],
    upper: Box<[Level]>,
}

impl Node {
    fn new(height: usize, score: f64) -> Node {
        Node {
            score,
            backward: NIL,
            height: height as u32,
            levels: [EMPTY_LEVEL; INLINE_LEVELS],
            upper: vec![EMPTY_LEVEL; height.saturating_sub(INLINE_LEVELS)].into_boxed_slice(),
        }
    }
    #[inline]
    fn level(&self, i: usize) -> &Level {
        if i < INLINE_LEVELS {
            &self.levels[i]
        } else {
            &self.upper[i - INLINE_LEVELS]
        }
    }
    #[inline]
    fn level_mut(&mut self, i: usize) -> &mut Level {
        if i < INLINE_LEVELS {
            &mut self.levels[i]
        } else {
            &mut self.upper[i - INLINE_LEVELS]
        }
    }
    fn height(&self) -> usize {
        self.height as usize
    }
}

/// A score interval for `ZRANGEBYSCORE`-style queries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoreRange {
    /// Lower bound.
    pub min: f64,
    /// Upper bound.
    pub max: f64,
    /// Lower bound is exclusive (`(min`).
    pub minex: bool,
    /// Upper bound is exclusive (`(max`).
    pub maxex: bool,
}

impl ScoreRange {
    /// True if `v` is at or above the lower bound.
    #[inline]
    pub fn gte_min(&self, v: f64) -> bool {
        if self.minex {
            v > self.min
        } else {
            v >= self.min
        }
    }
    /// True if `v` is at or below the upper bound.
    #[inline]
    pub fn lte_max(&self, v: f64) -> bool {
        if self.maxex {
            v < self.max
        } else {
            v <= self.max
        }
    }
    /// True if `v` is inside the range.
    pub fn contains(&self, v: f64) -> bool {
        self.gte_min(v) && self.lte_max(v)
    }
    /// True if the range cannot contain anything.
    pub fn is_empty(&self) -> bool {
        self.min > self.max || (self.min == self.max && (self.minex || self.maxex))
    }
}

/// One end of a lexicographic range (`-`, `+`, `[x`, `(x`).
#[derive(Clone, Debug, PartialEq)]
pub enum LexBound {
    /// `-`: below everything.
    NegInf,
    /// `+`: above everything.
    PosInf,
    /// `[x`: inclusive.
    Incl(Vec<u8>),
    /// `(x`: exclusive.
    Excl(Vec<u8>),
}

impl LexBound {
    /// Parses `-`, `+`, `[x` or `(x`.
    pub fn parse(s: &[u8]) -> Option<LexBound> {
        match s.first()? {
            b'-' if s.len() == 1 => Some(LexBound::NegInf),
            b'+' if s.len() == 1 => Some(LexBound::PosInf),
            b'[' => Some(LexBound::Incl(s[1..].to_vec())),
            b'(' => Some(LexBound::Excl(s[1..].to_vec())),
            _ => None,
        }
    }
}

/// A lexicographic interval for `ZRANGEBYLEX`-style queries.
#[derive(Clone, Debug, PartialEq)]
pub struct LexRange {
    /// Lower bound.
    pub min: LexBound,
    /// Upper bound.
    pub max: LexBound,
}

impl LexRange {
    /// True if `m` is at or above the lower bound.
    pub fn gte_min(&self, m: &[u8]) -> bool {
        match &self.min {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(x) => m >= &x[..],
            LexBound::Excl(x) => m > &x[..],
        }
    }
    /// True if `m` is at or below the upper bound.
    pub fn lte_max(&self, m: &[u8]) -> bool {
        match &self.max {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Incl(x) => m <= &x[..],
            LexBound::Excl(x) => m < &x[..],
        }
    }
    /// True if `m` is inside the range.
    pub fn contains(&self, m: &[u8]) -> bool {
        self.gte_min(m) && self.lte_max(m)
    }
    /// True if the range cannot contain anything.
    pub fn is_empty(&self) -> bool {
        use LexBound::*;
        match (&self.min, &self.max) {
            (PosInf, _) | (_, NegInf) => true,
            (NegInf, _) | (_, PosInf) => false,
            (Incl(a), Incl(b)) => a > b,
            (Incl(a), Excl(b)) | (Excl(a), Incl(b)) | (Excl(a), Excl(b)) => a >= b,
        }
    }
}

/// Compares `(score, member)` pairs the way sorted sets order them.
#[inline]
pub fn cmp_entry(s1: f64, m1: &[u8], s2: f64, m2: &[u8]) -> Ordering {
    match s1.partial_cmp(&s2) {
        Some(Ordering::Equal) | None => m1.cmp(m2),
        Some(o) => o,
    }
}

/// An opaque handle to a node, valid until the next mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeRef(u32);

/// The skiplist. See the module docs.
pub struct SkipList {
    nodes: Vec<Node>,
    /// `members[i]` belongs to `nodes[i]`.
    members: Vec<Sds>,
    free: Vec<u32>,
    level: usize,
    len: usize,
    tail: u32,
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

fn random_level() -> usize {
    let mut level = 1;
    while (crate::util::rand_u64() & 0xFFFF) < P && level < MAX_LEVEL {
        level += 1;
    }
    level
}

impl SkipList {
    /// An empty skiplist.
    pub fn new() -> Self {
        let head = Node::new(MAX_LEVEL, 0.0);
        SkipList {
            nodes: vec![head],
            members: vec![Sds::empty()],
            free: Vec::new(),
            level: 1,
            len: 0,
            tail: NIL,
        }
    }

    /// Number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Approximate heap bytes used.
    pub fn heap_bytes(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<Node>()
            + self.members.capacity() * std::mem::size_of::<Sds>()
            + self.members.iter().map(|m| m.heap_size()).sum::<usize>()
            + self
                .nodes
                .iter()
                .map(|n| n.upper.len() * std::mem::size_of::<Level>())
                .sum::<usize>()
    }

    #[inline]
    fn fwd(&self, x: u32, i: usize) -> u32 {
        self.nodes[x as usize].level(i).forward
    }
    #[inline]
    fn span(&self, x: u32, i: usize) -> u32 {
        self.nodes[x as usize].level(i).span
    }
    #[inline]
    fn lvl(&mut self, x: u32, i: usize) -> &mut Level {
        self.nodes[x as usize].level_mut(i)
    }
    #[inline]
    fn score_of(&self, x: u32) -> f64 {
        self.nodes[x as usize].score
    }
    #[inline]
    fn member_of(&self, x: u32) -> &[u8] {
        &self.members[x as usize]
    }

    /// Compares node `f` with `(score, member)`, reading the member (stored
    /// outside the node's cache line) only on a score tie.
    #[inline]
    fn cmp_node(&self, f: u32, score: f64, member: &[u8]) -> Ordering {
        let s = self.score_of(f);
        if s < score {
            Ordering::Less
        } else if s > score {
            Ordering::Greater
        } else {
            self.member_of(f).cmp(member)
        }
    }

    /// `(score, member)` of a node.
    pub fn entry(&self, n: NodeRef) -> (f64, &Sds) {
        (self.nodes[n.0 as usize].score, &self.members[n.0 as usize])
    }

    fn alloc(&mut self, height: usize, score: f64, member: Sds) -> u32 {
        let node = Node::new(height, score);
        if let Some(i) = self.free.pop() {
            self.nodes[i as usize] = node;
            self.members[i as usize] = member;
            i
        } else {
            self.nodes.push(node);
            self.members.push(member);
            (self.nodes.len() - 1) as u32
        }
    }

    fn release(&mut self, x: u32) -> Sds {
        let member = std::mem::take(&mut self.members[x as usize]);
        let n = &mut self.nodes[x as usize];
        n.upper = Box::new([]);
        n.height = 0;
        self.free.push(x);
        if self.len == 0 {
            // Reset the arena so a drained list frees its memory.
            self.nodes.truncate(1);
            self.nodes.shrink_to_fit();
            self.members.truncate(1);
            self.members.shrink_to_fit();
            self.free.clear();
            self.free.shrink_to_fit();
        }
        member
    }

    /// Inserts `(score, member)`. The caller guarantees `member` is not
    /// already present (sorted sets check their dict first).
    pub fn insert(&mut self, score: f64, member: Sds) -> NodeRef {
        let mut update = [HEAD; MAX_LEVEL];
        let mut rank = [0u32; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            rank[i] = if i == self.level - 1 { 0 } else { rank[i + 1] };
            loop {
                let f = self.fwd(x, i);
                if f != NIL && self.cmp_node(f, score, &member) == Ordering::Less {
                    rank[i] += self.span(x, i);
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let height = random_level();
        if height > self.level {
            for i in self.level..height {
                rank[i] = 0;
                update[i] = HEAD;
                self.lvl(HEAD, i).span = self.len as u32;
            }
            self.level = height;
        }
        let x = self.alloc(height, score, member);
        for i in 0..height {
            let u = update[i];
            let uf = self.fwd(u, i);
            let us = self.span(u, i);
            let l = self.lvl(x, i);
            l.forward = uf;
            l.span = us - (rank[0] - rank[i]);
            let ul = self.lvl(u, i);
            ul.forward = x;
            ul.span = (rank[0] - rank[i]) + 1;
        }
        for (i, &u) in update.iter().enumerate().take(self.level).skip(height) {
            self.lvl(u, i).span += 1;
        }
        self.nodes[x as usize].backward = if update[0] == HEAD { NIL } else { update[0] };
        let f0 = self.fwd(x, 0);
        if f0 != NIL {
            self.nodes[f0 as usize].backward = x;
        } else {
            self.tail = x;
        }
        self.len += 1;
        NodeRef(x)
    }

    fn delete_node(&mut self, x: u32, update: &[u32; MAX_LEVEL]) {
        for (i, &u) in update.iter().enumerate().take(self.level) {
            if self.fwd(u, i) == x {
                let xs = self.span(x, i);
                let xf = self.fwd(x, i);
                let l = self.lvl(u, i);
                l.span = l.span + xs - 1;
                l.forward = xf;
            } else {
                self.lvl(u, i).span -= 1;
            }
        }
        let f0 = self.fwd(x, 0);
        let back = self.nodes[x as usize].backward;
        if f0 != NIL {
            self.nodes[f0 as usize].backward = back;
        } else {
            self.tail = back;
        }
        while self.level > 1 && self.fwd(HEAD, self.level - 1) == NIL {
            self.level -= 1;
        }
        self.len -= 1;
    }

    fn find_update(&self, score: f64, member: &[u8], update: &mut [u32; MAX_LEVEL]) -> u32 {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && self.cmp_node(f, score, member) == Ordering::Less {
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        self.fwd(x, 0)
    }

    /// Removes `(score, member)`; returns the stored member if it existed.
    pub fn remove(&mut self, score: f64, member: &[u8]) -> Option<Sds> {
        let mut update = [HEAD; MAX_LEVEL];
        let x = self.find_update(score, member, &mut update);
        if x != NIL && self.score_of(x) == score && self.member_of(x) == member {
            self.delete_node(x, &update);
            Some(self.release(x))
        } else {
            None
        }
    }

    /// Changes the score of an existing element. Updates in place when the
    /// element keeps its position (the common `ZINCRBY` case).
    pub fn update_score(&mut self, cur: f64, member: &[u8], new: f64) -> NodeRef {
        let mut update = [HEAD; MAX_LEVEL];
        let x = self.find_update(cur, member, &mut update);
        assert!(
            x != NIL && self.score_of(x) == cur && self.member_of(x) == member,
            "element must exist"
        );
        let back = self.nodes[x as usize].backward;
        let f0 = self.fwd(x, 0);
        if (back == NIL || self.score_of(back) < new) && (f0 == NIL || self.score_of(f0) > new) {
            self.nodes[x as usize].score = new;
            return NodeRef(x);
        }
        self.delete_node(x, &update);
        let m = self.release(x);
        self.insert(new, m)
    }

    /// 0-based rank of `(score, member)`, if present.
    pub fn rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        let mut x = HEAD;
        let mut rank: u64 = 0;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && self.cmp_node(f, score, member) != Ordering::Greater {
                    rank += self.span(x, i) as u64;
                    x = f;
                } else {
                    break;
                }
            }
            if x != HEAD && self.member_of(x) == member {
                return Some(rank as usize - 1);
            }
        }
        None
    }

    /// The node at 0-based `rank`.
    pub fn by_rank(&self, rank: usize) -> Option<NodeRef> {
        if rank >= self.len {
            return None;
        }
        let target = rank as u64 + 1;
        let mut x = HEAD;
        let mut traversed: u64 = 0;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && traversed + self.span(x, i) as u64 <= target {
                    traversed += self.span(x, i) as u64;
                    x = f;
                } else {
                    break;
                }
            }
            if traversed == target {
                return Some(NodeRef(x));
            }
        }
        None
    }

    /// First node (lowest score).
    pub fn first(&self) -> Option<NodeRef> {
        let f = self.fwd(HEAD, 0);
        (f != NIL).then_some(NodeRef(f))
    }

    /// Last node (highest score).
    pub fn last(&self) -> Option<NodeRef> {
        (self.tail != NIL).then_some(NodeRef(self.tail))
    }

    /// The node after `n`.
    #[inline]
    pub fn next(&self, n: NodeRef) -> Option<NodeRef> {
        let f = self.fwd(n.0, 0);
        (f != NIL).then_some(NodeRef(f))
    }

    /// The node before `n`.
    #[inline]
    pub fn prev(&self, n: NodeRef) -> Option<NodeRef> {
        let b = self.nodes[n.0 as usize].backward;
        (b != NIL).then_some(NodeRef(b))
    }

    fn in_score_range(&self, r: &ScoreRange) -> bool {
        if r.is_empty() || self.tail == NIL {
            return false;
        }
        r.gte_min(self.score_of(self.tail)) && r.lte_max(self.score_of(self.fwd(HEAD, 0)))
    }

    /// First node whose score is inside `r`.
    pub fn first_in_score_range(&self, r: &ScoreRange) -> Option<NodeRef> {
        if !self.in_score_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && !r.gte_min(self.score_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.fwd(x, 0);
        (x != NIL && r.lte_max(self.score_of(x))).then_some(NodeRef(x))
    }

    /// Last node whose score is inside `r`.
    pub fn last_in_score_range(&self, r: &ScoreRange) -> Option<NodeRef> {
        if !self.in_score_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && r.lte_max(self.score_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        (x != HEAD && r.gte_min(self.score_of(x))).then_some(NodeRef(x))
    }

    fn in_lex_range(&self, r: &LexRange) -> bool {
        if r.is_empty() || self.tail == NIL {
            return false;
        }
        r.gte_min(self.member_of(self.tail)) && r.lte_max(self.member_of(self.fwd(HEAD, 0)))
    }

    /// First node whose member is inside `r` (all scores assumed equal).
    pub fn first_in_lex_range(&self, r: &LexRange) -> Option<NodeRef> {
        if !self.in_lex_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && !r.gte_min(self.member_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.fwd(x, 0);
        (x != NIL && r.lte_max(self.member_of(x))).then_some(NodeRef(x))
    }

    /// Last node whose member is inside `r`.
    pub fn last_in_lex_range(&self, r: &LexRange) -> Option<NodeRef> {
        if !self.in_lex_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && r.lte_max(self.member_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        (x != HEAD && r.gte_min(self.member_of(x))).then_some(NodeRef(x))
    }

    /// 0-based rank of a node obtained from this list.
    pub fn rank_of(&self, n: NodeRef) -> usize {
        let (s, m) = self.entry(n);
        self.rank(s, m).expect("node belongs to this list")
    }

    /// Removes every element with score in `r`; returns the removed members.
    pub fn remove_score_range(&mut self, r: &ScoreRange) -> Vec<Sds> {
        let mut update = [HEAD; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && !r.gte_min(self.score_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let mut removed = Vec::new();
        let mut x = self.fwd(x, 0);
        while x != NIL && r.lte_max(self.score_of(x)) {
            let next = self.fwd(x, 0);
            self.delete_node(x, &update);
            removed.push(self.release(x));
            x = next;
        }
        removed
    }

    /// Removes every element with member in `r`; returns the removed members.
    pub fn remove_lex_range(&mut self, r: &LexRange) -> Vec<Sds> {
        let mut update = [HEAD; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && !r.gte_min(self.member_of(f)) {
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let mut removed = Vec::new();
        let mut x = self.fwd(x, 0);
        while x != NIL && r.lte_max(self.member_of(x)) {
            let next = self.fwd(x, 0);
            self.delete_node(x, &update);
            removed.push(self.release(x));
            x = next;
        }
        removed
    }

    /// Removes elements with 0-based rank in `start..=end`.
    pub fn remove_rank_range(&mut self, start: usize, end: usize) -> Vec<Sds> {
        let (start, end) = (start as u64 + 1, end as u64 + 1);
        let mut update = [HEAD; MAX_LEVEL];
        let mut x = HEAD;
        let mut traversed: u64 = 0;
        for i in (0..self.level).rev() {
            loop {
                let f = self.fwd(x, i);
                if f != NIL && traversed + (self.span(x, i) as u64) < start {
                    traversed += self.span(x, i) as u64;
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        traversed += 1;
        let mut removed = Vec::new();
        let mut x = self.fwd(x, 0);
        while x != NIL && traversed <= end {
            let next = self.fwd(x, 0);
            self.delete_node(x, &update);
            removed.push(self.release(x));
            traversed += 1;
            x = next;
        }
        removed
    }

    /// Iterates `(score, member)` in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = (f64, &Sds)> + '_ {
        let mut cur = self.first();
        std::iter::from_fn(move || {
            let n = cur?;
            cur = self.next(n);
            Some(self.entry(n))
        })
    }

    /// Verifies structural invariants (ordering, spans, back pointers, tail).
    /// Used by tests; O(n log n).
    pub fn check_invariants(&self) -> Result<(), String> {
        // Level-0 order and backward links.
        let mut prev = NIL;
        let mut x = self.fwd(HEAD, 0);
        let mut count = 0usize;
        let mut pos_of = std::collections::HashMap::new();
        while x != NIL {
            count += 1;
            pos_of.insert(x, count as u64);
            if self.nodes[x as usize].backward != prev {
                return Err(format!("bad backward at node {x}"));
            }
            if prev != NIL
                && cmp_entry(
                    self.score_of(prev),
                    self.member_of(prev),
                    self.score_of(x),
                    self.member_of(x),
                ) != Ordering::Less
            {
                return Err("level-0 not strictly ordered".into());
            }
            prev = x;
            x = self.fwd(x, 0);
        }
        if count != self.len {
            return Err(format!("len {} but {} nodes", self.len, count));
        }
        if self.tail != prev {
            return Err("bad tail".into());
        }
        // Spans: following level i from node a to b must skip pos(b)-pos(a).
        for i in 0..self.level {
            let mut a = HEAD;
            let mut apos = 0u64;
            loop {
                let b = self.fwd(a, i);
                let bpos = if b == NIL {
                    self.len as u64 + 1
                } else {
                    pos_of[&b]
                };
                if b != NIL && self.span(a, i) as u64 != bpos - apos {
                    return Err(format!("bad span at level {i}"));
                }
                if b == NIL {
                    break;
                }
                if self.nodes[b as usize].height() <= i {
                    return Err("node linked above its height".into());
                }
                a = b;
                apos = bpos;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn m(i: impl std::fmt::Display) -> Sds {
        Sds::from(format!("m{i}").as_bytes())
    }

    #[test]
    fn basic_ordering_and_rank() {
        let mut sl = SkipList::new();
        for i in 0..100 {
            sl.insert((i % 10) as f64, m(i));
        }
        sl.check_invariants().unwrap();
        let all: Vec<_> = sl.iter().map(|(s, m)| (s, m.clone())).collect();
        assert_eq!(all.len(), 100);
        for w in all.windows(2) {
            assert!(cmp_entry(w[0].0, &w[0].1, w[1].0, &w[1].1) == Ordering::Less);
        }
        for (i, (s, mem)) in all.iter().enumerate() {
            assert_eq!(sl.rank(*s, mem), Some(i));
            assert_eq!(sl.entry(sl.by_rank(i).unwrap()).1, mem);
        }
        assert!(sl.by_rank(100).is_none());
        assert_eq!(sl.remove(0.0, b"m0").unwrap(), m(0));
        assert!(sl.remove(0.0, b"m0").is_none());
        sl.check_invariants().unwrap();
    }

    #[test]
    fn ranges() {
        let mut sl = SkipList::new();
        for i in 1..=10 {
            sl.insert(i as f64, m(i));
        }
        let r = ScoreRange {
            min: 3.0,
            max: 7.0,
            minex: true,
            maxex: false,
        };
        assert_eq!(sl.entry(sl.first_in_score_range(&r).unwrap()).0, 4.0);
        assert_eq!(sl.entry(sl.last_in_score_range(&r).unwrap()).0, 7.0);
        let none = ScoreRange {
            min: 11.0,
            max: 20.0,
            minex: false,
            maxex: false,
        };
        assert!(sl.first_in_score_range(&none).is_none());
        let removed = sl.remove_score_range(&r);
        assert_eq!(removed.len(), 4);
        assert_eq!(sl.len(), 6);
        sl.check_invariants().unwrap();
        let removed = sl.remove_rank_range(0, 1);
        assert_eq!(removed, vec![m(1), m(2)]);
        sl.check_invariants().unwrap();
    }

    #[test]
    fn update_in_place_and_moving() {
        let mut sl = SkipList::new();
        for i in 0..10 {
            sl.insert(i as f64 * 10.0, m(i));
        }
        sl.update_score(50.0, b"m5", 55.0); // stays in place
        sl.update_score(10.0, b"m1", 1000.0); // moves to the end
        sl.check_invariants().unwrap();
        assert_eq!(sl.entry(sl.last().unwrap()).1, &m(1));
        assert_eq!(sl.rank(55.0, b"m5"), Some(4));
    }

    #[test]
    fn lex_ranges() {
        let mut sl = SkipList::new();
        for c in ["a", "b", "c", "d", "e"] {
            sl.insert(0.0, Sds::from(c));
        }
        let r = LexRange {
            min: LexBound::Incl(b"b".to_vec()),
            max: LexBound::Excl(b"e".to_vec()),
        };
        assert_eq!(&**sl.entry(sl.first_in_lex_range(&r).unwrap()).1, b"b");
        assert_eq!(&**sl.entry(sl.last_in_lex_range(&r).unwrap()).1, b"d");
        assert_eq!(sl.remove_lex_range(&r).len(), 3);
        sl.check_invariants().unwrap();
        assert!(LexBound::parse(b"x").is_none());
        assert_eq!(LexBound::parse(b"-"), Some(LexBound::NegInf));
    }

    #[test]
    fn drain_resets_arena() {
        let mut sl = SkipList::new();
        for i in 0..1000 {
            sl.insert(i as f64, m(i));
        }
        sl.remove_rank_range(0, 999);
        assert!(sl.is_empty());
        assert_eq!(sl.nodes.len(), 1);
        assert_eq!(std::mem::size_of::<Node>(), 64);
        sl.insert(1.0, m(1));
        sl.check_invariants().unwrap();
    }

    #[derive(Debug, Clone)]
    enum Op {
        Add(u8, i8),
        Remove(u8),
        Update(u8, i8),
        RemoveScores(i8, i8, bool, bool),
        RemoveRanks(u8, u8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (any::<u8>(), any::<i8>()).prop_map(|(m, s)| Op::Add(m, s)),
            2 => any::<u8>().prop_map(Op::Remove),
            2 => (any::<u8>(), any::<i8>()).prop_map(|(m, s)| Op::Update(m, s)),
            1 => (any::<i8>(), any::<i8>(), any::<bool>(), any::<bool>()).prop_map(|(a, b, x, y)| Op::RemoveScores(a, b, x, y)),
            1 => (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Op::RemoveRanks(a, b)),
        ]
    }

    proptest! {
        /// The skiplist agrees with a sorted `Vec<(score, member)>` model on
        /// contents, ranks, rank lookups and range queries after any sequence of
        /// inserts, deletes, score updates and range deletions, and its spans
        /// and links stay consistent.
        #[test]
        fn matches_sorted_vec_model(ops in proptest::collection::vec(op(), 1..200)) {
            let mut sl = SkipList::new();
            let mut model: Vec<(f64, Vec<u8>)> = Vec::new();
            let sort = |v: &mut Vec<(f64, Vec<u8>)>| v.sort_by(|a, b| cmp_entry(a.0, &a.1, b.0, &b.1));
            for op in ops {
                match op {
                    Op::Add(mi, s) => {
                        let mem = m(mi).to_vec();
                        if !model.iter().any(|(_, x)| *x == mem) {
                            sl.insert(s as f64, Sds::from_slice(&mem));
                            model.push((s as f64, mem));
                            sort(&mut model);
                        }
                    }
                    Op::Remove(mi) => {
                        let mem = m(mi).to_vec();
                        if let Some(pos) = model.iter().position(|(_, x)| *x == mem) {
                            let (s, _) = model.remove(pos);
                            prop_assert!(sl.remove(s, &mem).is_some());
                        } else {
                            prop_assert!(sl.remove(0.0, &mem).is_none());
                        }
                    }
                    Op::Update(mi, s) => {
                        let mem = m(mi).to_vec();
                        if let Some(pos) = model.iter().position(|(_, x)| *x == mem) {
                            let cur = model[pos].0;
                            sl.update_score(cur, &mem, s as f64);
                            model[pos].0 = s as f64;
                            sort(&mut model);
                        }
                    }
                    Op::RemoveScores(a, b, minex, maxex) => {
                        let r = ScoreRange { min: a as f64, max: b as f64, minex, maxex };
                        let got: Vec<Vec<u8>> = sl.remove_score_range(&r).into_iter().map(|s| s.to_vec()).collect();
                        let want: Vec<Vec<u8>> = model.iter().filter(|(s, _)| r.contains(*s)).map(|(_, m)| m.clone()).collect();
                        model.retain(|(s, _)| !r.contains(*s));
                        prop_assert_eq!(got, want);
                    }
                    Op::RemoveRanks(a, b) => {
                        let (a, b) = (a as usize % 40, b as usize % 40);
                        if a <= b && a < model.len() {
                            let end = b.min(model.len() - 1);
                            let got = sl.remove_rank_range(a, end).len();
                            model.drain(a..=end);
                            prop_assert_eq!(got, end - a + 1);
                        }
                    }
                }
                prop_assert_eq!(sl.len(), model.len());
            }
            sl.check_invariants().map_err(TestCaseError::fail)?;
            let got: Vec<(f64, Vec<u8>)> = sl.iter().map(|(s, m)| (s, m.to_vec())).collect();
            prop_assert_eq!(&got, &model);
            for (i, (s, mem)) in model.iter().enumerate() {
                prop_assert_eq!(sl.rank(*s, mem), Some(i));
                prop_assert_eq!(sl.entry(sl.by_rank(i).unwrap()).1.to_vec(), mem.clone());
            }
            // Range queries agree with a linear filter.
            for (lo, hi) in [(-50.0, 50.0), (0.0, 0.0), (-128.0, -100.0), (10.0, 5.0)] {
                for (minex, maxex) in [(false, false), (true, false), (false, true), (true, true)] {
                    let r = ScoreRange { min: lo, max: hi, minex, maxex };
                    let want: Vec<&(f64, Vec<u8>)> = model.iter().filter(|(s, _)| r.contains(*s)).collect();
                    let first = sl.first_in_score_range(&r).map(|n| sl.entry(n).1.to_vec());
                    let last = sl.last_in_score_range(&r).map(|n| sl.entry(n).1.to_vec());
                    prop_assert_eq!(first, want.first().map(|e| e.1.clone()));
                    prop_assert_eq!(last, want.last().map(|e| e.1.clone()));
                }
            }
        }
    }
}
