//! Sorted sets: a sorted listpack of `(member, score)` pairs while small, a
//! [`SkipList`] plus a member→score [`Dict`] afterwards (exactly Redis'
//! dual-structure design: the dict gives O(1) `ZSCORE`, the skiplist gives
//! O(log n) rank and range operations).

use super::Limits;
use crate::dict::Dict;
use crate::listpack::Listpack;
use crate::sds::Sds;
use crate::skiplist::{LexRange, ScoreRange, SkipList, cmp_entry};
use std::cmp::Ordering;

/// Skiplist encoding: both structures always hold the same elements.
pub struct SkipZ {
    /// member → score.
    pub dict: Dict<f64>,
    /// `(score, member)` order with rank spans.
    pub list: SkipList,
}

/// A sorted set value.
pub enum ZSet {
    /// `member, score(8 bytes LE f64), ...` sorted by `(score, member)`.
    Packed(Listpack),
    /// Skiplist + dict.
    Skip(SkipZ),
}

#[inline]
fn dec_score(b: &[u8]) -> f64 {
    f64::from_le_bytes(b.try_into().expect("score entries are 8 bytes"))
}

/// Iterates `(member, score)` pairs of a packed zset, front to back.
fn packed_iter(lp: &Listpack) -> PackedPairs<'_> {
    PackedPairs { it: lp.iter() }
}

struct PackedPairs<'a> {
    it: crate::listpack::ListpackIter<'a>,
}

impl<'a> Iterator for PackedPairs<'a> {
    type Item = (&'a [u8], f64);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let m = self.it.next()?;
        let s = self.it.next()?;
        Some((m, dec_score(s)))
    }
}

impl DoubleEndedIterator for PackedPairs<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        let s = self.it.next_back()?;
        let m = self.it.next_back()?;
        Some((m, dec_score(s)))
    }
}

/// `zzlIsInLexRange`: the last member must be `>= min` and the first `<= max`.
fn packed_in_lex_range(pairs: &[(&[u8], f64)], r: &LexRange) -> bool {
    match (pairs.first(), pairs.last()) {
        (Some(f), Some(l)) => !r.is_empty() && r.gte_min(l.0) && r.lte_max(f.0),
        _ => false,
    }
}

/// Elements a forward lex scan visits, with Redis' listpack semantics: start at
/// the first element `>= min`, continue while `<= max`. (Lex queries assume
/// equal scores; with mixed scores this reproduces Redis' exact output.)
fn packed_lex_fwd<'a, 'b>(pairs: &'b [(&'a [u8], f64)], r: &LexRange) -> &'b [(&'a [u8], f64)] {
    if !packed_in_lex_range(pairs, r) {
        return &[];
    }
    let Some(start) = pairs.iter().position(|(m, _)| r.gte_min(m)) else {
        return &[];
    };
    let len = pairs[start..]
        .iter()
        .take_while(|(m, _)| r.lte_max(m))
        .count();
    &pairs[start..start + len]
}

/// Elements a reverse lex scan visits: start at the last element `<= max`,
/// continue backwards while `>= min`. Returned in ascending order.
fn packed_lex_rev<'a, 'b>(pairs: &'b [(&'a [u8], f64)], r: &LexRange) -> &'b [(&'a [u8], f64)] {
    if !packed_in_lex_range(pairs, r) {
        return &[];
    }
    let Some(end) = pairs.iter().rposition(|(m, _)| r.lte_max(m)) else {
        return &[];
    };
    let len = pairs[..=end]
        .iter()
        .rev()
        .take_while(|(m, _)| r.gte_min(m))
        .count();
    &pairs[end + 1 - len..=end]
}

impl Default for ZSet {
    fn default() -> Self {
        ZSet::Packed(Listpack::new())
    }
}

impl ZSet {
    /// Creates a zset sized for `size_hint` elements whose longest member is
    /// `max_len` bytes (`zsetTypeCreate`).
    pub fn create(size_hint: usize, max_len: usize, lim: &Limits) -> Self {
        if size_hint <= lim.zset_max_listpack_entries && max_len <= lim.zset_max_listpack_value {
            ZSet::Packed(Listpack::new())
        } else {
            ZSet::Skip(SkipZ {
                dict: Dict::new(),
                list: SkipList::new(),
            })
        }
    }

    /// Converts to the skiplist encoding when a bulk insert will exceed the
    /// listpack limits (`zsetTypeMaybeConvert`).
    pub fn maybe_convert(&mut self, size_hint: usize, max_len: usize, lim: &Limits) {
        if matches!(self, ZSet::Packed(_))
            && (size_hint > lim.zset_max_listpack_entries || max_len > lim.zset_max_listpack_value)
        {
            self.convert_to_skip();
        }
    }

    /// Converts back to a listpack if small enough (used by `*STORE` commands).
    pub fn shrink_if_possible(&mut self, lim: &Limits) {
        if let ZSet::Skip(z) = self
            && z.list.len() <= lim.zset_max_listpack_entries
            && z.list
                .iter()
                .all(|(_, m)| m.len() <= lim.zset_max_listpack_value)
        {
            let mut lp = Listpack::new();
            for (s, m) in z.list.iter() {
                lp.push_back(m);
                lp.push_back(&s.to_le_bytes());
            }
            *self = ZSet::Packed(lp);
        }
    }

    fn convert_to_skip(&mut self) {
        if let ZSet::Packed(lp) = self {
            let mut z = SkipZ {
                dict: Dict::new(),
                list: SkipList::new(),
            };
            for (m, s) in packed_iter(lp) {
                z.dict.insert(Sds::from_slice(m), s);
                z.list.insert(s, Sds::from_slice(m));
            }
            *self = ZSet::Skip(z);
        }
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            ZSet::Packed(lp) => lp.len() / 2,
            ZSet::Skip(z) => z.list.len(),
        }
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING` name.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            ZSet::Packed(_) => "listpack",
            ZSet::Skip(_) => "skiplist",
        }
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        match self {
            ZSet::Packed(lp) => lp.bytes(),
            ZSet::Skip(z) => z.dict.overhead_bytes() + z.list.heap_bytes(),
        }
    }

    /// Score of `member`.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            ZSet::Packed(lp) => {
                let p = lp.find_from(lp.first()?, member, 2)?;
                Some(dec_score(lp.get(lp.next(p)?)))
            }
            ZSet::Skip(z) => z.dict.get(member).copied(),
        }
    }

    /// Inserts a member known to be absent.
    pub fn insert(&mut self, member: &[u8], score: f64, lim: &Limits) {
        if let ZSet::Packed(lp) = self
            && (lp.len() / 2 + 1 > lim.zset_max_listpack_entries
                || member.len() > lim.zset_max_listpack_value)
        {
            self.convert_to_skip();
        }
        match self {
            ZSet::Packed(lp) => {
                // Redis' listpack stores integral scores as integers, so a
                // negative zero comes back as 0.
                let score = if score == 0.0 { 0.0 } else { score };
                // Find the first element greater than (score, member).
                let mut p = lp.first();
                while let Some(pos) = p {
                    let m = lp.get(pos);
                    let sp = lp.next(pos).expect("score follows member");
                    let s = dec_score(lp.get(sp));
                    if cmp_entry(s, m, score, member) == Ordering::Greater {
                        break;
                    }
                    p = lp.next(sp);
                }
                let at = p.unwrap_or(lp.bytes());
                lp.insert_at(at, &score.to_le_bytes());
                lp.insert_at(at, member);
            }
            ZSet::Skip(z) => {
                z.dict.insert_new_h(
                    crate::dict::hash_key(member),
                    Sds::from_slice(member),
                    score,
                );
                z.list.insert(score, Sds::from_slice(member));
            }
        }
    }

    /// Changes the score of an existing member.
    pub fn update(&mut self, member: &[u8], old: f64, new: f64, lim: &Limits) {
        match self {
            ZSet::Packed(_) => {
                self.remove(member);
                self.insert(member, new, lim);
            }
            ZSet::Skip(z) => {
                z.list.update_score(old, member, new);
                *z.dict.get_mut(member).expect("member exists") = new;
            }
        }
    }

    /// Removes a member; returns true if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            ZSet::Packed(lp) => match lp.first().and_then(|f| lp.find_from(f, member, 2)) {
                Some(p) => {
                    lp.remove_n_at(p, 2);
                    true
                }
                None => false,
            },
            ZSet::Skip(z) => match z.dict.remove(member) {
                Some((_, s)) => {
                    z.list.remove(s, member);
                    true
                }
                None => false,
            },
        }
    }

    /// 0-based rank (ascending, or descending when `rev`).
    pub fn rank(&self, member: &[u8], rev: bool) -> Option<(usize, f64)> {
        let len = self.len();
        let (r, s) = match self {
            ZSet::Packed(lp) => packed_iter(lp)
                .enumerate()
                .find(|(_, (m, _))| *m == member)
                .map(|(i, (_, s))| (i, s))?,
            ZSet::Skip(z) => {
                let s = *z.dict.get(member)?;
                (z.list.rank(s, member)?, s)
            }
        };
        Some((if rev { len - 1 - r } else { r }, s))
    }

    /// Visits elements with ascending ranks `start..end` (or the descending
    /// ranks when `rev`).
    pub fn range_by_rank(
        &self,
        start: usize,
        end: usize,
        rev: bool,
        mut f: impl FnMut(&[u8], f64),
    ) {
        if start >= end {
            return;
        }
        let n = end - start;
        match self {
            ZSet::Packed(lp) => {
                if rev {
                    packed_iter(lp)
                        .rev()
                        .skip(start)
                        .take(n)
                        .for_each(|(m, s)| f(m, s));
                } else {
                    packed_iter(lp)
                        .skip(start)
                        .take(n)
                        .for_each(|(m, s)| f(m, s));
                }
            }
            ZSet::Skip(z) => {
                let len = z.list.len();
                let first = if rev { len - 1 - start } else { start };
                let mut node = z.list.by_rank(first);
                for _ in 0..n {
                    let Some(nd) = node else { break };
                    let (s, m) = z.list.entry(nd);
                    f(m, s);
                    node = if rev {
                        z.list.prev(nd)
                    } else {
                        z.list.next(nd)
                    };
                }
            }
        }
    }

    /// Visits elements whose score is in `r`, skipping `offset` and visiting at
    /// most `limit` (negative limit = unlimited). Descending when `rev`.
    pub fn range_by_score(
        &self,
        r: &ScoreRange,
        rev: bool,
        offset: usize,
        limit: i64,
        mut f: impl FnMut(&[u8], f64),
    ) {
        let mut remaining = if limit < 0 {
            usize::MAX
        } else {
            limit as usize
        };
        if remaining == 0 {
            return;
        }
        match self {
            ZSet::Packed(lp) => {
                let mut skip = offset;
                let mut visit = |m: &[u8], s: f64| -> bool {
                    if skip > 0 {
                        skip -= 1;
                        return true;
                    }
                    f(m, s);
                    remaining -= 1;
                    remaining > 0
                };
                if rev {
                    for (m, s) in packed_iter(lp).rev() {
                        if !r.gte_min(s) {
                            break;
                        }
                        if r.lte_max(s) && !visit(m, s) {
                            break;
                        }
                    }
                } else {
                    for (m, s) in packed_iter(lp) {
                        if !r.lte_max(s) {
                            break;
                        }
                        if r.gte_min(s) && !visit(m, s) {
                            break;
                        }
                    }
                }
            }
            ZSet::Skip(z) => {
                let mut node = if rev {
                    z.list.last_in_score_range(r)
                } else {
                    z.list.first_in_score_range(r)
                };
                let mut skip = offset;
                while let Some(nd) = node {
                    if skip == 0 {
                        break;
                    }
                    skip -= 1;
                    node = if rev {
                        z.list.prev(nd)
                    } else {
                        z.list.next(nd)
                    };
                }
                while let Some(nd) = node {
                    let (s, m) = z.list.entry(nd);
                    if !(if rev { r.gte_min(s) } else { r.lte_max(s) }) {
                        break;
                    }
                    f(m, s);
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                    node = if rev {
                        z.list.prev(nd)
                    } else {
                        z.list.next(nd)
                    };
                }
            }
        }
    }

    /// Like [`ZSet::range_by_score`] for lexicographic ranges.
    pub fn range_by_lex(
        &self,
        r: &LexRange,
        rev: bool,
        offset: usize,
        limit: i64,
        mut f: impl FnMut(&[u8], f64),
    ) {
        let mut remaining = if limit < 0 {
            usize::MAX
        } else {
            limit as usize
        };
        if remaining == 0 {
            return;
        }
        match self {
            ZSet::Packed(lp) => {
                let pairs: Vec<(&[u8], f64)> = packed_iter(lp).collect();
                let it: Box<dyn Iterator<Item = &(&[u8], f64)>> = if rev {
                    Box::new(packed_lex_rev(&pairs, r).iter().rev())
                } else {
                    Box::new(packed_lex_fwd(&pairs, r).iter())
                };
                for (m, s) in it.skip(offset) {
                    f(m, *s);
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                }
            }
            ZSet::Skip(z) => {
                let mut node = if rev {
                    z.list.last_in_lex_range(r)
                } else {
                    z.list.first_in_lex_range(r)
                };
                let mut skip = offset;
                while let Some(nd) = node {
                    if skip == 0 {
                        break;
                    }
                    skip -= 1;
                    node = if rev {
                        z.list.prev(nd)
                    } else {
                        z.list.next(nd)
                    };
                }
                while let Some(nd) = node {
                    let (s, m) = z.list.entry(nd);
                    if !(if rev { r.gte_min(m) } else { r.lte_max(m) }) {
                        break;
                    }
                    f(m, s);
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                    node = if rev {
                        z.list.prev(nd)
                    } else {
                        z.list.next(nd)
                    };
                }
            }
        }
    }

    /// Number of elements with score in `r` (O(log n) for skiplists).
    pub fn count_score(&self, r: &ScoreRange) -> usize {
        match self {
            ZSet::Packed(lp) => packed_iter(lp).filter(|(_, s)| r.contains(*s)).count(),
            ZSet::Skip(z) => match (
                z.list.first_in_score_range(r),
                z.list.last_in_score_range(r),
            ) {
                (Some(a), Some(b)) => z.list.rank_of(b) - z.list.rank_of(a) + 1,
                _ => 0,
            },
        }
    }

    /// Number of elements with member in `r` (`ZLEXCOUNT`). With mixed scores
    /// the result follows Redis' rank arithmetic exactly (it may even be
    /// negative), since lex ranges assume equal scores.
    pub fn count_lex(&self, r: &LexRange) -> i64 {
        match self {
            ZSet::Packed(lp) => {
                packed_lex_fwd(&packed_iter(lp).collect::<Vec<_>>(), r).len() as i64
            }
            ZSet::Skip(z) => match z.list.first_in_lex_range(r) {
                None => 0,
                Some(a) => {
                    let len = z.list.len() as i64;
                    let ra = z.list.rank_of(a) as i64;
                    let mut count = len - ra;
                    if let Some(b) = z.list.last_in_lex_range(r) {
                        count -= len - (z.list.rank_of(b) as i64 + 1);
                    }
                    count
                }
            },
        }
    }

    /// Removes elements with ascending rank in `start..end`; returns the count.
    pub fn remove_rank_range(&mut self, start: usize, end: usize) -> usize {
        if start >= end {
            return 0;
        }
        match self {
            ZSet::Packed(lp) => {
                if let Some(p) = lp.seek(start as i64 * 2) {
                    lp.remove_n_at(p, (end - start) * 2);
                }
                end - start
            }
            ZSet::Skip(z) => {
                let removed = z.list.remove_rank_range(start, end - 1);
                for m in &removed {
                    z.dict.remove(m);
                }
                removed.len()
            }
        }
    }

    /// Removes elements whose score is in `r`; returns the count.
    pub fn remove_score_range(&mut self, r: &ScoreRange) -> usize {
        match self {
            ZSet::Packed(_) => {
                let victims: Vec<Vec<u8>> = self.collect(|_, s| r.contains(s));
                victims.iter().for_each(|m| {
                    self.remove(m);
                });
                victims.len()
            }
            ZSet::Skip(z) => {
                let removed = z.list.remove_score_range(r);
                for m in &removed {
                    z.dict.remove(m);
                }
                removed.len()
            }
        }
    }

    /// Removes elements whose member is in `r`; returns the count.
    pub fn remove_lex_range(&mut self, r: &LexRange) -> usize {
        match self {
            ZSet::Packed(lp) => {
                let pairs: Vec<(&[u8], f64)> = packed_iter(lp).collect();
                let victims: Vec<Vec<u8>> = packed_lex_fwd(&pairs, r)
                    .iter()
                    .map(|(m, _)| m.to_vec())
                    .collect();
                victims.iter().for_each(|m| {
                    self.remove(m);
                });
                victims.len()
            }
            ZSet::Skip(z) => {
                let removed = z.list.remove_lex_range(r);
                for m in &removed {
                    z.dict.remove(m);
                }
                removed.len()
            }
        }
    }

    fn collect(&self, pred: impl Fn(&[u8], f64) -> bool) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        self.for_each(|m, s| {
            if pred(m, s) {
                out.push(m.to_vec())
            }
        });
        out
    }

    /// Removes and returns the lowest (`max == false`) or highest element.
    pub fn pop(&mut self, max: bool) -> Option<(Vec<u8>, f64)> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let mut out = None;
        let r = if max { len - 1 } else { 0 };
        self.range_by_rank(r, r + 1, false, |m, s| out = Some((m.to_vec(), s)));
        self.remove_rank_range(r, r + 1);
        out
    }

    /// Visits all elements in ascending order.
    pub fn for_each(&self, mut f: impl FnMut(&[u8], f64)) {
        match self {
            ZSet::Packed(lp) => packed_iter(lp).for_each(|(m, s)| f(m, s)),
            ZSet::Skip(z) => z.list.iter().for_each(|(s, m)| f(m, s)),
        }
    }

    /// A random element.
    pub fn random_element(&self) -> Option<(Vec<u8>, f64)> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        match self {
            ZSet::Packed(_) => {
                let r = crate::util::rand_below(len as u64) as usize;
                let mut out = None;
                self.range_by_rank(r, r + 1, false, |m, s| out = Some((m.to_vec(), s)));
                out
            }
            ZSet::Skip(z) => z.dict.random_entry().map(|(k, s)| (k.to_vec(), *s)),
        }
    }

    /// One `ZSCAN` step.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(&[u8], f64)) -> u64 {
        match self {
            ZSet::Packed(_) => {
                self.for_each(f);
                0
            }
            ZSet::Skip(z) => {
                let mut c = cursor;
                let mut visited = 0;
                let mut iterations = count * 10;
                loop {
                    c = z.dict.scan(c, |k, s| {
                        visited += 1;
                        f(k, *s)
                    });
                    iterations = iterations.saturating_sub(1);
                    if c == 0 || visited >= count || iterations == 0 {
                        return c;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn contents(z: &ZSet) -> Vec<(Vec<u8>, f64)> {
        let mut v = Vec::new();
        z.for_each(|m, s| v.push((m.to_vec(), s)));
        v
    }

    #[test]
    fn packed_and_skip_agree() {
        let lim = Limits::default();
        let mut a = ZSet::create(1, 1, &lim);
        let mut b = ZSet::create(1000, 1, &lim);
        assert_eq!(a.encoding_name(), "listpack");
        assert_eq!(b.encoding_name(), "skiplist");
        for (i, m) in ["d", "a", "c", "b", "e"].iter().enumerate() {
            let s = (i % 3) as f64;
            a.insert(m.as_bytes(), s, &lim);
            b.insert(m.as_bytes(), s, &lim);
        }
        assert_eq!(contents(&a), contents(&b));
        assert_eq!(a.rank(b"c", false), b.rank(b"c", false));
        assert_eq!(a.rank(b"c", true), b.rank(b"c", true));
        let r = ScoreRange {
            min: 0.0,
            max: 1.0,
            minex: true,
            maxex: false,
        };
        assert_eq!(a.count_score(&r), b.count_score(&r));
        a.update(b"d", 0.0, 10.0, &lim);
        b.update(b"d", 0.0, 10.0, &lim);
        assert_eq!(contents(&a), contents(&b));
        assert_eq!(a.pop(true), b.pop(true));
        assert_eq!(a.pop(false), b.pop(false));
        assert_eq!(contents(&a), contents(&b));
    }

    #[test]
    fn converts_past_threshold() {
        let lim = Limits::default();
        let mut z = ZSet::default();
        for i in 0..=128 {
            z.insert(format!("m{i}").as_bytes(), i as f64, &lim);
        }
        assert_eq!(z.encoding_name(), "skiplist");
        assert_eq!(z.len(), 129);
        z.remove_rank_range(0, 100);
        z.shrink_if_possible(&lim);
        assert_eq!(z.encoding_name(), "listpack");
        assert_eq!(z.score(b"m128"), Some(128.0));
    }

    proptest! {
        /// Both encodings agree with each other on every query after the same
        /// sequence of inserts, updates, removals and range deletions.
        #[test]
        fn encodings_agree(ops in proptest::collection::vec((0u8..5, 0u8..30, -5i8..5), 0..150)) {
            let lim = Limits::default();
            let mut a = ZSet::create(1, 1, &lim);
            let mut b = ZSet::create(1000, 1000, &lim);
            for (op, m, s) in ops {
                let m = format!("m{m}").into_bytes();
                let s = s as f64;
                for z in [&mut a, &mut b] {
                    match op {
                        0 | 1 => match z.score(&m) {
                            Some(old) => z.update(&m, old, s, &lim),
                            None => z.insert(&m, s, &lim),
                        },
                        2 => { z.remove(&m); }
                        3 => { z.remove_score_range(&ScoreRange { min: s, max: s + 1.0, minex: false, maxex: true }); }
                        _ => { let n = z.len(); z.remove_rank_range(n / 2, n / 2 + 1); }
                    }
                }
                prop_assert_eq!(contents(&a), contents(&b));
                prop_assert_eq!(a.score(&m), b.score(&m));
                prop_assert_eq!(a.rank(&m, true), b.rank(&m, true));
            }
            // Lex queries are only meaningful when all scores are equal.
            let lim = Limits::default();
            let mut la = ZSet::create(1, 1, &lim);
            let mut lb = ZSet::create(1000, 1000, &lim);
            a.for_each(|m, _| { la.insert(m, 0.0, &lim); lb.insert(m, 0.0, &lim); });
            let r = LexRange { min: LexBound::Incl(b"m1".to_vec()), max: LexBound::Excl(b"m5".to_vec()) };
            prop_assert_eq!(la.count_lex(&r), lb.count_lex(&r));
            let (mut x, mut y) = (Vec::new(), Vec::new());
            la.range_by_lex(&r, true, 1, 5, |m, _| x.push(m.to_vec()));
            lb.range_by_lex(&r, true, 1, 5, |m, _| y.push(m.to_vec()));
            prop_assert_eq!(x, y);
            let mut x = Vec::new();
            let mut y = Vec::new();
            a.range_by_score(&ScoreRange { min: -2.0, max: 3.0, minex: false, maxex: false }, true, 1, 3, |m, s| x.push((m.to_vec(), s)));
            b.range_by_score(&ScoreRange { min: -2.0, max: 3.0, minex: false, maxex: false }, true, 1, 3, |m, s| y.push((m.to_vec(), s)));
            prop_assert_eq!(x, y);
        }
    }

    use crate::skiplist::LexBound;
}
