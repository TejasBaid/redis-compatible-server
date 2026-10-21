//! Lists: a listpack while small, a `VecDeque<Sds>` once they grow.
//!
//! Redis uses a quicklist (a linked list of listpacks) for large lists. A ring
//! buffer of compact strings gives O(1) push/pop at both ends *and* O(1)
//! `LINDEX`/`LSET`, at the cost of ~24 bytes per element instead of a few.
//! The listpack↔deque conversion points mirror Redis' size rules
//! (`list-max-listpack-size`), computed with Redis' own listpack entry sizes.

use super::{Limits, REDIS_LP_OVERHEAD, redis_lp_entry_size};
use crate::listpack::{Listpack, ListpackIter};
use crate::sds::Sds;
use std::collections::VecDeque;

enum Enc {
    Packed(Listpack),
    Deque(VecDeque<Sds>),
}

/// A list value.
pub struct List {
    enc: Enc,
    /// Bytes a Redis listpack holding these elements would use; drives the
    /// encoding conversions.
    redis_bytes: usize,
}

const OPTIMIZATION_LEVEL: [usize; 5] = [4096, 8192, 16384, 32768, 65536];
const SIZE_SAFETY_LIMIT: usize = 8192;

fn node_limit(fill: i64) -> (usize, usize) {
    if fill >= 0 {
        (usize::MAX, if fill == 0 { 1 } else { fill as usize })
    } else {
        let off = ((-fill - 1) as usize).min(OPTIMIZATION_LEVEL.len() - 1);
        (OPTIMIZATION_LEVEL[off], usize::MAX)
    }
}

fn exceeds_limit(fill: i64, bytes: usize, count: usize) -> bool {
    let (sz, cnt) = node_limit(fill);
    if sz != usize::MAX {
        bytes > sz
    } else {
        bytes > SIZE_SAFETY_LIMIT || count > cnt
    }
}

impl Default for List {
    fn default() -> Self {
        Self::new()
    }
}

impl List {
    /// An empty, listpack-encoded list.
    pub fn new() -> Self {
        List {
            enc: Enc::Packed(Listpack::new()),
            redis_bytes: REDIS_LP_OVERHEAD,
        }
    }

    /// Number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.enc {
            Enc::Packed(lp) => lp.len(),
            Enc::Deque(d) => d.len(),
        }
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING` name (`quicklist` is reported for the large
    /// encoding so tooling that inspects encodings keeps working).
    pub fn encoding_name(&self) -> &'static str {
        match self.enc {
            Enc::Packed(_) => "listpack",
            Enc::Deque(_) => "quicklist",
        }
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        match &self.enc {
            Enc::Packed(lp) => lp.bytes(),
            Enc::Deque(d) => {
                d.capacity() * std::mem::size_of::<Sds>()
                    + d.iter().map(|s| s.heap_size()).sum::<usize>()
            }
        }
    }

    /// Converts to the large encoding before an insert that would cross the
    /// listpack limit (`listTypeTryConvertListpack`).
    fn grow_check(&mut self, items: &[&[u8]], lim: &Limits) {
        if let Enc::Packed(lp) = &self.enc {
            let add_bytes: usize = items.iter().map(|i| i.len()).sum();
            if exceeds_limit(
                lim.list_max_listpack_size,
                self.redis_bytes + add_bytes,
                lp.len() + items.len(),
            ) {
                let d: VecDeque<Sds> = lp.iter().map(Sds::from_slice).collect();
                self.enc = Enc::Deque(d);
            }
        }
    }

    /// Converts back to a listpack once the list is small again
    /// (`listTypeTryConvertQuicklist` with the "shrinking" half-limit rule).
    pub fn shrink_check(&mut self, lim: &Limits) {
        if let Enc::Deque(d) = &self.enc {
            let (sz, cnt) = node_limit(lim.list_max_listpack_size);
            let (sz, cnt) = (sz / 2, cnt / 2);
            if self.redis_bytes <= sz && d.len() <= cnt {
                let mut lp = Listpack::new();
                for s in d {
                    lp.push_back(s);
                }
                self.enc = Enc::Packed(lp);
            }
        }
    }

    /// Pushes elements one by one to the head (`front`) or tail.
    pub fn push(&mut self, items: &[&[u8]], front: bool, lim: &Limits) {
        self.grow_check(items, lim);
        for it in items {
            self.redis_bytes += redis_lp_entry_size(it);
            match &mut self.enc {
                Enc::Packed(lp) => {
                    if front {
                        lp.push_front(it)
                    } else {
                        lp.push_back(it)
                    }
                }
                Enc::Deque(d) => {
                    if front {
                        d.push_front(Sds::from_slice(it))
                    } else {
                        d.push_back(Sds::from_slice(it))
                    }
                }
            }
        }
    }

    /// Pops one element from the head or tail. Does not run the shrink check
    /// (callers popping several elements call [`List::shrink_check`] once).
    pub fn pop(&mut self, front: bool) -> Option<Sds> {
        let v = match &mut self.enc {
            Enc::Packed(lp) => {
                let v = if front { lp.pop_front() } else { lp.pop_back() }?;
                Sds::from_vec(v)
            }
            Enc::Deque(d) => if front { d.pop_front() } else { d.pop_back() }?,
        };
        self.redis_bytes -= redis_lp_entry_size(&v);
        Some(v)
    }

    /// Element at `idx` (negative counts from the tail).
    pub fn get(&self, idx: i64) -> Option<&[u8]> {
        match &self.enc {
            Enc::Packed(lp) => lp.seek(idx).map(|p| lp.get(p)),
            Enc::Deque(d) => {
                let len = d.len() as i64;
                let i = if idx < 0 { len + idx } else { idx };
                if i < 0 || i >= len {
                    None
                } else {
                    Some(&d[i as usize])
                }
            }
        }
    }

    /// Replaces the element at `idx`; returns false if out of range.
    pub fn set(&mut self, idx: i64, value: &[u8], lim: &Limits) -> bool {
        self.grow_check(&[value], lim);
        let old_size;
        match &mut self.enc {
            Enc::Packed(lp) => {
                let Some(p) = lp.seek(idx) else {
                    return false;
                };
                old_size = redis_lp_entry_size(lp.get(p));
                lp.replace_at(p, value);
            }
            Enc::Deque(d) => {
                let len = d.len() as i64;
                let i = if idx < 0 { len + idx } else { idx };
                if i < 0 || i >= len {
                    return false;
                }
                old_size = redis_lp_entry_size(&d[i as usize]);
                d[i as usize] = Sds::from_slice(value);
            }
        }
        self.redis_bytes = self.redis_bytes - old_size + redis_lp_entry_size(value);
        self.shrink_check(lim);
        true
    }

    /// Iterates from head to tail.
    pub fn iter(&self) -> ListIter<'_> {
        match &self.enc {
            Enc::Packed(lp) => ListIter::Packed(lp.iter()),
            Enc::Deque(d) => ListIter::Deque(d.iter()),
        }
    }

    /// Inserts `value` before/after the first occurrence of `pivot`. Returns
    /// the new length, or `None` if the pivot was not found.
    pub fn insert(
        &mut self,
        after: bool,
        pivot: &[u8],
        value: &[u8],
        lim: &Limits,
    ) -> Option<usize> {
        let pos = self.iter().position(|e| e == pivot)?;
        self.grow_check(&[value], lim);
        let at = if after { pos + 1 } else { pos };
        match &mut self.enc {
            Enc::Packed(lp) => {
                let p = lp.seek(at as i64).unwrap_or(lp.bytes());
                lp.insert_at(p, value);
            }
            Enc::Deque(d) => d.insert(at, Sds::from_slice(value)),
        }
        self.redis_bytes += redis_lp_entry_size(value);
        Some(self.len())
    }

    /// `LREM`: removes up to `count` occurrences (`count < 0` scans from the
    /// tail, `0` removes all). Returns the number removed.
    pub fn remove(&mut self, count: i64, value: &[u8], lim: &Limits) -> usize {
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let from_tail = count < 0;
        let mut removed = 0;
        match &mut self.enc {
            Enc::Packed(lp) => {
                if from_tail {
                    let mut p = lp.last();
                    while let Some(pos) = p {
                        if removed >= limit {
                            break;
                        }
                        let prev = lp.prev(pos);
                        if lp.get(pos) == value {
                            lp.remove_at(pos);
                            removed += 1;
                        }
                        p = prev;
                    }
                } else {
                    let mut p = lp.first();
                    while let Some(pos) = p {
                        if removed >= limit {
                            break;
                        }
                        if lp.get(pos) == value {
                            p = lp.remove_at(pos);
                            removed += 1;
                        } else {
                            p = lp.next(pos);
                        }
                    }
                }
            }
            Enc::Deque(d) => {
                let mut keep = VecDeque::with_capacity(d.len());
                if from_tail {
                    while let Some(e) = d.pop_back() {
                        if removed < limit && &*e == value {
                            removed += 1;
                        } else {
                            keep.push_front(e);
                        }
                    }
                } else {
                    while let Some(e) = d.pop_front() {
                        if removed < limit && &*e == value {
                            removed += 1;
                        } else {
                            keep.push_back(e);
                        }
                    }
                }
                *d = keep;
            }
        }
        self.redis_bytes -= removed * redis_lp_entry_size(value);
        if removed > 0 {
            self.shrink_check(lim);
        }
        removed
    }

    /// `LTRIM`: keeps only elements in the inclusive range `[start, end]`
    /// (already normalized by the caller), or nothing when `None`.
    pub fn trim(&mut self, keep: Option<(usize, usize)>, lim: &Limits) {
        let len = self.len();
        let (s, e) = keep.unwrap_or((0, 0));
        let removed: usize = self
            .iter()
            .enumerate()
            .filter(|(i, _)| *i < s || *i >= e)
            .map(|(_, v)| redis_lp_entry_size(v))
            .sum();
        match &mut self.enc {
            Enc::Packed(lp) => {
                if e < len
                    && let Some(p) = lp.seek(e as i64)
                {
                    lp.remove_n_at(p, len - e);
                }
                if s > 0
                    && let Some(p) = lp.first()
                {
                    lp.remove_n_at(p, s);
                }
            }
            Enc::Deque(d) => {
                d.truncate(e);
                d.drain(..s.min(d.len()));
                d.shrink_to_fit();
            }
        }
        self.redis_bytes -= removed;
        self.shrink_check(lim);
    }
}

/// Head-to-tail iterator over a [`List`].
pub enum ListIter<'a> {
    /// Listpack encoding.
    Packed(ListpackIter<'a>),
    /// Deque encoding.
    Deque(std::collections::vec_deque::Iter<'a, Sds>),
}

impl<'a> Iterator for ListIter<'a> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            ListIter::Packed(i) => i.next(),
            ListIter::Deque(i) => i.next().map(|s| s.as_bytes()),
        }
    }
    fn nth(&mut self, n: usize) -> Option<&'a [u8]> {
        match self {
            ListIter::Packed(i) => i.nth(n),
            ListIter::Deque(i) => i.nth(n).map(|s| s.as_bytes()),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            ListIter::Packed(i) => i.size_hint(),
            ListIter::Deque(i) => i.size_hint(),
        }
    }
}

impl DoubleEndedIterator for ListIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            ListIter::Packed(i) => i.next_back(),
            ListIter::Deque(i) => i.next_back().map(|s| s.as_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn lim() -> Limits {
        Limits::default()
    }

    #[test]
    fn converts_on_size() {
        let mut l = List::new();
        l.push(&[b"a", b"b"], false, &lim());
        assert_eq!(l.encoding_name(), "listpack");
        let big = vec![b'x'; 5000];
        l.push(&[&big, &big], false, &lim());
        assert_eq!(l.encoding_name(), "quicklist");
        l.pop(false);
        l.pop(false);
        l.shrink_check(&lim());
        assert_eq!(l.encoding_name(), "listpack");
        assert_eq!(l.iter().collect::<Vec<_>>(), vec![&b"a"[..], b"b"]);
    }

    #[test]
    fn insert_remove_trim() {
        let mut l = List::new();
        l.push(&[b"a", b"b", b"c", b"b"], false, &lim());
        assert_eq!(l.insert(true, b"b", b"X", &lim()), Some(5));
        assert_eq!(l.insert(false, b"zz", b"X", &lim()), None);
        assert_eq!(l.remove(-1, b"b", &lim()), 1);
        assert_eq!(
            l.iter().collect::<Vec<_>>(),
            vec![&b"a"[..], b"b", b"X", b"c"]
        );
        l.trim(Some((1, 3)), &lim());
        assert_eq!(l.iter().collect::<Vec<_>>(), vec![&b"b"[..], b"X"]);
        assert!(l.set(-1, b"Y", &lim()));
        assert!(!l.set(5, b"Y", &lim()));
        assert_eq!(l.get(1), Some(&b"Y"[..]));
    }

    #[derive(Debug, Clone)]
    enum Op {
        Push(bool, Vec<u8>),
        Pop(bool),
        Set(i8, Vec<u8>),
        Rem(i8, Vec<u8>),
        Trim(i8, i8),
        Insert(bool, Vec<u8>, Vec<u8>),
    }

    fn val() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            8 => proptest::sample::select(vec![b"a".to_vec(), b"b".to_vec(), b"1".to_vec(), b"300".to_vec()]),
            1 => Just(vec![b'z'; 3000]),
        ]
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<bool>(), val()).prop_map(|(f, v)| Op::Push(f, v)),
            any::<bool>().prop_map(Op::Pop),
            (any::<i8>(), val()).prop_map(|(i, v)| Op::Set(i, v)),
            (-2i8..3, val()).prop_map(|(c, v)| Op::Rem(c, v)),
            (any::<i8>(), any::<i8>()).prop_map(|(a, b)| Op::Trim(a, b)),
            (any::<bool>(), val(), val()).prop_map(|(a, p, v)| Op::Insert(a, p, v)),
        ]
    }

    proptest! {
        /// Lists agree with a `VecDeque<Vec<u8>>` model across encoding
        /// conversions, and the tracked Redis listpack size stays exact.
        #[test]
        fn matches_model(ops in proptest::collection::vec(op(), 1..120), fill in prop_oneof![Just(-2i64), Just(-1), Just(4)]) {
            let lim = Limits { list_max_listpack_size: fill, ..Limits::default() };
            let mut l = List::new();
            let mut m: VecDeque<Vec<u8>> = VecDeque::new();
            for op in ops {
                match op {
                    Op::Push(front, v) => {
                        l.push(&[&v], front, &lim);
                        if front { m.push_front(v) } else { m.push_back(v) }
                    }
                    Op::Pop(front) => {
                        let got = l.pop(front).map(|s| s.to_vec());
                        let want = if front { m.pop_front() } else { m.pop_back() };
                        prop_assert_eq!(got, want);
                        l.shrink_check(&lim);
                    }
                    Op::Set(i, v) => {
                        let len = m.len() as i64;
                        let idx = if i < 0 { len + i as i64 } else { i as i64 };
                        let ok = l.set(i as i64, &v, &lim);
                        prop_assert_eq!(ok, idx >= 0 && idx < len);
                        if ok { m[idx as usize] = v; }
                    }
                    Op::Rem(c, v) => {
                        let got = l.remove(c as i64, &v, &lim);
                        let mut want = 0;
                        let limit = if c == 0 { usize::MAX } else { c.unsigned_abs() as usize };
                        if c < 0 {
                            let mut i = m.len();
                            while i > 0 && want < limit { i -= 1; if m[i] == v { m.remove(i); want += 1; } }
                        } else {
                            let mut i = 0;
                            while i < m.len() && want < limit { if m[i] == v { m.remove(i); want += 1; } else { i += 1; } }
                        }
                        prop_assert_eq!(got, want);
                    }
                    Op::Trim(a, b) => {
                        let r = crate::util::normalize_range(a as i64, b as i64, m.len());
                        l.trim(r, &lim);
                        m = match r { Some((s, e)) => m.into_iter().skip(s).take(e - s).collect(), None => VecDeque::new() };
                    }
                    Op::Insert(after, p, v) => {
                        let got = l.insert(after, &p, &v, &lim);
                        let want = m.iter().position(|e| *e == p).map(|i| { m.insert(if after { i + 1 } else { i }, v.clone()); m.len() });
                        prop_assert_eq!(got, want);
                    }
                }
                let got: Vec<Vec<u8>> = l.iter().map(|e| e.to_vec()).collect();
                prop_assert_eq!(&got, &m.iter().cloned().collect::<Vec<_>>());
                let want_bytes = REDIS_LP_OVERHEAD + m.iter().map(|e| redis_lp_entry_size(e)).sum::<usize>();
                prop_assert_eq!(l.redis_bytes, want_bytes);
            }
        }
    }
}
