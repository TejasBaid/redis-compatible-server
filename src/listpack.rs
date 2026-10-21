//! `Listpack`: a compact, contiguous encoding for small collections.
//!
//! Like Redis' listpack, entries are packed back-to-back in one allocation and
//! can be walked in both directions:
//!
//! ```text
//! entry := <len: LEB128 varint> <data: len bytes> <backlen: reverse varint>
//! ```
//!
//! `backlen` stores the size of `len + data` so the previous entry can be found
//! from the current position. A 3-byte member costs 5 bytes instead of a
//! 24-byte `Sds` plus hash-table node, which is why small lists, hashes, sets
//! and sorted sets use this encoding until they cross the configured size
//! thresholds (`*-max-listpack-entries` / `*-max-listpack-value`).
//!
//! All operations are O(n) in the number of bytes, which is fine because the
//! encoding is only used while `n` is small.

/// A packed sequence of byte strings. See the module docs.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Listpack {
    buf: Vec<u8>,
    count: u32,
}

#[inline]
fn varint_len(mut v: usize) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

#[inline]
fn write_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn read_varint(buf: &[u8], mut p: usize) -> (usize, usize) {
    let mut v = 0usize;
    let mut shift = 0;
    let start = p;
    loop {
        let b = buf[p];
        v |= ((b & 0x7f) as usize) << shift;
        p += 1;
        if b & 0x80 == 0 {
            return (v, p - start);
        }
        shift += 7;
    }
}

/// Writes `v` so it can be decoded by reading bytes backwards from the end.
fn write_backlen(out: &mut Vec<u8>, v: usize) {
    // Memory order: most significant group first (unflagged, it terminates a
    // backwards read), then lower groups flagged "more bytes before me".
    let n = varint_len(v);
    for i in (0..n).rev() {
        let group = ((v >> (7 * i)) & 0x7f) as u8;
        out.push(if i == n - 1 { group } else { group | 0x80 });
    }
}

/// Reads a backlen that ends just before `end`. Returns (value, encoded size).
fn read_backlen(buf: &[u8], end: usize) -> (usize, usize) {
    let mut v = 0usize;
    let mut p = end;
    let mut shift = 0;
    loop {
        p -= 1;
        let b = buf[p];
        v |= ((b & 0x7f) as usize) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            return (v, end - p);
        }
    }
}

fn encode_entry(out: &mut Vec<u8>, data: &[u8]) {
    write_varint(out, data.len());
    out.extend_from_slice(data);
    write_backlen(out, varint_len(data.len()) + data.len());
}

fn encoded_size(data: &[u8]) -> usize {
    let inner = varint_len(data.len()) + data.len();
    inner + varint_len(inner)
}

impl Listpack {
    /// An empty listpack.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// True when there are no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Encoded size in bytes.
    pub fn bytes(&self) -> usize {
        self.buf.len()
    }

    /// Releases excess capacity.
    pub fn shrink_to_fit(&mut self) {
        self.buf.shrink_to_fit();
    }

    /// Byte offset of the first entry, or `None` when empty.
    #[inline]
    pub fn first(&self) -> Option<usize> {
        if self.buf.is_empty() { None } else { Some(0) }
    }

    /// Byte offset of the last entry, or `None` when empty.
    pub fn last(&self) -> Option<usize> {
        if self.buf.is_empty() {
            None
        } else {
            self.prev(self.buf.len())
        }
    }

    /// The entry at byte offset `p`.
    #[inline]
    pub fn get(&self, p: usize) -> &[u8] {
        let (len, h) = read_varint(&self.buf, p);
        &self.buf[p + h..p + h + len]
    }

    #[inline]
    fn entry_end(&self, p: usize) -> usize {
        let (len, h) = read_varint(&self.buf, p);
        let inner = h + len;
        p + inner + varint_len(inner)
    }

    /// Offset of the entry after `p`, or `None` at the end.
    #[inline]
    pub fn next(&self, p: usize) -> Option<usize> {
        let e = self.entry_end(p);
        if e >= self.buf.len() { None } else { Some(e) }
    }

    /// Offset of the entry before `p` (`p` may equal `bytes()`), or `None`.
    pub fn prev(&self, p: usize) -> Option<usize> {
        if p == 0 {
            return None;
        }
        let (inner, bl) = read_backlen(&self.buf, p);
        Some(p - bl - inner)
    }

    /// Offset of the entry at `index` (negative counts from the end).
    pub fn seek(&self, index: i64) -> Option<usize> {
        let len = self.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return None;
        }
        if idx <= len / 2 {
            let mut p = self.first()?;
            for _ in 0..idx {
                p = self.next(p)?;
            }
            Some(p)
        } else {
            let mut p = self.last()?;
            for _ in 0..(len - 1 - idx) {
                p = self.prev(p)?;
            }
            Some(p)
        }
    }

    /// Appends an entry.
    pub fn push_back(&mut self, data: &[u8]) {
        encode_entry(&mut self.buf, data);
        self.count += 1;
    }

    /// Prepends an entry.
    pub fn push_front(&mut self, data: &[u8]) {
        self.insert_at(0, data);
    }

    /// Inserts an entry before the entry at offset `p` (`p == bytes()` appends).
    pub fn insert_at(&mut self, p: usize, data: &[u8]) {
        let mut enc = Vec::with_capacity(encoded_size(data));
        encode_entry(&mut enc, data);
        self.buf.splice(p..p, enc);
        self.count += 1;
    }

    /// Replaces the entry at `p`, returning the offset of the entry after it.
    pub fn replace_at(&mut self, p: usize, data: &[u8]) -> usize {
        let end = self.entry_end(p);
        let mut enc = Vec::with_capacity(encoded_size(data));
        encode_entry(&mut enc, data);
        let new_end = p + enc.len();
        self.buf.splice(p..end, enc);
        new_end
    }

    /// Removes the entry at `p`. Returns the offset of the following entry
    /// (which now lives at `p`), or `None` if it was the last one.
    pub fn remove_at(&mut self, p: usize) -> Option<usize> {
        let end = self.entry_end(p);
        self.buf.drain(p..end);
        self.count -= 1;
        if p < self.buf.len() { Some(p) } else { None }
    }

    /// Removes `n` consecutive entries starting at offset `p`.
    pub fn remove_n_at(&mut self, p: usize, n: usize) {
        let mut end = p;
        for _ in 0..n {
            if end >= self.buf.len() {
                break;
            }
            end = self.entry_end(end);
            self.count -= 1;
        }
        self.buf.drain(p..end);
    }

    /// Removes and returns the first entry.
    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        let p = self.first()?;
        let v = self.get(p).to_vec();
        self.remove_at(p);
        Some(v)
    }

    /// Removes and returns the last entry.
    pub fn pop_back(&mut self) -> Option<Vec<u8>> {
        let p = self.last()?;
        let v = self.get(p).to_vec();
        self.remove_at(p);
        Some(v)
    }

    /// Finds the first entry equal to `data`, visiting every `step`-th entry
    /// starting at offset `p` (step 2 searches only hash fields / zset members).
    pub fn find_from(&self, mut p: usize, data: &[u8], step: usize) -> Option<usize> {
        if p >= self.buf.len() {
            return None;
        }
        loop {
            if self.get(p) == data {
                return Some(p);
            }
            for _ in 0..step {
                p = self.next(p)?;
            }
        }
    }

    /// Forward iterator over entries.
    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            lp: self,
            pos: self.first(),
            back: None,
            remaining: self.len(),
        }
    }

    /// The size an entry would occupy, for threshold checks.
    pub fn entry_size(data: &[u8]) -> usize {
        encoded_size(data)
    }
}

/// Double-ended iterator over a [`Listpack`].
pub struct ListpackIter<'a> {
    lp: &'a Listpack,
    pos: Option<usize>,
    back: Option<usize>,
    remaining: usize,
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        let p = self.pos?;
        self.remaining -= 1;
        self.pos = self.lp.next(p);
        Some(self.lp.get(p))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl DoubleEndedIterator for ListpackIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let p = match self.back {
            None => self.lp.last()?,
            Some(b) => self.lp.prev(b)?,
        };
        self.back = Some(p);
        self.remaining -= 1;
        Some(self.lp.get(p))
    }
}

impl ExactSizeIterator for ListpackIter<'_> {}

impl std::fmt::Debug for Listpack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.iter().map(String::from_utf8_lossy))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn backlen_roundtrip() {
        for v in [0usize, 1, 127, 128, 300, 16383, 16384, 1 << 20, 1 << 30] {
            let mut buf = vec![0xAA];
            write_backlen(&mut buf, v);
            let (got, n) = read_backlen(&buf, buf.len());
            assert_eq!(got, v);
            assert_eq!(n, buf.len() - 1);
        }
    }

    #[test]
    fn push_pop_both_ends() {
        let mut lp = Listpack::new();
        lp.push_back(b"b");
        lp.push_front(b"a");
        lp.push_back(&[b'x'; 300]);
        assert_eq!(lp.len(), 3);
        let all: Vec<_> = lp.iter().collect();
        assert_eq!(all[0], b"a");
        assert_eq!(all[2].len(), 300);
        let rev: Vec<_> = lp.iter().rev().collect();
        assert_eq!(rev[2], b"a");
        assert_eq!(lp.pop_back().unwrap().len(), 300);
        assert_eq!(lp.pop_front().unwrap(), b"a");
        assert_eq!(lp.pop_front().unwrap(), b"b");
        assert!(lp.pop_front().is_none());
        assert_eq!(lp.bytes(), 0);
    }

    #[derive(Debug, Clone)]
    enum Op {
        PushBack(Vec<u8>),
        PushFront(Vec<u8>),
        PopBack,
        PopFront,
        Insert(usize, Vec<u8>),
        Remove(usize),
        Replace(usize, Vec<u8>),
        RemoveN(usize, usize),
    }

    fn data() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..8),
            proptest::collection::vec(any::<u8>(), 120..200),
        ]
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            data().prop_map(Op::PushBack),
            data().prop_map(Op::PushFront),
            Just(Op::PopBack),
            Just(Op::PopFront),
            (any::<usize>(), data()).prop_map(|(i, d)| Op::Insert(i, d)),
            any::<usize>().prop_map(Op::Remove),
            (any::<usize>(), data()).prop_map(|(i, d)| Op::Replace(i, d)),
            (any::<usize>(), 0usize..4).prop_map(|(i, n)| Op::RemoveN(i, n)),
        ]
    }

    proptest! {
        /// Every operation agrees with a `Vec<Vec<u8>>` model, and forward,
        /// backward and indexed traversal all see the same sequence.
        #[test]
        fn matches_vec_model(ops in proptest::collection::vec(op(), 1..100)) {
            let mut lp = Listpack::new();
            let mut m: Vec<Vec<u8>> = Vec::new();
            for op in ops {
                match op {
                    Op::PushBack(d) => { lp.push_back(&d); m.push(d); }
                    Op::PushFront(d) => { lp.push_front(&d); m.insert(0, d); }
                    Op::PopBack => prop_assert_eq!(lp.pop_back(), m.pop()),
                    Op::PopFront => {
                        let want = if m.is_empty() { None } else { Some(m.remove(0)) };
                        prop_assert_eq!(lp.pop_front(), want);
                    }
                    Op::Insert(i, d) => {
                        let i = i % (m.len() + 1);
                        let p = lp.seek(i as i64).unwrap_or(lp.bytes());
                        lp.insert_at(p, &d);
                        m.insert(i, d);
                    }
                    Op::Remove(i) if !m.is_empty() => {
                        let i = i % m.len();
                        let p = lp.seek(i as i64).unwrap();
                        lp.remove_at(p);
                        m.remove(i);
                    }
                    Op::Replace(i, d) if !m.is_empty() => {
                        let i = i % m.len();
                        let p = lp.seek(i as i64).unwrap();
                        lp.replace_at(p, &d);
                        m[i] = d;
                    }
                    Op::RemoveN(i, n) if !m.is_empty() => {
                        let i = i % m.len();
                        let p = lp.seek(i as i64).unwrap();
                        lp.remove_n_at(p, n);
                        let end = (i + n).min(m.len());
                        m.drain(i..end);
                    }
                    _ => {}
                }
                prop_assert_eq!(lp.len(), m.len());
            }
            let fwd: Vec<Vec<u8>> = lp.iter().map(|e| e.to_vec()).collect();
            prop_assert_eq!(&fwd, &m);
            let mut back: Vec<Vec<u8>> = lp.iter().rev().map(|e| e.to_vec()).collect();
            back.reverse();
            prop_assert_eq!(&back, &m);
            for (i, want) in m.iter().enumerate() {
                prop_assert_eq!(lp.get(lp.seek(i as i64).unwrap()), &want[..]);
                prop_assert_eq!(lp.get(lp.seek(i as i64 - m.len() as i64).unwrap()), &want[..]);
            }
        }
    }
}
