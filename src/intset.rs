//! `IntSet`: a sorted array of integers using the narrowest width that fits.
//!
//! Small sets whose members are all integers (very common: ids, flags) are
//! stored as a sorted array of 16, 32 or 64-bit integers, upgrading the width
//! on demand, exactly like Redis' `intset.c`. Lookups are binary searches.

/// A compact sorted set of `i64`. See the module docs.
#[derive(Clone, PartialEq, Eq)]
pub struct IntSet {
    width: u8,
    data: Vec<u8>,
}

impl Default for IntSet {
    fn default() -> Self {
        IntSet {
            width: 2,
            data: Vec::new(),
        }
    }
}

fn width_for(v: i64) -> u8 {
    if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
        2
    } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        4
    } else {
        8
    }
}

impl IntSet {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of members.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() / self.width as usize
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Bytes used by the encoded array.
    pub fn bytes(&self) -> usize {
        self.data.len()
    }

    /// Current integer width in bytes (2, 4 or 8).
    pub fn width(&self) -> u8 {
        self.width
    }

    /// The `i`-th smallest member.
    #[inline]
    pub fn get(&self, i: usize) -> i64 {
        let w = self.width as usize;
        let s = &self.data[i * w..i * w + w];
        match w {
            2 => i16::from_le_bytes([s[0], s[1]]) as i64,
            4 => i32::from_le_bytes([s[0], s[1], s[2], s[3]]) as i64,
            _ => i64::from_le_bytes(s.try_into().expect("8 bytes")),
        }
    }

    fn put(&mut self, i: usize, v: i64) {
        let w = self.width as usize;
        let dst = &mut self.data[i * w..i * w + w];
        match w {
            2 => dst.copy_from_slice(&(v as i16).to_le_bytes()),
            4 => dst.copy_from_slice(&(v as i32).to_le_bytes()),
            _ => dst.copy_from_slice(&v.to_le_bytes()),
        }
    }

    fn search(&self, v: i64) -> Result<usize, usize> {
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.get(mid).cmp(&v) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// True if `v` is a member.
    pub fn contains(&self, v: i64) -> bool {
        width_for(v) <= self.width && self.search(v).is_ok()
    }

    /// Adds `v`; returns false if it was already present.
    pub fn insert(&mut self, v: i64) -> bool {
        let w = width_for(v);
        if w > self.width {
            self.upgrade(w);
        }
        match self.search(v) {
            Ok(_) => false,
            Err(pos) => {
                let w = self.width as usize;
                let at = pos * w;
                self.data.splice(at..at, std::iter::repeat_n(0u8, w));
                self.put(pos, v);
                true
            }
        }
    }

    fn upgrade(&mut self, width: u8) {
        let vals: Vec<i64> = self.iter().collect();
        self.width = width;
        self.data = vec![0; vals.len() * width as usize];
        for (i, v) in vals.into_iter().enumerate() {
            self.put(i, v);
        }
    }

    /// Removes `v`; returns false if it was not present.
    pub fn remove(&mut self, v: i64) -> bool {
        if width_for(v) > self.width {
            return false;
        }
        match self.search(v) {
            Ok(pos) => {
                let w = self.width as usize;
                self.data.drain(pos * w..pos * w + w);
                true
            }
            Err(_) => false,
        }
    }

    /// Iterates members in ascending order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = i64> + ExactSizeIterator + '_ {
        (0..self.len()).map(move |i| self.get(i))
    }
}

impl std::fmt::Debug for IntSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn upgrades_width() {
        let mut s = IntSet::new();
        assert!(s.insert(5));
        assert!(s.insert(-3));
        assert_eq!(s.width(), 2);
        assert!(s.insert(100_000));
        assert_eq!(s.width(), 4);
        assert!(s.insert(i64::MIN));
        assert_eq!(s.width(), 8);
        assert!(!s.insert(5));
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![i64::MIN, -3, 5, 100_000]);
        assert!(s.contains(100_000));
        assert!(!s.contains(6));
        assert!(s.remove(-3));
        assert!(!s.remove(-3));
        assert_eq!(s.len(), 3);
    }

    proptest! {
        #[test]
        fn matches_btreeset(ops in proptest::collection::vec((any::<bool>(), prop_oneof![
            -40i64..40, any::<i16>().prop_map(i64::from), any::<i32>().prop_map(i64::from), any::<i64>()
        ]), 0..300)) {
            let mut s = IntSet::new();
            let mut m = BTreeSet::new();
            for (ins, v) in ops {
                if ins {
                    prop_assert_eq!(s.insert(v), m.insert(v));
                } else {
                    prop_assert_eq!(s.remove(v), m.remove(&v));
                }
                prop_assert_eq!(s.contains(v), m.contains(&v));
            }
            prop_assert_eq!(s.iter().collect::<Vec<_>>(), m.into_iter().collect::<Vec<_>>());
        }
    }
}
