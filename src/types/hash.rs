//! Hashes: a listpack of alternating fields and values while small
//! (`hash-max-listpack-entries`/`-value`), a [`Dict`] afterwards.

use super::Limits;
use crate::dict::Dict;
use crate::listpack::Listpack;
use crate::sds::Sds;

/// A hash value.
pub enum Hash {
    /// `field, value, field, value, ...`
    Packed(Listpack),
    /// Hash table encoding.
    Table(Dict<Sds>),
}

impl Default for Hash {
    fn default() -> Self {
        Hash::Packed(Listpack::new())
    }
}

impl Hash {
    /// An empty hash.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of fields.
    pub fn len(&self) -> usize {
        match self {
            Hash::Packed(lp) => lp.len() / 2,
            Hash::Table(d) => d.len(),
        }
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING` name.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            Hash::Packed(_) => "listpack",
            Hash::Table(_) => "hashtable",
        }
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Hash::Packed(lp) => lp.bytes(),
            Hash::Table(d) => {
                d.overhead_bytes()
                    + d.iter()
                        .map(|(k, v)| k.heap_size() + v.heap_size())
                        .sum::<usize>()
            }
        }
    }

    fn convert(&mut self) {
        if let Hash::Packed(lp) = self {
            let mut d = Dict::new();
            let mut it = lp.iter();
            while let (Some(f), Some(v)) = (it.next(), it.next()) {
                d.insert(Sds::from_slice(f), Sds::from_slice(v));
            }
            *self = Hash::Table(d);
        }
    }

    /// Pre-converts before a multi-field write (`hashTypeTryConversion`):
    /// `args` are the field/value arguments about to be written.
    pub fn try_conversion(&mut self, args: &[&[u8]], lim: &Limits) {
        if let Hash::Packed(_) = self
            && (args.len() / 2 > lim.hash_max_listpack_entries
                || args.iter().any(|a| a.len() > lim.hash_max_listpack_value))
        {
            self.convert();
        }
    }

    /// Looks up a field.
    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        match self {
            Hash::Packed(lp) => {
                let p = lp.find_from(lp.first()?, field, 2)?;
                let v = lp.next(p).expect("value follows field");
                Some(lp.get(v))
            }
            Hash::Table(d) => d.get(field).map(|v| v.as_bytes()),
        }
    }

    /// True if `field` exists.
    pub fn contains(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    /// Sets a field; returns true if the field is new.
    pub fn set(&mut self, field: &[u8], value: &[u8], lim: &Limits) -> bool {
        if let Hash::Packed(_) = self
            && (field.len() > lim.hash_max_listpack_value
                || value.len() > lim.hash_max_listpack_value)
        {
            self.convert();
        }
        match self {
            Hash::Packed(lp) => {
                if let Some(p) = lp.first().and_then(|f| lp.find_from(f, field, 2)) {
                    let v = lp.next(p).expect("value follows field");
                    lp.replace_at(v, value);
                    false
                } else {
                    lp.push_back(field);
                    lp.push_back(value);
                    if lp.len() / 2 > lim.hash_max_listpack_entries {
                        self.convert();
                    }
                    true
                }
            }
            Hash::Table(d) => d
                .insert(Sds::from_slice(field), Sds::from_slice(value))
                .is_none(),
        }
    }

    /// Deletes a field; returns true if it existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self {
            Hash::Packed(lp) => match lp.first().and_then(|f| lp.find_from(f, field, 2)) {
                Some(p) => {
                    lp.remove_n_at(p, 2);
                    true
                }
                None => false,
            },
            Hash::Table(d) => d.remove(field).is_some(),
        }
    }

    /// Calls `f(field, value)` for every pair.
    pub fn for_each(&self, mut f: impl FnMut(&[u8], &[u8])) {
        match self {
            Hash::Packed(lp) => {
                let mut it = lp.iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    f(k, v);
                }
            }
            Hash::Table(d) => {
                for (k, v) in d.iter() {
                    f(k, v);
                }
            }
        }
    }

    /// Collects all pairs (owned).
    pub fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::with_capacity(self.len());
        self.for_each(|k, v| out.push((k.to_vec(), v.to_vec())));
        out
    }

    /// One `HSCAN` step: visits pairs and returns the next cursor. Small
    /// encodings are returned in one call with cursor 0, like Redis.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(&[u8], &[u8])) -> u64 {
        match self {
            Hash::Packed(_) => {
                self.for_each(f);
                0
            }
            Hash::Table(d) => {
                let mut c = cursor;
                let mut visited = 0;
                let mut iterations = count * 10;
                loop {
                    c = d.scan(c, |k, v| {
                        visited += 1;
                        f(k, v)
                    });
                    iterations = iterations.saturating_sub(1);
                    if c == 0 || visited >= count || iterations == 0 {
                        return c;
                    }
                }
            }
        }
    }

    /// A random field/value pair.
    pub fn random_pair(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Hash::Packed(lp) => {
                if lp.is_empty() {
                    return None;
                }
                let i = crate::util::rand_below((lp.len() / 2) as u64) as i64;
                let p = lp.seek(i * 2)?;
                Some((lp.get(p).to_vec(), lp.get(lp.next(p)?).to_vec()))
            }
            Hash::Table(d) => d.random_entry().map(|(k, v)| (k.to_vec(), v.to_vec())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    #[test]
    fn converts_on_long_value_and_count() {
        let lim = Limits::default();
        let mut h = Hash::new();
        assert!(h.set(b"f", b"v", &lim));
        assert!(!h.set(b"f", b"v2", &lim));
        assert_eq!(h.get(b"f"), Some(&b"v2"[..]));
        assert_eq!(h.encoding_name(), "listpack");
        h.set(b"g", &[b'x'; 65], &lim);
        assert_eq!(h.encoding_name(), "hashtable");
        assert_eq!(h.get(b"f"), Some(&b"v2"[..]));

        let mut h = Hash::new();
        for i in 0..=128 {
            h.set(format!("f{i}").as_bytes(), b"v", &lim);
        }
        assert_eq!(h.encoding_name(), "hashtable");
        assert_eq!(h.len(), 129);
    }

    proptest! {
        #[test]
        fn matches_btreemap(ops in proptest::collection::vec((any::<bool>(), 0u8..40, 0u8..3), 0..300)) {
            let lim = Limits { hash_max_listpack_entries: 16, ..Limits::default() };
            let mut h = Hash::new();
            let mut m = BTreeMap::new();
            for (set, f, v) in ops {
                let f = format!("field{f}").into_bytes();
                if set {
                    let v = vec![b'v'; v as usize * 40];
                    prop_assert_eq!(h.set(&f, &v, &lim), m.insert(f.clone(), v).is_none());
                } else {
                    prop_assert_eq!(h.remove(&f), m.remove(&f).is_some());
                }
                prop_assert_eq!(h.len(), m.len());
                prop_assert_eq!(h.get(&f).map(|x| x.to_vec()), m.get(&f).cloned());
            }
            let mut got = h.pairs();
            got.sort();
            prop_assert_eq!(got, m.into_iter().collect::<Vec<_>>());
        }
    }
}
