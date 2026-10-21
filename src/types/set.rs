//! Sets: an [`IntSet`] while every member is an integer, a listpack while
//! small, a [`Dict<()>`](Dict) afterwards. Conversion rules follow Redis 7.2+
//! (`setTypeCreate`, `setTypeAdd`, `setTypeMaybeConvert`).

use super::{Limits, as_int};
use crate::dict::Dict;
use crate::intset::IntSet;
use crate::listpack::Listpack;
use crate::sds::Sds;

/// A set value.
pub enum Set {
    /// All-integer small set.
    Ints(IntSet),
    /// Small set of arbitrary members, insertion ordered.
    Packed(Listpack),
    /// Hash table encoding.
    Table(Dict<()>),
}

impl Default for Set {
    fn default() -> Self {
        Set::Ints(IntSet::new())
    }
}

fn digits10(v: i64) -> usize {
    let mut b = itoa::Buffer::new();
    b.format(v).len()
}

impl Set {
    /// An empty intset-encoded set (what `SINTERSTORE` & co. start from).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a set sized for `size_hint` members, the first being `first`.
    pub fn create(first: &[u8], size_hint: usize, lim: &Limits) -> Self {
        if as_int(first).is_some() && size_hint <= lim.set_max_intset_entries {
            Set::Ints(IntSet::new())
        } else if size_hint <= lim.set_max_listpack_entries {
            Set::Packed(Listpack::new())
        } else {
            Set::Table(Dict::new())
        }
    }

    /// Converts to a hash table up front when a bulk add is known to exceed
    /// the small-encoding limits.
    pub fn maybe_convert(&mut self, size_hint: usize, lim: &Limits) {
        let convert = match self {
            Set::Packed(_) => size_hint > lim.set_max_listpack_entries,
            Set::Ints(_) => size_hint > lim.set_max_intset_entries,
            Set::Table(_) => false,
        };
        if convert {
            self.convert_to_table();
        }
    }

    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            Set::Ints(s) => s.len(),
            Set::Packed(lp) => lp.len(),
            Set::Table(d) => d.len(),
        }
    }

    /// True if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING` name.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            Set::Ints(_) => "intset",
            Set::Packed(_) => "listpack",
            Set::Table(_) => "hashtable",
        }
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Set::Ints(s) => s.bytes(),
            Set::Packed(lp) => lp.bytes(),
            Set::Table(d) => {
                d.overhead_bytes() + d.iter().map(|(k, _)| k.heap_size()).sum::<usize>()
            }
        }
    }

    fn convert_to_table(&mut self) {
        let mut d = Dict::new();
        self.for_each(|m| {
            d.insert(Sds::from_slice(m), ());
        });
        *self = Set::Table(d);
    }

    fn convert_to_packed(&mut self) {
        let mut lp = Listpack::new();
        self.for_each(|m| lp.push_back(m));
        *self = Set::Packed(lp);
    }

    /// True if `m` is a member.
    pub fn contains(&self, m: &[u8]) -> bool {
        match self {
            Set::Ints(s) => as_int(m).is_some_and(|v| s.contains(v)),
            Set::Packed(lp) => lp.first().and_then(|p| lp.find_from(p, m, 1)).is_some(),
            Set::Table(d) => d.contains_key(m),
        }
    }

    /// Adds a member; returns true if it was not present.
    pub fn add(&mut self, m: &[u8], lim: &Limits) -> bool {
        match self {
            Set::Ints(s) => {
                if let Some(v) = as_int(m) {
                    let added = s.insert(v);
                    if added && s.len() > lim.set_max_intset_entries {
                        self.convert_to_table();
                    }
                    return added;
                }
                let n = s.len();
                let maxelelen = if n == 0 {
                    0
                } else {
                    digits10(s.get(0)).max(digits10(s.get(n - 1)))
                };
                if n < lim.set_max_listpack_entries
                    && m.len() <= lim.set_max_listpack_value
                    && maxelelen <= lim.set_max_listpack_value
                {
                    self.convert_to_packed();
                } else {
                    self.convert_to_table();
                }
                self.add(m, lim)
            }
            Set::Packed(lp) => {
                if lp.first().and_then(|p| lp.find_from(p, m, 1)).is_some() {
                    return false;
                }
                if lp.len() < lim.set_max_listpack_entries && m.len() <= lim.set_max_listpack_value
                {
                    lp.push_back(m);
                    true
                } else {
                    self.convert_to_table();
                    self.add(m, lim)
                }
            }
            Set::Table(d) => {
                if d.contains_key(m) {
                    false
                } else {
                    d.insert_new_h(crate::dict::hash_key(m), Sds::from_slice(m), ());
                    true
                }
            }
        }
    }

    /// Removes a member; returns true if it was present.
    pub fn remove(&mut self, m: &[u8]) -> bool {
        match self {
            Set::Ints(s) => as_int(m).is_some_and(|v| s.remove(v)),
            Set::Packed(lp) => match lp.first().and_then(|p| lp.find_from(p, m, 1)) {
                Some(p) => {
                    lp.remove_at(p);
                    true
                }
                None => false,
            },
            Set::Table(d) => d.remove(m).is_some(),
        }
    }

    /// Calls `f` for every member.
    pub fn for_each(&self, mut f: impl FnMut(&[u8])) {
        match self {
            Set::Ints(s) => {
                let mut b = itoa::Buffer::new();
                for v in s.iter() {
                    f(b.format(v).as_bytes());
                }
            }
            Set::Packed(lp) => lp.iter().for_each(f),
            Set::Table(d) => d.iter().for_each(|(k, _)| f(k)),
        }
    }

    /// All members, owned.
    pub fn members(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(self.len());
        self.for_each(|m| out.push(m.to_vec()));
        out
    }

    /// A uniformly random member (approximately uniform for hash tables).
    pub fn random_member(&self) -> Option<Vec<u8>> {
        match self {
            Set::Ints(s) => {
                if s.is_empty() {
                    None
                } else {
                    Some(
                        s.get(crate::util::rand_below(s.len() as u64) as usize)
                            .to_string()
                            .into_bytes(),
                    )
                }
            }
            Set::Packed(lp) => {
                if lp.is_empty() {
                    None
                } else {
                    let i = crate::util::rand_below(lp.len() as u64) as i64;
                    lp.seek(i).map(|p| lp.get(p).to_vec())
                }
            }
            Set::Table(d) => d.random_entry().map(|(k, _)| k.to_vec()),
        }
    }

    /// Removes and returns a random member.
    pub fn pop_random(&mut self) -> Option<Vec<u8>> {
        let m = self.random_member()?;
        self.remove(&m);
        Some(m)
    }

    /// One `SSCAN` step.
    pub fn scan(&self, cursor: u64, count: usize, mut f: impl FnMut(&[u8])) -> u64 {
        match self {
            Set::Table(d) => {
                let mut c = cursor;
                let mut visited = 0;
                let mut iterations = count * 10;
                loop {
                    c = d.scan(c, |k, _| {
                        visited += 1;
                        f(k)
                    });
                    iterations = iterations.saturating_sub(1);
                    if c == 0 || visited >= count || iterations == 0 {
                        return c;
                    }
                }
            }
            _ => {
                self.for_each(f);
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn encoding_transitions() {
        let lim = Limits::default();
        let mut s = Set::create(b"1", 1, &lim);
        assert!(s.add(b"1", &lim));
        assert!(s.add(b"2", &lim));
        assert_eq!(s.encoding_name(), "intset");
        assert!(s.add(b"abc", &lim));
        assert_eq!(s.encoding_name(), "listpack");
        assert!(s.contains(b"1") && s.contains(b"abc"));
        assert!(s.add(&[b'x'; 65], &lim));
        assert_eq!(s.encoding_name(), "hashtable");
        assert_eq!(s.len(), 4);

        let mut s = Set::new();
        for i in 0..=512 {
            s.add(i.to_string().as_bytes(), &lim);
        }
        assert_eq!(s.encoding_name(), "hashtable");
    }

    proptest! {
        #[test]
        fn matches_btreeset(ops in proptest::collection::vec((0u8..3, prop_oneof![
            (0i32..300).prop_map(|i| i.to_string()),
            "[a-c]{1,3}",
        ]), 0..400)) {
            let lim = Limits { set_max_intset_entries: 20, set_max_listpack_entries: 10, ..Limits::default() };
            let mut s = Set::new();
            let mut m = BTreeSet::new();
            for (op, v) in ops {
                let v = v.into_bytes();
                match op {
                    0 | 1 => prop_assert_eq!(s.add(&v, &lim), m.insert(v.clone())),
                    _ => prop_assert_eq!(s.remove(&v), m.remove(&v)),
                }
                prop_assert_eq!(s.contains(&v), m.contains(&v));
                prop_assert_eq!(s.len(), m.len());
            }
            let got: BTreeSet<Vec<u8>> = s.members().into_iter().collect();
            prop_assert_eq!(got, m);
        }
    }
}
