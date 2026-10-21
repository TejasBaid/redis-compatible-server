//! Redis value types and their memory-efficient encodings.
//!
//! | Type   | Small encoding                  | Large encoding               |
//! |--------|---------------------------------|------------------------------|
//! | string | `int` (i64, no allocation)      | `Sds` (inline ≤ 22 bytes)    |
//! | list   | listpack                        | `VecDeque<Sds>`              |
//! | hash   | listpack (field, value, ...)    | [`Dict`](crate::dict::Dict)  |
//! | set    | intset or listpack              | `Dict<()>`                   |
//! | zset   | listpack (member, score, ...)   | skiplist + `Dict<f64>`       |
//! | stream | —                               | `BTreeMap<StreamId, fields>` |
//!
//! Conversion thresholds follow Redis' defaults and are configurable at runtime
//! through [`Limits`] (`CONFIG SET hash-max-listpack-entries ...`).

pub mod hash;
pub mod list;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

pub use hash::Hash;
pub use list::List;
pub use set::Set;
pub use stream::Stream;
pub use string::RStr;
pub use zset::ZSet;

/// A value stored under a key.
pub enum Value {
    /// A string (or integer-encoded string).
    Str(RStr),
    /// A list.
    List(Box<List>),
    /// A hash.
    Hash(Box<Hash>),
    /// A set.
    Set(Box<Set>),
    /// A sorted set.
    ZSet(Box<ZSet>),
    /// A stream.
    Stream(Box<Stream>),
}

impl Value {
    /// The name reported by `TYPE`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
        }
    }

    /// The name reported by `OBJECT ENCODING`.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            Value::Str(s) => s.encoding_name(),
            Value::List(l) => l.encoding_name(),
            Value::Hash(h) => h.encoding_name(),
            Value::Set(s) => s.encoding_name(),
            Value::ZSet(z) => z.encoding_name(),
            Value::Stream(_) => "stream",
        }
    }

    /// A string value from bytes, integer-encoded when possible.
    pub fn string(bytes: &[u8]) -> Value {
        Value::Str(RStr::from_bytes(bytes))
    }

    /// True for collection values that have become empty (and must be
    /// deleted, since Redis never keeps empty aggregates).
    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::Str(_) | Value::Stream(_) => false,
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
        }
    }

    /// Approximate heap bytes owned by this value (for `MEMORY USAGE`).
    pub fn heap_bytes(&self) -> usize {
        match self {
            Value::Str(s) => s.heap_bytes(),
            Value::List(l) => l.heap_bytes(),
            Value::Hash(h) => h.heap_bytes(),
            Value::Set(s) => s.heap_bytes(),
            Value::ZSet(z) => z.heap_bytes(),
            Value::Stream(s) => s.heap_bytes(),
        }
    }
}

/// Encoding conversion thresholds (Redis config names in the field docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// `list-max-listpack-size` (negative: byte limit class, positive: count).
    pub list_max_listpack_size: i64,
    /// `hash-max-listpack-entries`.
    pub hash_max_listpack_entries: usize,
    /// `hash-max-listpack-value`.
    pub hash_max_listpack_value: usize,
    /// `set-max-intset-entries`.
    pub set_max_intset_entries: usize,
    /// `set-max-listpack-entries`.
    pub set_max_listpack_entries: usize,
    /// `set-max-listpack-value`.
    pub set_max_listpack_value: usize,
    /// `zset-max-listpack-entries`.
    pub zset_max_listpack_entries: usize,
    /// `zset-max-listpack-value`.
    pub zset_max_listpack_value: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            list_max_listpack_size: -2,
            hash_max_listpack_entries: 128,
            hash_max_listpack_value: 64,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
        }
    }
}

/// Size, in bytes, that Redis' own listpack would use for `data`. Used only to
/// decide encoding conversions at the same points Redis does.
pub fn redis_lp_entry_size(data: &[u8]) -> usize {
    fn backlen(l: usize) -> usize {
        match l {
            0..=127 => 1,
            128..=16382 => 2,
            16383..=2097150 => 3,
            2097151..=268435454 => 4,
            _ => 5,
        }
    }
    if data.len() <= 20
        && let Some(v) = crate::util::parse_i64(data)
    {
        let enc = if (0..=127).contains(&v) {
            1
        } else if (-4096..=4095).contains(&v) {
            2
        } else if (i16::MIN as i64..=i16::MAX as i64).contains(&v) {
            3
        } else if (-(1 << 23)..(1 << 23)).contains(&v) {
            4
        } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
            5
        } else {
            9
        };
        return enc + backlen(enc);
    }
    let l = data.len();
    let hdr = if l < 64 {
        1
    } else if l < 4096 {
        2
    } else {
        5
    };
    hdr + l + backlen(hdr + l)
}

/// Header + terminator bytes of a Redis listpack.
pub const REDIS_LP_OVERHEAD: usize = 7;

/// True if `s` would be stored as an integer by Redis
/// (`isSdsRepresentableAsLongLong`).
pub fn as_int(s: &[u8]) -> Option<i64> {
    crate::util::parse_i64(s)
}

/// Deep-copies a value (`COPY`).
pub fn clone_value(v: &Value, lim: &Limits) -> Value {
    match v {
        Value::Str(s) => Value::Str(s.clone()),
        Value::List(l) => {
            let mut n = List::new();
            let items: Vec<&[u8]> = l.iter().collect();
            n.push(&items, false, lim);
            Value::List(Box::new(n))
        }
        Value::Hash(h) => {
            let mut n = Hash::new();
            let mut args = Vec::new();
            h.for_each(|f, v| {
                args.push(f.to_vec());
                args.push(v.to_vec());
            });
            let refs: Vec<&[u8]> = args.iter().map(|a| &a[..]).collect();
            n.try_conversion(&refs, lim);
            for kv in refs.chunks(2) {
                n.set(kv[0], kv[1], lim);
            }
            Value::Hash(Box::new(n))
        }
        Value::Set(s) => {
            let mut n = Set::new();
            s.for_each(|m| {
                n.add(m, lim);
            });
            Value::Set(Box::new(n))
        }
        Value::ZSet(z) => {
            let mut max_len = 0;
            z.for_each(|m, _| max_len = max_len.max(m.len()));
            let mut n = ZSet::create(z.len(), max_len, lim);
            z.for_each(|m, s| n.insert(m, s, lim));
            Value::ZSet(Box::new(n))
        }
        Value::Stream(s) => {
            let mut n = Stream::new();
            for (id, fields) in s.iter() {
                n.append(id, fields.to_vec());
            }
            n.last_id = s.last_id;
            n.entries_added = s.entries_added;
            n.max_deleted_id = s.max_deleted_id;
            Value::Stream(Box::new(n))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_is_compact() {
        assert_eq!(std::mem::size_of::<RStr>(), 24);
        assert!(
            std::mem::size_of::<Value>() <= 32,
            "Value is {} bytes",
            std::mem::size_of::<Value>()
        );
    }

    #[test]
    fn redis_sizes() {
        assert_eq!(redis_lp_entry_size(b"5"), 2);
        assert_eq!(redis_lp_entry_size(b"abc"), 5);
        assert_eq!(redis_lp_entry_size(&[b'x'; 100]), 2 + 100 + 1);
    }
}
