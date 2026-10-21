//! `Sds`: a compact, immutable-length byte string with small-string
//! optimization.
//!
//! Keys, hash fields, set members and most string values in a Redis workload
//! are short. Storing them inline avoids one heap allocation (and its 16-byte
//! malloc quantum) per key. `Sds` is 24 bytes: up to [`INLINE_CAP`] bytes are
//! stored inline, longer strings live in a boxed slice.

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Maximum number of bytes stored without a heap allocation.
pub const INLINE_CAP: usize = 22;

/// Inline length as an enum with exactly `INLINE_CAP + 1` values. The other
/// 233 bit patterns of this byte are a *niche* the compiler uses to store the
/// discriminants of enclosing enums (`Repr`, `RStr`, `Value`) for free, which
/// keeps `Value` at 24 bytes and a keyspace dict node at exactly 64 bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
#[rustfmt::skip]
enum InlineLen {
    L0, L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11,
    L12, L13, L14, L15, L16, L17, L18, L19, L20, L21, L22,
}

use InlineLen::*;
const LENS: [InlineLen; INLINE_CAP + 1] = [
    L0, L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12, L13, L14, L15, L16, L17, L18, L19, L20,
    L21, L22,
];

#[derive(Clone)]
enum Repr {
    Inline {
        len: InlineLen,
        buf: [u8; INLINE_CAP],
    },
    Heap(Box<[u8]>),
}

/// A compact byte string. See the module docs.
#[derive(Clone)]
pub struct Sds(Repr);

impl Sds {
    /// The empty string.
    pub const fn empty() -> Self {
        Sds(Repr::Inline {
            len: L0,
            buf: [0; INLINE_CAP],
        })
    }

    /// Copies `bytes` into a new `Sds`.
    #[inline]
    pub fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE_CAP {
            let mut buf = [0u8; INLINE_CAP];
            buf[..bytes.len()].copy_from_slice(bytes);
            Sds(Repr::Inline {
                len: LENS[bytes.len()],
                buf,
            })
        } else {
            Sds(Repr::Heap(bytes.into()))
        }
    }

    /// Takes ownership of a `Vec`, avoiding a copy for long strings.
    pub fn from_vec(v: Vec<u8>) -> Self {
        if v.len() <= INLINE_CAP {
            Self::from_slice(&v)
        } else {
            Sds(Repr::Heap(v.into_boxed_slice()))
        }
    }

    /// The bytes of the string.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Repr::Inline { len, buf } => &buf[..*len as usize],
            Repr::Heap(b) => b,
        }
    }

    /// Returns true when the string is stored inline (no heap allocation).
    pub fn is_inline(&self) -> bool {
        matches!(self.0, Repr::Inline { .. })
    }

    /// Bytes of heap memory owned by this string (0 when inline).
    pub fn heap_size(&self) -> usize {
        match &self.0 {
            Repr::Inline { .. } => 0,
            Repr::Heap(b) => b.len(),
        }
    }

    /// Converts into an owned `Vec<u8>`.
    pub fn into_vec(self) -> Vec<u8> {
        match self.0 {
            Repr::Inline { len, buf } => buf[..len as usize].to_vec(),
            Repr::Heap(b) => b.into_vec(),
        }
    }
}

impl Default for Sds {
    fn default() -> Self {
        Sds::empty()
    }
}

impl Deref for Sds {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Sds {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for Sds {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for Sds {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for Sds {}

impl PartialOrd for Sds {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Sds {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for Sds {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state)
    }
}

impl From<&[u8]> for Sds {
    fn from(b: &[u8]) -> Self {
        Sds::from_slice(b)
    }
}
impl From<&str> for Sds {
    fn from(s: &str) -> Self {
        Sds::from_slice(s.as_bytes())
    }
}
impl From<Vec<u8>> for Sds {
    fn from(v: Vec<u8>) -> Self {
        Sds::from_vec(v)
    }
}

impl fmt::Debug for Sds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_24_bytes() {
        assert_eq!(std::mem::size_of::<Sds>(), 24);
        assert_eq!(std::mem::size_of::<Option<Sds>>(), 24);
    }

    #[test]
    fn inline_and_heap() {
        let short = Sds::from_slice(b"hello");
        assert!(short.is_inline());
        assert_eq!(&*short, b"hello");
        let exact = Sds::from_slice(&[7u8; INLINE_CAP]);
        assert!(exact.is_inline());
        let long = Sds::from_slice(&[1u8; INLINE_CAP + 1]);
        assert!(!long.is_inline());
        assert_eq!(long.len(), INLINE_CAP + 1);
        assert_eq!(Sds::from_vec(b"abc".to_vec()), Sds::from("abc"));
        assert!(Sds::from("a") < Sds::from("b"));
        assert_eq!(Sds::empty().len(), 0);
    }
}
