//! String values: integer-encoded when the bytes are a canonical `i64`
//! (so `INCR` never parses or allocates), otherwise a compact [`Sds`].

use crate::sds::Sds;

/// A string value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RStr {
    /// Canonical integer representation (`OBJECT ENCODING` → `int`).
    Int(i64),
    /// Raw bytes. Reported as `embstr` up to 44 bytes (like Redis'
    /// single-allocation strings) unless the key's `raw` flag is set by an
    /// in-place modification (APPEND, SETRANGE); see [`crate::db::Obj`].
    Raw(Sds),
}

/// Longest string Redis stores as `embstr`.
pub const EMBSTR_MAX: usize = 44;

impl RStr {
    /// Encodes `b`, choosing the integer encoding when Redis would.
    #[inline]
    pub fn from_bytes(b: &[u8]) -> RStr {
        if b.len() <= 20
            && let Some(v) = crate::util::parse_i64(b)
        {
            return RStr::Int(v);
        }
        RStr::Raw(Sds::from_slice(b))
    }

    /// Calls `f` with the byte representation (formats integers on the stack).
    #[inline]
    pub fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        match self {
            RStr::Int(v) => {
                let mut b = itoa::Buffer::new();
                f(b.format(*v).as_bytes())
            }
            RStr::Raw(s) => f(s),
        }
    }

    /// Owned copy of the bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.with_bytes(|b| b.to_vec())
    }

    /// Length in bytes of the string representation.
    pub fn len(&self) -> usize {
        match self {
            RStr::Int(v) => {
                let mut b = itoa::Buffer::new();
                b.format(*v).len()
            }
            RStr::Raw(s) => s.len(),
        }
    }

    /// True for the empty string.
    pub fn is_empty(&self) -> bool {
        matches!(self, RStr::Raw(s) if s.is_empty())
    }

    /// The integer value, if the string is a canonical integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            RStr::Int(v) => Some(*v),
            RStr::Raw(s) => {
                if s.len() <= 20 {
                    crate::util::parse_i64(s)
                } else {
                    None
                }
            }
        }
    }

    /// `OBJECT ENCODING` name.
    pub fn encoding_name(&self) -> &'static str {
        match self {
            RStr::Int(_) => "int",
            RStr::Raw(s) if s.len() <= EMBSTR_MAX => "embstr",
            RStr::Raw(_) => "raw",
        }
    }

    /// Heap bytes owned (0 for integers and inline strings).
    pub fn heap_bytes(&self) -> usize {
        match self {
            RStr::Int(_) => 0,
            RStr::Raw(s) => s.heap_size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_encoding() {
        assert_eq!(RStr::from_bytes(b"123"), RStr::Int(123));
        assert_eq!(RStr::from_bytes(b"-5"), RStr::Int(-5));
        assert!(matches!(RStr::from_bytes(b"0123"), RStr::Raw(_)));
        assert!(matches!(RStr::from_bytes(b"1.5"), RStr::Raw(_)));
        assert_eq!(RStr::Int(-42).len(), 3);
        assert_eq!(RStr::Int(7).to_vec(), b"7");
        assert_eq!(RStr::from_bytes(b"x").encoding_name(), "embstr");
        assert_eq!(RStr::from_bytes(&[b'x'; 45]).encoding_name(), "raw");
    }
}
