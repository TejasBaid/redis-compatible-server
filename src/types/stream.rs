//! Streams: an append-only log of field/value entries keyed by `ms-seq` IDs.
//!
//! Redis stores streams in a radix tree of listpacks. Here entries live in a
//! `BTreeMap` keyed by [`StreamId`], which gives the same ordered range scans
//! (`XRANGE`, `XREAD`) with far less code. Consumer groups are not
//! implemented.

use crate::sds::Sds;
use std::collections::BTreeMap;
use std::ops::Bound;

/// A stream entry ID (`<ms>-<seq>`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    /// Milliseconds part.
    pub ms: u64,
    /// Sequence part.
    pub seq: u64,
}

impl StreamId {
    /// The smallest possible ID.
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    /// The largest possible ID.
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// Formats as `ms-seq`.
    pub fn to_bytes(&self) -> Vec<u8> {
        format!("{}-{}", self.ms, self.seq).into_bytes()
    }

    /// The next ID in order, if any.
    pub fn incr(&self) -> Option<StreamId> {
        if self.seq < u64::MAX {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq + 1,
            })
        } else if self.ms < u64::MAX {
            Some(StreamId {
                ms: self.ms + 1,
                seq: 0,
            })
        } else {
            None
        }
    }

    /// The previous ID in order, if any.
    pub fn decr(&self) -> Option<StreamId> {
        if self.seq > 0 {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq - 1,
            })
        } else if self.ms > 0 {
            Some(StreamId {
                ms: self.ms - 1,
                seq: u64::MAX,
            })
        } else {
            None
        }
    }

    /// Parses `ms-seq` or `ms` (the sequence defaults to `missing_seq`).
    /// Returns `None` on malformed input.
    pub fn parse(s: &[u8], missing_seq: u64) -> Option<StreamId> {
        let text = std::str::from_utf8(s).ok()?;
        let (ms, seq) = match text.split_once('-') {
            Some((a, b)) => (a, Some(b)),
            None => (text, None),
        };
        let ms = parse_u64_strict(ms)?;
        let seq = match seq {
            Some(b) => parse_u64_strict(b)?,
            None => missing_seq,
        };
        Some(StreamId { ms, seq })
    }
}

fn parse_u64_strict(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// A stream value.
#[derive(Default)]
pub struct Stream {
    entries: BTreeMap<StreamId, Box<[Sds]>>,
    /// ID of the last entry ever added (survives deletion of that entry).
    pub last_id: StreamId,
    /// Total entries ever added.
    pub entries_added: u64,
    /// Largest ID ever deleted.
    pub max_deleted_id: StreamId,
}

impl Stream {
    /// An empty stream.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when the stream has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate heap bytes.
    pub fn heap_bytes(&self) -> usize {
        self.entries
            .values()
            .map(|f| 48 + f.len() * 24 + f.iter().map(|s| s.heap_size()).sum::<usize>())
            .sum()
    }

    /// Appends an entry with an ID strictly greater than `last_id` (checked by
    /// the caller).
    pub fn append(&mut self, id: StreamId, fields: Vec<Sds>) {
        debug_assert!(id > self.last_id || self.entries_added == 0);
        self.entries.insert(id, fields.into_boxed_slice());
        self.last_id = id;
        self.entries_added += 1;
    }

    /// Deletes an entry by ID; returns true if it existed.
    pub fn delete(&mut self, id: StreamId) -> bool {
        if self.entries.remove(&id).is_some() {
            if id > self.max_deleted_id {
                self.max_deleted_id = id;
            }
            true
        } else {
            false
        }
    }

    /// First entry ID.
    pub fn first_id(&self) -> Option<StreamId> {
        self.entries.keys().next().copied()
    }

    /// Entries in `[start, end]` (inclusive), ascending or descending.
    pub fn range(
        &self,
        start: StreamId,
        end: StreamId,
        rev: bool,
        count: usize,
    ) -> Vec<(StreamId, &[Sds])> {
        if start > end {
            return Vec::new();
        }
        let it = self
            .entries
            .range((Bound::Included(start), Bound::Included(end)));
        if rev {
            it.rev().take(count).map(|(k, v)| (*k, &v[..])).collect()
        } else {
            it.take(count).map(|(k, v)| (*k, &v[..])).collect()
        }
    }

    /// Entries strictly after `after`, ascending (for `XREAD`).
    pub fn after(&self, after: StreamId, count: usize) -> Vec<(StreamId, &[Sds])> {
        self.entries
            .range((Bound::Excluded(after), Bound::Unbounded))
            .take(count)
            .map(|(k, v)| (*k, &v[..]))
            .collect()
    }

    /// Trims to at most `maxlen` entries (oldest first); returns the count
    /// removed.
    pub fn trim_maxlen(&mut self, maxlen: usize, limit: usize) -> usize {
        let mut removed = 0;
        while self.entries.len() > maxlen && removed < limit {
            if let Some((id, _)) = self.entries.pop_first() {
                if id > self.max_deleted_id {
                    self.max_deleted_id = id;
                }
                removed += 1;
            }
        }
        removed
    }

    /// Removes entries with IDs below `minid`; returns the count removed.
    pub fn trim_minid(&mut self, minid: StreamId, limit: usize) -> usize {
        let mut removed = 0;
        while removed < limit {
            match self.entries.first_key_value() {
                Some((id, _)) if *id < minid => {
                    let id = *id;
                    self.entries.pop_first();
                    if id > self.max_deleted_id {
                        self.max_deleted_id = id;
                    }
                    removed += 1;
                }
                _ => break,
            }
        }
        removed
    }

    /// Iterates all entries in order.
    pub fn iter(&self) -> impl Iterator<Item = (StreamId, &[Sds])> {
        self.entries.iter().map(|(k, v)| (*k, &v[..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids() {
        assert_eq!(StreamId::parse(b"5-3", 0), Some(StreamId { ms: 5, seq: 3 }));
        assert_eq!(
            StreamId::parse(b"5", u64::MAX),
            Some(StreamId {
                ms: 5,
                seq: u64::MAX
            })
        );
        assert_eq!(StreamId::parse(b"-1", 0), None);
        assert_eq!(StreamId::parse(b"a-b", 0), None);
        assert_eq!(
            StreamId {
                ms: 1,
                seq: u64::MAX
            }
            .incr(),
            Some(StreamId { ms: 2, seq: 0 })
        );
        assert_eq!(StreamId::MIN.decr(), None);
    }

    #[test]
    fn append_range_trim() {
        let mut s = Stream::new();
        for i in 1..=10 {
            s.append(
                StreamId { ms: i, seq: 0 },
                vec![Sds::from("f"), Sds::from("v")],
            );
        }
        assert_eq!(s.len(), 10);
        let r = s.range(
            StreamId { ms: 3, seq: 0 },
            StreamId { ms: 5, seq: 0 },
            false,
            usize::MAX,
        );
        assert_eq!(r.len(), 3);
        let r = s.range(StreamId::MIN, StreamId::MAX, true, 2);
        assert_eq!(r[0].0.ms, 10);
        assert_eq!(s.after(StreamId { ms: 8, seq: 0 }, 10).len(), 2);
        assert_eq!(s.trim_maxlen(5, usize::MAX), 5);
        assert_eq!(s.first_id().unwrap().ms, 6);
        assert_eq!(s.trim_minid(StreamId { ms: 8, seq: 0 }, usize::MAX), 2);
        assert!(s.delete(StreamId { ms: 9, seq: 0 }));
        assert_eq!(s.len(), 2);
        assert_eq!(s.last_id.ms, 10);
    }
}
