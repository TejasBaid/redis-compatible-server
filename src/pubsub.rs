//! Publish/subscribe registry.
//!
//! Channels and patterns map to the subscribing clients' shared handles.
//! `PUBLISH` takes a read lock, encodes the message once per protocol version
//! and pushes it into each subscriber's cross-thread output queue; the
//! subscriber's own worker thread writes it to the socket.

use crate::resp::Reply;
use crate::sds::Sds;
use crate::server::client::ClientHandle;
use std::collections::HashMap;
use std::sync::Arc;

type Subscribers = HashMap<u64, Arc<ClientHandle>, foldhash::fast::RandomState>;

/// The registry. Lives behind an `RwLock` in the server.
#[derive(Default)]
pub struct PubSub {
    channels: HashMap<Sds, Subscribers, foldhash::fast::RandomState>,
    patterns: HashMap<Sds, Subscribers, foldhash::fast::RandomState>,
}

fn encode(kind: &[u8], parts: &[&[u8]], resp3: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + parts.iter().map(|p| p.len()).sum::<usize>());
    let mut r = Reply::new(&mut buf, resp3);
    r.push(parts.len() + 1);
    r.bulk(kind);
    for p in parts {
        r.bulk(p);
    }
    buf
}

impl PubSub {
    /// Subscribes `h` to `ch`; returns true if it was not subscribed yet.
    pub fn subscribe(&mut self, ch: &[u8], h: &Arc<ClientHandle>) -> bool {
        self.channels
            .entry(Sds::from_slice(ch))
            .or_default()
            .insert(h.id, h.clone())
            .is_none()
    }

    /// Unsubscribes client `id` from `ch`.
    pub fn unsubscribe(&mut self, ch: &[u8], id: u64) -> bool {
        let key = Sds::from_slice(ch);
        let Some(subs) = self.channels.get_mut(&key) else {
            return false;
        };
        let removed = subs.remove(&id).is_some();
        if subs.is_empty() {
            self.channels.remove(&key);
        }
        removed
    }

    /// Subscribes `h` to pattern `p`.
    pub fn psubscribe(&mut self, p: &[u8], h: &Arc<ClientHandle>) -> bool {
        self.patterns
            .entry(Sds::from_slice(p))
            .or_default()
            .insert(h.id, h.clone())
            .is_none()
    }

    /// Unsubscribes client `id` from pattern `p`.
    pub fn punsubscribe(&mut self, p: &[u8], id: u64) -> bool {
        let key = Sds::from_slice(p);
        let Some(subs) = self.patterns.get_mut(&key) else {
            return false;
        };
        let removed = subs.remove(&id).is_some();
        if subs.is_empty() {
            self.patterns.remove(&key);
        }
        removed
    }

    /// Delivers `msg` on `ch` to every subscriber and returns the receiver
    /// count. If the publisher itself is a receiver, its copy is written to
    /// `self_out` directly so it precedes the `PUBLISH` reply, as in Redis.
    pub fn publish(&self, ch: &[u8], msg: &[u8], self_id: u64, self_out: &mut Vec<u8>) -> usize {
        let mut count = 0;
        let mut enc: [Option<Vec<u8>>; 2] = [None, None];
        let mut deliver = |h: &Arc<ClientHandle>,
                           kind: &[u8],
                           parts: &[&[u8]],
                           enc: &mut [Option<Vec<u8>>; 2]| {
            let r3 = h.resp3.load(std::sync::atomic::Ordering::Relaxed);
            let slot = &mut enc[r3 as usize];
            let bytes = slot.get_or_insert_with(|| encode(kind, parts, r3));
            if h.id == self_id {
                self_out.extend_from_slice(bytes);
            } else {
                h.push(bytes);
            }
        };
        if let Some(subs) = self.channels.get(ch) {
            for h in subs.values() {
                deliver(h, b"message", &[ch, msg], &mut enc);
                count += 1;
            }
        }
        for (pat, subs) in &self.patterns {
            if crate::util::glob_match(pat, ch, false) {
                let mut penc: [Option<Vec<u8>>; 2] = [None, None];
                for h in subs.values() {
                    deliver(h, b"pmessage", &[pat, ch, msg], &mut penc);
                    count += 1;
                }
            }
        }
        count
    }

    /// Active channels, optionally filtered by a glob pattern.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Sds> {
        self.channels
            .keys()
            .filter(|c| pattern.is_none_or(|p| crate::util::glob_match(p, c, false)))
            .cloned()
            .collect()
    }

    /// Subscriber count for a channel.
    pub fn numsub(&self, ch: &[u8]) -> usize {
        self.channels.get(ch).map_or(0, |s| s.len())
    }

    /// Number of distinct patterns.
    pub fn numpat(&self) -> usize {
        self.patterns.len()
    }

    /// Total channel subscriptions (for INFO).
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

/// Encodes a subscription confirmation (`subscribe`, `unsubscribe`, ...).
pub fn confirm(out: &mut Vec<u8>, resp3: bool, kind: &[u8], name: Option<&[u8]>, count: usize) {
    let mut r = Reply::new(out, resp3);
    r.push(3);
    r.bulk(kind);
    match name {
        Some(n) => r.bulk(n),
        None => r.null(),
    }
    r.int(count as i64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_reaches_channels_and_patterns() {
        let mut ps = PubSub::default();
        let a = Arc::new(ClientHandle::new(1, None, String::new(), String::new()));
        let b = Arc::new(ClientHandle::new(2, None, String::new(), String::new()));
        assert!(ps.subscribe(b"news", &a));
        assert!(!ps.subscribe(b"news", &a));
        assert!(ps.psubscribe(b"n*", &b));
        let mut own = Vec::new();
        assert_eq!(ps.publish(b"news", b"hi", 99, &mut own), 2);
        assert_eq!(
            a.take_pending().0,
            b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
        );
        assert_eq!(
            b.take_pending().0,
            b"*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
        );
        assert_eq!(ps.numsub(b"news"), 1);
        assert!(ps.unsubscribe(b"news", 1));
        assert_eq!(ps.channels(None).len(), 0);
        assert_eq!(ps.numpat(), 1);
    }
}
