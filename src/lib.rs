//! # redis-clone
//!
//! A multi-threaded, Redis-compatible in-memory data store.
//!
//! * [`resp`] — RESP2/RESP3 parsing and encoding (pipelining, inline commands)
//! * [`dict`], [`skiplist`], [`listpack`], [`intset`], [`sds`] — the data
//!   structures behind the keyspace and the value types in [`types`]
//! * [`db`] — the lock-striped keyspace; [`cmd`] — command table and dispatcher
//! * [`server`] — worker event loops, cron, eviction, active expiry
//! * [`persistence`] — fork-less snapshots and a multi-part AOF
//! * [`replication`] — PSYNC with full and partial resynchronization
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod alloc;
pub mod blocking;
pub mod cmd;
pub mod config;
pub mod db;
pub mod dict;
pub mod intset;
pub mod listpack;
pub mod persistence;
pub mod propagate;
pub mod pubsub;
pub mod replication;
pub mod resp;
pub mod sds;
pub mod server;
pub mod skiplist;
pub mod types;
pub mod util;
