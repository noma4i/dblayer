//! The counters of the process: an id minted here is never issued twice in the process.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy)]
pub enum Counter {
    Reader,
    Round,
}

static COUNTERS: [AtomicU64; 2] = [AtomicU64::new(1), AtomicU64::new(1)];

pub fn next_id(counter: Counter) -> u64 { COUNTERS[counter as usize].fetch_add(1, Ordering::SeqCst) }
