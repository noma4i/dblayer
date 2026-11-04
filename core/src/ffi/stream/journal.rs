use std::collections::VecDeque;

use super::{StreamEnd, StreamValue};

pub(super) const JOURNAL_LIMIT: usize = 256;

/// What an append did to the journal.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Appended {
    /// The reader had read everything before: it hears that the journal has values again.
    First,
    Queued,
    /// The journal was full of values that a whole snapshot replaces. They are gone and the value was not kept.
    Superseded,
}

/// The one queue of a stream between the core and its reader. Positions count every value from 1. An event that does not fit leaves its position in a range of lost positions. The end is kept apart and reaches the reader once, after everything else.
#[derive(Default)]
pub(super) struct Journal {
    entries: VecDeque<(u64, StreamValue)>,
    last: u64,
    end: Option<(u64, StreamEnd)>,
    end_read: bool,
}

fn evictable(value: &StreamValue) -> bool { matches!(value, StreamValue::Event { .. } | StreamValue::Lost { .. }) }

impl Journal {
    pub(super) fn is_empty(&self) -> bool { self.entries.is_empty() && (self.end.is_none() || self.end_read) }


    /// Keeps a value. A full journal makes room by turning its oldest events into lost positions; a value that is not an event drops every waiting value that a whole snapshot replaces.
    pub(super) fn append(&mut self, value: StreamValue) -> Appended {
        if self.end.is_some() { return Appended::Queued; }
        if matches!(value, StreamValue::Snapshot { .. } | StreamValue::Count { .. }) { self.entries.retain(|(_, kept)| evictable(kept)); }
        let was_empty = self.is_empty();
        if self.entries.len() >= JOURNAL_LIMIT && !evictable(&value) && !self.entries.iter().any(|(_, kept)| evictable(kept)) {
            self.entries.clear();
            return Appended::Superseded;
        }
        self.last += 1;
        self.entries.push_back((self.last, value));
        while self.entries.len() > JOURNAL_LIMIT && self.evict() {}
        if was_empty { Appended::First } else { Appended::Queued }
    }

    /// Positions that could not be read: the reader hears them as lost.
    pub(super) fn lose(&mut self, count: u64) -> Appended {
        if self.end.is_some() || count == 0 { return Appended::Queued; }
        let was_empty = self.is_empty();
        let first = self.last + 1;
        self.last += count;
        self.entries.push_back((self.last, StreamValue::Lost { first, last: self.last }));
        self.merge();
        while self.entries.len() > JOURNAL_LIMIT && self.evict() {}
        if was_empty { Appended::First } else { Appended::Queued }
    }

    /// Turns every waiting event into lost positions: the data they were read from is gone.
    pub(super) fn lose_events(&mut self) {
        for (position, value) in &mut self.entries {
            if matches!(value, StreamValue::Event { .. }) { *value = StreamValue::Lost { first: *position, last: *position }; }
        }
        self.merge();
    }

    /// Drops every waiting value. The end stays.
    pub(super) fn clear(&mut self) { self.entries.clear(); }

    /// Keeps the end. Values appended after it are not kept.
    pub(super) fn end(&mut self, reason: StreamEnd) -> Appended {
        if self.end.is_some() { return Appended::Queued; }
        let was_empty = self.is_empty();
        self.last += 1;
        self.end = Some((self.last, reason));
        if was_empty { Appended::First } else { Appended::Queued }
    }

    /// The next value with its position, and the end once after the last value.
    pub(super) fn take(&mut self) -> Option<(u64, StreamValue)> {
        if let Some(entry) = self.entries.pop_front() { return Some(entry); }
        match self.end {
            Some((position, reason)) if !self.end_read => {
                self.end_read = true;
                Some((position, StreamValue::Ended { reason }))
            }
            _ => None,
        }
    }

    pub(super) fn end_read(&self) -> bool { self.end_read }

    fn evict(&mut self) -> bool {
        let Some((position, value)) = self.entries.iter_mut().find(|(_, value)| matches!(value, StreamValue::Event { .. })) else { return false };
        *value = StreamValue::Lost { first: *position, last: *position };
        self.merge();
        true
    }

    /// Joins lost ranges that touch. The joined range keeps the place of its later part.
    fn merge(&mut self) {
        let mut merged: VecDeque<(u64, StreamValue)> = VecDeque::with_capacity(self.entries.len());
        for (position, value) in self.entries.drain(..) {
            if let (Some((kept, StreamValue::Lost { last, .. })), StreamValue::Lost { first: next, last: next_last }) = (merged.back_mut(), &value) {
                if *last + 1 == *next {
                    *last = *next_last;
                    *kept = position;
                    continue;
                }
            }
            merged.push_back((position, value));
        }
        self.entries = merged;
    }
}
