use std::collections::HashMap;

use rusqlite::{params, Connection};

pub const QUARANTINE_CAP: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuarantineReason {
    PlanRowRejected,
    OrphanTempRow,
    DecodeFailed,
    MissingId,
    NormalizeRejected,
    ReplaceNormalizeRejected,
    IdentityMismatch,
    SchemaChanged,
}

impl QuarantineReason {
    pub const ALL: [QuarantineReason; 8] = [
        QuarantineReason::PlanRowRejected,
        QuarantineReason::OrphanTempRow,
        QuarantineReason::DecodeFailed,
        QuarantineReason::MissingId,
        QuarantineReason::NormalizeRejected,
        QuarantineReason::ReplaceNormalizeRejected,
        QuarantineReason::IdentityMismatch,
        QuarantineReason::SchemaChanged,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineReason::PlanRowRejected => "plan-row-rejected",
            QuarantineReason::OrphanTempRow => "orphan-temp-row",
            QuarantineReason::DecodeFailed => "decode-failed",
            QuarantineReason::MissingId => "missing-id",
            QuarantineReason::NormalizeRejected => "normalize-rejected",
            QuarantineReason::ReplaceNormalizeRejected => "replace-normalize-rejected",
            QuarantineReason::IdentityMismatch => "identity-mismatch",
            QuarantineReason::SchemaChanged => "schema-changed",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|reason| reason.as_str() == text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuarantineTicket {
    pub seq: i64,
    pub reason: QuarantineReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineEntry {
    pub sequence: i64,
    pub model: String,
    pub payload: Vec<u8>,
}

pub fn is_evictable(reason: QuarantineReason) -> bool {
    reason == QuarantineReason::PlanRowRejected
}

pub fn keeps_operation_open(reason: QuarantineReason) -> bool {
    reason == QuarantineReason::ReplaceNormalizeRejected
}

pub fn counts_as_rejected_record(reason: QuarantineReason) -> bool {
    matches!(reason, QuarantineReason::DecodeFailed | QuarantineReason::IdentityMismatch)
}

pub fn evicted(tickets: &[QuarantineTicket], cap: usize) -> Vec<i64> {
    let mut rejected: Vec<i64> = tickets.iter().filter(|ticket| is_evictable(ticket.reason)).map(|ticket| ticket.seq).collect();
    rejected.sort_unstable();
    let excess = rejected.len().saturating_sub(cap);
    rejected.truncate(excess);
    rejected
}

pub fn restore_order(tickets: &[QuarantineTicket]) -> Vec<i64> {
    let mut sequences: Vec<i64> = tickets.iter().map(|ticket| ticket.seq).collect();
    sequences.sort_unstable();
    sequences
}

pub fn quarantine_add(connection: &Connection, model: &str, payload: &[u8], reason: QuarantineReason) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO quarantine (model, payload, reason) VALUES (?, ?, ?)", params![model, payload, reason.as_str()])?;
    Ok(connection.last_insert_rowid())
}

pub fn quarantine_counts(connection: &Connection) -> rusqlite::Result<HashMap<String, usize>> {
    connection
        .prepare("SELECT reason, COUNT(*) FROM quarantine GROUP BY reason")?
        .query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as usize)))?
        .collect()
}

pub fn quarantine_entries(connection: &Connection, reason: QuarantineReason, limit: usize) -> rusqlite::Result<Vec<QuarantineEntry>> {
    connection
        .prepare("SELECT seq, model, payload FROM quarantine WHERE reason = ? ORDER BY seq LIMIT ?")?
        .query_map(params![reason.as_str(), limit as i64], |row| {
            Ok(QuarantineEntry { sequence: row.get(0)?, model: row.get::<_, Option<String>>(1)?.unwrap_or_default(), payload: row.get(2)? })
        })?
        .collect()
}

pub fn quarantine_oldest(connection: &Connection, reason: QuarantineReason, limit: usize) -> rusqlite::Result<Vec<i64>> {
    Ok(quarantine_entries(connection, reason, limit)?.into_iter().map(|entry| entry.sequence).collect())
}

pub fn evict_over_cap(connection: &Connection, cap: usize) -> rusqlite::Result<Vec<i64>> {
    let tickets: Vec<QuarantineTicket> = quarantine_oldest(connection, QuarantineReason::PlanRowRejected, i64::MAX as usize)?
        .into_iter()
        .map(|seq| QuarantineTicket { seq, reason: QuarantineReason::PlanRowRejected })
        .collect();
    let evicted = evicted(&tickets, cap);
    for sequence in &evicted {
        connection.execute("DELETE FROM quarantine WHERE seq = ?", [sequence])?;
    }
    Ok(evicted)
}
