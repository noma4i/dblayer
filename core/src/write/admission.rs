use std::collections::BTreeMap;

use super::path::Path;
use super::policy::WritePolicy;
use crate::identity::RowId;
use crate::pacing::Counters;

pub const DROP_EVENT_CAPACITY: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionTarget {
    pub model: String,
    pub row: RowId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDrop {
    pub model: String,
    pub row: RowId,
    pub fields: Vec<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionVerdict {
    pub accepted_fields: Vec<Path>,
    pub dropped_fields: Vec<Path>,
    pub drop: Option<AdmissionDrop>,
}

pub fn admits_server_fields(row_revision: i64, base_revision: i64) -> bool {
    row_revision <= base_revision
}

pub fn admission_verdict(
    target: &AdmissionTarget,
    fields: &[Path],
    policies: &BTreeMap<Path, WritePolicy>,
    row_revision: i64,
    base_revision: i64,
) -> AdmissionVerdict {
    let server_admitted = admits_server_fields(row_revision, base_revision);
    let (accepted_fields, dropped_fields): (Vec<Path>, Vec<Path>) = fields.iter().cloned().partition(|field| {
        let policy = policies.get(field).cloned().unwrap_or_else(WritePolicy::default_policy);
        policy != WritePolicy::Server || server_admitted
    });
    let drop = (!dropped_fields.is_empty())
        .then(|| AdmissionDrop { model: target.model.clone(), row: target.row.clone(), fields: dropped_fields.clone() });
    AdmissionVerdict { accepted_fields, dropped_fields, drop }
}

pub fn append_drop(buffer: &mut Vec<AdmissionDrop>, drop: AdmissionDrop) {
    buffer.push(drop);
    if buffer.len() > DROP_EVENT_CAPACITY {
        buffer.drain(..buffer.len() - DROP_EVENT_CAPACITY);
    }
}

pub fn count_drops(counters: &mut Counters, verdict: &AdmissionVerdict) {
    if verdict.drop.is_some() {
        counters.causal_admission_drops += 1;
    }
}
