use std::collections::BTreeMap;

use serde_json::Value;

use super::path::Path;
use crate::operation::{OperationId, OperationStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrelationVerdict {
    Single(OperationId),
    Unmatched,
    Ambiguous(Vec<OperationId>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrelationRule {
    pub fields: Vec<Path>,
    pub created_at_window: Option<f64>,
}

impl CorrelationRule {
    pub fn new(fields: Vec<Path>, created_at_window: Option<f64>) -> Self {
        Self { fields, created_at_window }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrelationCandidate {
    pub operation: OperationId,
    pub values: BTreeMap<Path, Value>,
    pub created_at: i64,
    pub status: OperationStatus,
    pub transport_started: bool,
}

pub fn eligible(candidates: &[CorrelationCandidate]) -> Vec<CorrelationCandidate> {
    candidates
        .iter()
        .filter(|candidate| matches!(candidate.status, OperationStatus::Pending | OperationStatus::DeliveryUnknown))
        .filter(|candidate| candidate.transport_started)
        .cloned()
        .collect()
}

pub fn resolve_correlation(
    rule: &CorrelationRule,
    candidates: &[CorrelationCandidate],
    response: &BTreeMap<Path, Value>,
    response_created_at: i64,
) -> CorrelationVerdict {
    if rule.fields.is_empty() {
        return CorrelationVerdict::Unmatched;
    }
    let matches: Vec<OperationId> = eligible(candidates)
        .into_iter()
        .filter(|candidate| {
            let fields_match = rule.fields.iter().all(|field| matches!((response.get(field), candidate.values.get(field)), (Some(a), Some(b)) if a == b));
            let in_window = rule
                .created_at_window
                .is_none_or(|window| (response_created_at as f64 - candidate.created_at as f64).abs() <= window);
            fields_match && in_window
        })
        .map(|candidate| candidate.operation)
        .collect();
    match matches.len() {
        0 => CorrelationVerdict::Unmatched,
        1 => CorrelationVerdict::Single(matches[0]),
        _ => CorrelationVerdict::Ambiguous(matches),
    }
}

pub fn replaces_temporary(verdict: &CorrelationVerdict) -> Option<OperationId> {
    match verdict {
        CorrelationVerdict::Single(operation) => Some(*operation),
        _ => None,
    }
}
