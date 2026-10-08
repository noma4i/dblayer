use super::*;
use crate::operation::{OperationId, OperationStatus};
use crate::schema::ActionMode;
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn candidate(seq: i64, values: &[(&Path, Value)], created_at: i64, status: OperationStatus, started: bool, mode: ActionMode) -> CorrelationCandidate {
    CorrelationCandidate {
        operation: OperationId(seq),
        values: values.iter().map(|(p, v)| ((*p).clone(), v.clone())).collect(),
        created_at,
        status,
        transport_started: started,
        mode,
    }
}

fn unknown(seq: i64, values: &[(&Path, Value)], created_at: i64) -> CorrelationCandidate {
    candidate(seq, values, created_at, OperationStatus::DeliveryUnknown, true, ActionMode::Request)
}

fn response(values: &[(&Path, Value)]) -> BTreeMap<Path, Value> {
    values.iter().map(|(p, v)| ((*p).clone(), v.clone())).collect()
}

#[test]
fn eligible_keeps_open_operations() {
    let kept = vec![
        candidate(1, &[], 100, OperationStatus::Pending, false, ActionMode::Request),
        candidate(2, &[], 100, OperationStatus::DeliveryUnknown, false, ActionMode::Request),
    ];
    let mut all = kept.clone();
    all.push(candidate(3, &[], 100, OperationStatus::Committed, false, ActionMode::Request));
    all.push(candidate(4, &[], 100, OperationStatus::Failed, false, ActionMode::Request));
    all.push(candidate(5, &[], 100, OperationStatus::Pending, false, ActionMode::Durable));
    assert_eq!(eligible(&all), kept);
    assert_eq!(eligible(&[candidate(6, &[], 100, OperationStatus::Pending, true, ActionMode::Durable)]).len(), 1);
}

#[test]
fn single_match_within_window_resolves_operation() {
    let (kind, author) = (Path::new("kind"), Path::new("author"));
    let message = json!("message");
    let matched = unknown(1, &[(&kind, message.clone()), (&author, json!("alice"))], 100);
    let inside = unknown(2, &[(&kind, message.clone())], 90);
    let outside = unknown(3, &[(&kind, message.clone())], 89);
    let verdict = resolve_correlation(
        &CorrelationRule::new(vec![kind.clone(), author.clone()], None),
        &[matched],
        &response(&[(&kind, message.clone()), (&author, json!("alice"))]),
        100,
    );
    assert_eq!(verdict, CorrelationVerdict::Single(OperationId(1)));
    let windowed = resolve_correlation(&CorrelationRule::new(vec![kind.clone()], Some(10.0)), &[inside, outside], &response(&[(&kind, message)]), 100);
    assert_eq!(windowed, CorrelationVerdict::Single(OperationId(2)));
}

#[test]
fn missing_fields_and_empty_rule_give_unmatched() {
    let (kind, author) = (Path::new("kind"), Path::new("author"));
    let message = json!("message");
    let rule = CorrelationRule::new(vec![kind.clone(), author.clone()], None);
    let complete = unknown(1, &[(&kind, message.clone()), (&author, json!("alice"))], 100);
    let missing = unknown(2, &[(&kind, message.clone())], 100);
    assert_eq!(resolve_correlation(&rule, &[complete], &response(&[(&kind, message.clone())]), 100), CorrelationVerdict::Unmatched);
    assert_eq!(
        resolve_correlation(&rule, &[missing], &response(&[(&kind, message), (&author, json!("alice"))]), 100),
        CorrelationVerdict::Unmatched
    );
    assert_eq!(resolve_correlation(&CorrelationRule::new(vec![], None), &[unknown(3, &[], 100)], &response(&[]), 100), CorrelationVerdict::Unmatched);
}

#[test]
fn several_matches_give_ambiguous_and_only_single_replaces_temporary() {
    let kind = Path::new("kind");
    let message = json!("message");
    let first = unknown(1, &[(&kind, message.clone())], 100);
    let second = candidate(2, &[(&kind, message.clone())], 100, OperationStatus::Pending, false, ActionMode::Request);
    let verdict = resolve_correlation(&CorrelationRule::new(vec![kind.clone()], None), &[first, second], &response(&[(&kind, message)]), 100);
    assert_eq!(verdict, CorrelationVerdict::Ambiguous(vec![OperationId(1), OperationId(2)]));
    assert_eq!(replaces_temporary(&CorrelationVerdict::Single(OperationId(1))), Some(OperationId(1)));
    assert_eq!(replaces_temporary(&CorrelationVerdict::Unmatched), None);
    assert_eq!(replaces_temporary(&verdict), None);
}
