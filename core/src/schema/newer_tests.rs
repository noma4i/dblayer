use serde_json::{json, Map, Value};

use super::*;
use crate::schema::{AssociationKind, Newer};

fn chat_link() -> AssociationSpec {
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "chats", "chatId");
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "createdAt"), ("lastSequenceNumber", "sequenceNumber")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    chat.newer = [("sequenceNumber", "lastSequenceNumber"), ("createdAt", "lastMessageAt")].map(|(child, parent)| Newer { child: child.to_string(), parent: parent.to_string() }).into();
    chat
}

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn a_child_moves_the_parent_pointer_only_when_it_is_newer_by_the_tuple() {
    let chat = chat_link();
    let parent = row(json!({"lastMessageId": "7", "lastSequenceNumber": 10, "lastMessageAt": "2026-01-03T00:00:05.000Z"}));
    let cases = [
        ("a higher sequence wins over an older date", json!({"id": "8", "sequenceNumber": 11, "createdAt": "2026-01-01T00:00:00.000Z"}), true),
        ("a lower sequence loses over a newer date", json!({"id": "6", "sequenceNumber": 9, "createdAt": "2026-01-09T00:00:00.000Z"}), false),
        ("without a sequence the date decides", json!({"id": "8", "createdAt": "2026-01-03T00:00:05.900Z"}), true),
        ("the same id refreshes", json!({"id": "7", "sequenceNumber": 10, "createdAt": "2026-01-02T00:00:00.000Z"}), true),
        ("a temporary id outranks a confirmed one whatever the clocks say", json!({"id": "temp-a", "createdAt": "2025-01-01T00:00:00.000Z"}), true),
        ("equal tuple: the greater numeric id wins", json!({"id": "9", "sequenceNumber": 10, "createdAt": "2026-01-03T00:00:05.000Z"}), true),
        ("equal tuple: the smaller numeric id loses", json!({"id": "6", "sequenceNumber": 10, "createdAt": "2026-01-03T00:00:05.000Z"}), false),
    ];
    for (name, child, expected) in cases {
        assert_eq!(chat.child_is_newer(&parent, &row(child)), expected, "{name}");
    }
}

#[test]
fn large_sequences_decide_before_the_id_tiebreaker() {
    let parent = row(json!({"lastMessageId": "7", "lastSequenceNumber": 9_007_199_254_740_992_i64}));
    let child = row(json!({"id": "6", "sequenceNumber": 9_007_199_254_740_993_i64}));
    assert!(chat_link().child_is_newer(&parent, &child));
    let parent = row(json!({"lastMessageId": "7", "lastSequenceNumber": 9_007_199_254_740_993_i64}));
    let child = row(json!({"id": "8", "sequenceNumber": 9_007_199_254_740_992_i64}));
    assert!(!chat_link().child_is_newer(&parent, &child));
}

#[test]
fn a_pending_pointer_holds_against_a_confirmed_child() {
    let chat = chat_link();
    let parent = row(json!({"lastMessageId": "temp-a", "lastMessageAt": "2026-01-03T00:00:05.000Z"}));
    assert!(!chat.child_is_newer(&parent, &row(json!({"id": "8", "sequenceNumber": 11, "createdAt": "2026-01-09T00:00:00.000Z"}))));
}

#[test]
fn an_empty_parent_pointer_is_older_than_any_child() {
    assert!(chat_link().child_is_newer(&row(json!({"lastMessageId": null})), &row(json!({"id": "1"}))));
}
