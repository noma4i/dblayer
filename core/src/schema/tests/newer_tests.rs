use serde_json::{json, Map, Value};

use super::*;
use crate::schema::AssociationKind;

fn chat_link() -> AssociationSpec {
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "chats", "chatId");
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "createdAt"), ("lastSequenceNumber", "sequenceNumber")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    chat.newer = vec!["sequenceNumber".to_string(), "createdAt".to_string()];
    chat
}

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn large_sequences_decide_before_the_id_tiebreaker() {
    let older = row(json!({"id": "7", "sequenceNumber": 9_007_199_254_740_992_i64}));
    let child = row(json!({"id": "6", "sequenceNumber": 9_007_199_254_740_993_i64}));
    assert_eq!(chat_link().newest([&older, &child]), Some(&child));
    let newer = row(json!({"id": "7", "sequenceNumber": 9_007_199_254_740_993_i64}));
    let child = row(json!({"id": "8", "sequenceNumber": 9_007_199_254_740_992_i64}));
    assert_eq!(chat_link().newest([&newer, &child]), Some(&newer));
}
