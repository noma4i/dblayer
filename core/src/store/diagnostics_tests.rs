use super::*;
use crate::identity::ModelKey;
use std::collections::BTreeMap;

#[test]
fn tags_contain_only_source_for_minimal_context() {
    assert_eq!(sync_tags(&SyncContext::new("landing")), BTreeMap::from([("db.source".to_string(), "landing".to_string())]));
}

#[test]
fn tags_include_model_key_and_event() {
    let context = SyncContext {
        model: Some(ModelKey::new("chats")),
        key: Some("chats:1".into()),
        event: Some("insert".into()),
        ..SyncContext::new("landing")
    };
    let tags = sync_tags(&context);
    assert_eq!(tags.len(), 4);
    assert_eq!(tags["db.model"], "chats");
    assert_eq!(tags["db.key"], "chats:1");
    assert_eq!(tags["db.event"], "insert");
}

#[test]
fn tags_skip_empty_key_and_event() {
    let context = SyncContext { key: Some(String::new()), event: Some(String::new()), ..SyncContext::new("landing") };
    assert_eq!(sync_tags(&context).len(), 1);
}

#[test]
fn tags_from_details_do_not_override_existing_keys() {
    let context = SyncContext {
        model: Some(ModelKey::new("chats")),
        details: Some(BTreeMap::from([("model".into(), "other".into()), ("reason".into(), "timeout".into())])),
        ..SyncContext::new("landing")
    };
    let tags = sync_tags(&context);
    assert_eq!(tags["db.model"], "chats");
    assert_eq!(tags["db.reason"], "timeout");
    assert_eq!(tags.len(), 3);
}

#[test]
fn error_context_carries_event_and_reason() {
    let error = SyncFault::TrackFailed("offline".into());
    let context = error.context("realtime", Some(ModelKey::new("chats")), None);
    assert_eq!(context.source, "realtime");
    assert_eq!(context.event.as_deref(), Some("track"));
    assert_eq!(context.model, Some(ModelKey::new("chats")));
    assert_eq!(context.key, None);
    assert_eq!(sync_tags(&context)["db.reason"], "offline");
}

#[test]
fn error_details_cover_counts_and_unit_variants() {
    assert_eq!(SyncFault::SubscriptionDegraded { attempts: 4 }.details().unwrap()["attempts"], "4");
    assert_eq!(SyncFault::UserResetDiscard { open_ops: 2 }.details().unwrap()["openOps"], "2");
    assert!(SyncFault::SubscriptionPayloadMismatch.details().is_none());
    assert_eq!(SyncFault::RejectedRecord("x".into()).event_name(), "rejectedRecord");
}
