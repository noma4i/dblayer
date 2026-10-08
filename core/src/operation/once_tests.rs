use super::*;
use std::collections::HashSet;

fn request(key: Option<&str>) -> OnceRequest {
    OnceRequest::new("deleteGroup", key)
}

#[test]
fn verdict_follows_declaration_key_and_request_sets() {
    let req = request(Some("chat-1"));
    let none = HashSet::new();
    let both: HashSet<_> = [req.clone()].into();
    assert_eq!(once_verdict(&req, false, &none, &none), OnceVerdict::Run);
    assert_eq!(once_verdict(&request(None), true, &none, &none), OnceVerdict::MissingKey);
    assert_eq!(once_verdict(&request(Some("")), true, &none, &none), OnceVerdict::MissingKey);
    assert_eq!(once_verdict(&req, true, &both, &none), OnceVerdict::SkipCommittedKey);
    assert_eq!(once_verdict(&req, true, &none, &both), OnceVerdict::SkipOpenOperation);
    assert_eq!(once_verdict(&req, true, &both, &both), OnceVerdict::SkipCommittedKey);
    assert!(calls_transport(OnceVerdict::Run));
    assert!(!calls_transport(OnceVerdict::MissingKey));
    assert!(!calls_transport(OnceVerdict::SkipCommittedKey));
}

#[test]
fn committing_updates_set_only_for_run_with_dedupe_key() {
    let req = request(Some("chat-1"));
    assert_eq!(committing(&HashSet::new(), &req, OnceVerdict::Run), [req.clone()].into());
    assert!(committing(&HashSet::new(), &req, OnceVerdict::SkipCommittedKey).is_empty());
    assert!(committing(&HashSet::new(), &request(None), OnceVerdict::Run).is_empty());
}
