use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};

use super::*;
use crate::runtime::ManualClock;
use crate::transport::encrypted::tests::fake_server_tests::MemoryCredentials;

fn pending(body: Value, class: OperationClass) -> PendingCall {
    PendingCall { request_id: 1, sequence: 4, class, operation: "read".into(), path: "/read".into(), body: body.to_string(), credential: None, purpose: 0, author: None, owner: None, epoch: 1, reply: Some(1) }
}

#[test]
fn sess_sealing_fits() {
    let credentials = MemoryCredentials::default();
    let secret = "secret";
    credentials.set(secret).unwrap();
    let digest: String = Sha256::digest(secret.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
    let identity = crate::store::identity_of(&digest);
    let config = SealConfig::new(&crate::transport::encrypted::tests::protocol_tests::config(), Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)))).unwrap();
    let small = config.probe(&credentials, identity, &pending(json!({"text":"hello"}), OperationClass::Authenticated)).unwrap();
    let large = config.probe(&credentials, identity, &pending(json!({"text":"x".repeat(262144)}), OperationClass::Authenticated)).unwrap();
    assert!(small <= 262144);
    assert!(large > 262144);
    let public = config.probe(&credentials, None, &pending(json!({"text":"hello"}), OperationClass::Sessionless)).unwrap();
    assert!(public < small);
}

#[test]
fn sess_sealing_failed() {
    let credentials = MemoryCredentials::default();
    let config = SealConfig::new(&crate::transport::encrypted::tests::protocol_tests::config(), Arc::new(ManualClock::new(Duration::from_secs(1)))).unwrap();
    let request = pending(json!({}), OperationClass::Authenticated);
    assert!(matches!(config.probe(&credentials, Some(1), &request), Err(SessionError::StateLost)));
    credentials.set("different").unwrap();
    assert!(matches!(config.probe(&credentials, Some(1), &request), Err(SessionError::StateLost)));
}
