//! Session sequence on the schemas of the app: phone sign-in (otp + confirm), the secret slot reads None once right after confirm, two calls at once, a kill between landings (lifecycle.md Session: one credential per run, `sequence = credential.sequence+1` for each new pair, Landing Committed sets credential.sequence).
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{CredentialStore, DbError};

/// The Keychain of the app: the first read after `blank_next_read` finds nothing.
#[derive(Default)]
struct Keychain {
    inner: WatchedHeld,
    blank: AtomicBool,
}

impl CredentialStore for Keychain {
    fn get(&self) -> Result<Option<String>, DbError> {
        if self.blank.swap(false, Ordering::SeqCst) { return Ok(None); }
        self.inner.get()
    }
    fn set(&self, credential: String) -> Result<(), DbError> { self.inner.set(credential) }
    fn clear(&self) -> Result<(), DbError> { self.inner.clear() }
}

fn stored(path: &std::path::Path) -> (String, i64) {
    let db = crate::store::Store::open_models(Some(path), vec![], None, std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    let inner = db.lock();
    let session = crate::store::session_state::session(&inner.connection).unwrap().unwrap();
    (session.credential_digest, session.sequence as i64)
}

fn location() -> serde_json::Value { json!({"userLocationUpdate": {"location": {"address": "a", "city": "c", "lat": 1.0, "lng": 2.0, "name": "n"}, "viewer": {"user": {"id": "u1", "updatedAt": "2026-11-09T00:00:00Z"}}}}) }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_phone_sign_in_sequences_never_repeat_and_survive_kill() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let keychain = Arc::new(Keychain::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), keychain.clone()).await;
    stand.answer("UserLocationUpdate", location());
    stand.answer("DeviceRegister", json!({"deviceRegister": {"viewer": {"user": {"id": "u1"}}}}));
    bound(stand.database.session_call("otp".into(), json!({"phone": "+10000000000"}).to_string())).await.unwrap();
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    keychain.blank.store(true, Ordering::SeqCst);
    let first = { let database = stand.database.clone(); tokio::spawn(async move { database.run(epoch, "UserLocationUpdate".into(), json!({"input": {"lat": 1.0, "lng": 2.0}}).to_string()).await }) };
    let second = { let database = stand.database.clone(); tokio::spawn(async move { database.run(epoch, "DeviceRegister".into(), json!({"input": {"token": "t", "platform": "IOS"}}).to_string()).await }) };
    let _ = bound(async { first.await.unwrap() }).await;
    let _ = bound(async { second.await.unwrap() }).await;
    assert!(!keychain.blank.load(Ordering::SeqCst), "the blank read never happened");
    stand.settle().await;
    let sequences: Vec<i64> = stand.sequenced().into_iter().filter(|(operation, sequence)| operation != "login_link" && *sequence > 0).map(|(_, sequence)| sequence).collect();
    assert_eq!(sequences.windows(2).filter(|pair| pair[0] >= pair[1]).count(), 0, "a sequence repeated or went back: {sequences:?}");
    bound(stand.database.close_database()).await.unwrap();
    let (digest, sequence) = stored(&path);
    assert_eq!(sequence, sequences.last().copied().unwrap_or(2), "the stored sequence is not the last landed one");

    let next = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), keychain.clone()).await;
    next.answer("UserLocationUpdate", location());
    let epoch = next.database.session_epoch();
    bound(next.database.run(epoch, "UserLocationUpdate".into(), json!({"input": {"lat": 3.0, "lng": 4.0}}).to_string())).await.unwrap();
    let sent: Vec<i64> = next.sequenced().into_iter().map(|(_, sequence)| sequence).filter(|sequence| *sequence > 0).collect();
    assert_eq!(sent, vec![sequence + 1]);
    bound(next.database.close_database()).await.unwrap();
    assert_eq!(stored(&path).0, digest, "a second credential appeared during the run");
}
