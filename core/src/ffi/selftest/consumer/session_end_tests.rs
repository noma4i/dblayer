//! P-SESS-25: a server end writes the credential removal with the landing, so a reopen of the file after the server ended the session finds no credential and no pending.
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;

fn stored(path: &Path) -> (bool, bool) {
    let db = crate::store::Store::open_models(Some(path), vec![], None).map(Arc::new).unwrap();
    let inner = db.lock();
    let session = crate::store::session_state::session(&inner.connection).unwrap();
    let pending = crate::store::session_state::pending(&inner.connection).unwrap();
    (session.is_some(), pending.is_some())
}

#[tokio::test(flavor = "multi_thread")]
async fn sess_prop_server_end_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.settle().await;
    assert!(stand.database.session_epoch() > 0, "the sign-in opened a session");
    stand.answer_next("UserLocationUpdate", json!({"errors": [{"message": "session ended", "extensions": {"code": "SESSION_ENDED", "reasonCode": "session_ended"}}], "data": {"userLocationUpdate": null}}));
    let epoch = stand.database.session_epoch();
    let ended = bound(stand.database.run(epoch, "UserLocationUpdate".into(), json!({"input": {"lat": 1.0, "lng": 2.0}}).to_string())).await;
    assert!(ended.is_err(), "the server end fails the call: {ended:?}");
    stand.settle().await;
    bound(stand.database.close_database()).await.unwrap();
    assert_eq!(stored(&path), (false, false), "the reopened file carries no credential and no pending of the ended session");
}
