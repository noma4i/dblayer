use serde_json::json;

use super::rig_tests::{bound, Rig};
use crate::ffi::DbError;

const REFUSE: &str = "CREATE TRIGGER refuse_publication BEFORE INSERT ON manifest WHEN NEW.key = 'saved_epoch' BEGIN SELECT RAISE(ABORT, 'disk refused'); END";

/// life_storage_failed_retried_on_event, P-LIFE-25: a publication the disk refused refuses the calls of the app; going online does not write it while the disk refuses, the foreground event writes it again and the calls work.
#[tokio::test(flavor = "multi_thread")]
async fn life_storage_failed_exits_on_foreground_and_not_on_online() {
    let rig = Rig::encrypted_with_options(false, false).await;
    let database = &rig.database;
    database.db.lock().connection.execute_batch(REFUSE).unwrap();
    let _ = bound(database.session_call("login_link".into(), json!({"account": "A"}).to_string())).await;
    let refused = || async { bound(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await };
    let first = refused().await;
    assert!(matches!(first, Err(DbError::Storage { .. })), "a call while the disk refuses: {first:?}");

    database.set_online(false).await.unwrap();
    bound(crate::ffi::settled(database)).await.unwrap();
    let offline = refused().await;
    assert!(matches!(offline, Err(DbError::Storage { .. })), "going offline does not write: {offline:?}");
    database.set_online(true).await.unwrap();
    bound(crate::ffi::settled(database)).await.unwrap();
    let online = refused().await;
    assert!(matches!(online, Err(DbError::Storage { .. })), "the disk still refuses: {online:?}");

    database.db.lock().connection.execute_batch("DROP TRIGGER refuse_publication").unwrap();
    let _foreground = database.life.foreground();
    bound(crate::ffi::settled(database)).await.unwrap();
    refused().await.unwrap();
    database.close_now();
}
