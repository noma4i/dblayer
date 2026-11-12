use serde_json::json;

use super::rig_tests::{bound, Rig};
use crate::ffi::DbError;
use crate::owners::contract::PublishedState;

const REFUSE: &str = "CREATE TRIGGER refuse_publication BEFORE INSERT ON manifest WHEN NEW.key = 'saved_epoch' BEGIN SELECT RAISE(ABORT, 'disk refused'); END";

/// life_storage_failed_retried_on_event, P-LIFE-25: a publication the disk refused refuses the calls of the app; the next event of the platform writes it again and the calls work.
#[tokio::test(flavor = "multi_thread")]
async fn life_storage_failed_exits_on_online_and_foreground() {
    let rig = Rig::encrypted_with_options(false, false).await;
    let database = &rig.database;
    database.db.lock().connection.execute_batch(REFUSE).unwrap();
    let _ = bound(database.session_call("login_link".into(), json!({"account": "A"}).to_string())).await;
    let failed = || database.life.published().state == PublishedState::StorageFailed;
    crate::test_wait::settled(database, "the refused publication is shown", failed).await;
    let refused = bound(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await;
    assert!(matches!(refused, Err(DbError::Storage { .. })), "a call while the disk refuses: {refused:?}");

    database.set_online(false).unwrap();
    bound(crate::ffi::settled(database)).await.unwrap();
    assert!(failed(), "going offline does not write");
    database.set_online(true).unwrap();
    bound(crate::ffi::settled(database)).await.unwrap();
    assert!(failed(), "the disk still refuses: the state stays");

    database.db.lock().connection.execute_batch("DROP TRIGGER refuse_publication").unwrap();
    let _foreground = database.life.foreground();
    crate::test_wait::settled(database, "the publication is written again", || !failed()).await;
    bound(database.run(database.session_epoch(), "SessionProfile".into(), "{}".into())).await.unwrap();
    database.close_now();
}
