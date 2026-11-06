use super::rig_tests::{bound, Rig};
use crate::ffi::DbError;

#[tokio::test(flavor = "multi_thread")]
async fn a_query_of_a_database_without_a_session_is_refused_before_the_transport() {
    let rig = Rig::encrypted().await;
    let refused = bound(rig.database.run(rig.database.session_epoch(), "MyProfile".into(), "{}".into())).await;
    assert!(matches!(refused, Err(DbError::NoSession { .. })), "the run was not refused as no session: {refused:?}");
    assert_eq!(rig.world.hits(), 0, "the refused run reached the transport");
    rig.database.close_now();
}
