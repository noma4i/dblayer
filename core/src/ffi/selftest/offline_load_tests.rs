use super::rig_tests::{bound, Rig};
use crate::ffi::DbError;

#[tokio::test(flavor = "multi_thread")]
async fn a_load_whose_request_is_at_the_server_ends_with_the_network_error_when_the_network_goes_away() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("MyProfile");
    let loading = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "profile".into(), "{}".into()).await }) };
    held.entered_for("the profile load").await;
    rig.database.set_online(false).unwrap();
    let ended = bound(loading).await.unwrap();
    held.release();
    assert!(matches!(ended, Err(DbError::Network { .. })), "the load did not end with the network error: {ended:?}");
    rig.database.close_now();
}
