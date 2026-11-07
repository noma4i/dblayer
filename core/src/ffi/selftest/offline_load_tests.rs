use super::rig_tests::{bound, Rig};
use crate::test_wait::yield_until;

fn profile_requests(rig: &Rig) -> usize { rig.world.log().iter().filter(|entry| entry.operation == "MyProfile").count() }

#[tokio::test(flavor = "multi_thread")]
async fn a_load_whose_request_is_at_the_server_waits_for_the_network_and_ends_with_the_new_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let before = profile_requests(&rig);
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("MyProfile");
    let loading = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await }) };
    held.entered().await;
    rig.database.set_online(false).unwrap();
    held.release();
    for _ in 0..50 { tokio::task::yield_now().await; }
    assert!(!loading.is_finished(), "the load ended while the network was away");
    assert_eq!(profile_requests(&rig), before + 1, "a request went out while the network was away");
    rig.database.set_online(true).unwrap();
    yield_until("the load goes out again", || profile_requests(&rig) == before + 2).await;
    bound(loading).await.unwrap().unwrap();
    rig.database.close_now();
}
