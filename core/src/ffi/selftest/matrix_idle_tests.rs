//! C7: idle and work counters. After a trace has settled, 24 hours pass on the test clock with no input: the core sends no request and arms no timer. A reconnect (online flap) rereads each observed window at most once.
use super::bridge_support_tests::message;
use super::rig_tests::{bound, Rig};
use crate::test_matrix::Matrix;

#[tokio::test(flavor = "multi_thread")]
async fn idle_costs_nothing_for_a_day() {
    let mut matrix = Matrix::new("C7-idle");
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(message(epoch, "m", 1))).await.unwrap();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    let _ = bound(rig.database.run(epoch, "MyProfile".into(), "{}".into())).await;
    bound(rig.settle()).await;
    let (requests, armed) = (rig.world.log().len(), rig.clock.armed());
    for hour in 1..=6u64 {
        rig.clock.advance(4 * 3_600_000);
        bound(rig.settle()).await;
        matrix.check(&format!("{} hours", hour * 4), "requests while idle", "after a settled trace", &requests.to_string(), &rig.world.log().len().to_string());
        matrix.check(&format!("{} hours", hour * 4), "timers armed while idle", "after a settled trace", &armed.to_string(), &rig.clock.armed().to_string());
    }
    rig.database.close_now();
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_reads_each_observed_window_once() {
    let mut matrix = Matrix::new("C7-reconnect");
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    bound(rig.settle()).await;
    let before = rig.world.log().iter().filter(|entry| entry.operation == "MyProfile").count();
    for flap in 1..=3u64 {
        rig.database.set_online(false).unwrap();
        bound(rig.settle()).await;
        rig.database.set_online(true).unwrap();
        bound(rig.settle()).await;
        let reads = rig.world.log().iter().filter(|entry| entry.operation == "MyProfile").count() - before;
        matrix.check(&format!("flap {flap}"), "reads of a window nobody watches after a reconnect", "online false then true", "0", &reads.to_string());
    }
    rig.database.close_now();
    matrix.finish();
}
