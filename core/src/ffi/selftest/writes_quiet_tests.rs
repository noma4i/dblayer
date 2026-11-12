use super::bridge_support_tests::message;
use super::flow_fixtures::{add_chat, chat_row};
use super::rig_events::events;
use super::rig_tests::{bound, Rig, World};

/// P-XFER-77: a database with no declared uploads has no Transfer owner, and `writes_quiet` still waits for the operation in flight and its landing.
/// Observable: the answer of `writes_quiet` before and after the answer of the held request. Catches: an answer at once when Transfer is absent.
#[tokio::test(flavor = "multi_thread")]
async fn writes_quiet_waits_for_the_operations_without_transfers() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(add_chat)).await;
    rig.land("Chat", chat_row("c1", 10)).await;
    let hold = rig.world.hold_next("SendMessage");
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.send(message(epoch, "up", 30))).await.unwrap();
    hold.entered().await;
    let database = rig.database.clone();
    let quiet = tokio::spawn(async move { database.writes_quiet(epoch).await });
    let life = rig.database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert!(!quiet.is_finished(), "the operation is in flight: not quiet");
    hold.release();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    bound(quiet).await.unwrap();
    rig.database.close_now();
}
