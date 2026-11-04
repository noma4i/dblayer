use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::bridge_tests::{group, member_list, message, seed_members, strings};
use super::rig_tests::{bound, Rig};
use crate::ffi::CommandStatus;
use crate::test_wait::yield_until;

/// A member whose row is not cached is skipped by the optimistic join: the call reaches the server and ends with the answer, never with a missing-row error.
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_member_whose_row_is_not_cached_ends_with_the_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    seed_members(&rig, &created.id);
    let result = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u4", "u9"]}).to_string())).await;
    assert!(result.is_ok(), "the call failed: {:?}", result.as_ref().err());
    rig.settle().await;
    assert_eq!(rig.world.count("AddMember"), 1, "the add did not reach the server");
    assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (strings(&["u1", "u2", "u3", "u4"]), strings(&["u2", "u1", "u3", "u4"])), "the cached row joined, the missing one did not");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_read_between_its_confirming_commit_and_the_announcement_is_delivered() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    let shared = rig.database.db.shared.clone();
    let (in_window, read_done) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let (probe_shared, probe_window, probe_done) = (shared.clone(), in_window.clone(), read_done.clone());
    *shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.replaced.is_empty() { return; }
        probe_window.store(true, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while probe_shared.store.announce_waiters.load(Ordering::SeqCst) == 0 && !probe_done.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline, "the read of the command never came");
            std::thread::yield_now();
        }
    }));
    rig.database.set_online(true).unwrap();
    yield_until("the confirming commit is visible and not announced", || in_window.load(Ordering::SeqCst)).await;
    let reading = {
        let (database, receipt, done) = (rig.database.clone(), receipt.clone(), read_done.clone());
        tokio::task::spawn_blocking(move || {
            let status = database.command_status(receipt);
            done.store(true, Ordering::SeqCst);
            status
        })
    };
    assert_eq!(bound(reading).await.unwrap().unwrap(), CommandStatus::Delivered { id: "m1".into() });
    *shared.store.after_commit.lock().unwrap() = None;
    rig.database.close_now();
}
