//! A process killed after any commit of a server delete of a row that has an open operation leaves a file that reopens with the row gone or kept as the commit says and the queue empty. Requirement -> observed value -> assert.

use std::collections::BTreeSet;

use serde_json::json;

use super::crash_image_classes_tests::{flips, rows};
use super::crash_image_support::{checkpoint_live, drained, over_images, saved_work, Capture};
use super::rig_events::events;
use super::rig_tests::{bound, Fault, Rig, World};

const MESSAGE: &str = "m1";

fn present(image: &std::path::Path) -> bool { rows(image, "row_Message").iter().any(|id| id == MESSAGE) }

struct Cut {
    present_before: bool,
    present_after: bool,
    saved: i64,
    pairs: BTreeSet<String>,
}

/// A held update of a row, then the server event that deletes the row through the feed, then the refusal of the update, killed at any commit: the row is present before the delete commit and absent after it, an image after the delete commit sends no new request of the row, only the replay of the pair sent before it (P-OUT-19, mutation without an answer), and an earlier image sends the operation with one pair.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_server_delete_of_a_row_with_an_open_operation_reopens_consistent() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let (rig, credentials) = Rig::signed_on_file(&live, World::new("server", "A", true), events(|_| {})).await;
    rig.land("Message", json!({"id": MESSAGE, "chatId": "c1", "text": "first", "at": 1})).await;
    let epoch = rig.database.session_epoch();
    checkpoint_live(&rig.database, &live).await;
    let base = Capture::begin(&live);
    let held = rig.world.hold_then("MessageEdit", Fault::Business);
    let values = json!({"text": "edited"}).to_string();
    let receipt = bound(rig.database.act(epoch, "Message".into(), MESSAGE.into(), "edit".into(), values.clone(), values)).await.unwrap();
    held.entered().await;
    let sent_pair = rig.world.log().into_iter().find(|entry| entry.operation == "MessageEdit").and_then(|entry| entry.request_id).expect("the held request carries its pair");
    rig.land_unsettled("Message", json!({"id": MESSAGE, "deleted": true})).await;
    crate::test_wait::settled(&rig, "the delete shows", || rig.database.peek_row_now(epoch, "Message".into(), MESSAGE.into()).unwrap().is_none()).await;
    held.release();
    let outcome = bound(rig.database.confirmed(receipt, false)).await;
    assert!(matches!(&outcome, Err(crate::ffi::DbError::Interrupted { detail }) if detail == "Discarded"), "P-OUT-19: the operation of a row the server deleted ends Discarded: {outcome:?}");
    rig.settle().await;
    assert!(rig.database.peek_row_now(epoch, "Message".into(), MESSAGE.into()).unwrap().is_none(), "the refusal of the update does not bring back a row the server deleted");
    let (capture, files) = base.end(&live);
    let secret = credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &files, |_, image| {
        let secret = secret.clone();
        async move {
            let present_before = present(&image);
            let (saved, _) = saved_work(&image);
            let world = World::new("server", "A", true);
            world.fail_next("MessageEdit", Fault::Business);
            let reopened = Rig::reopened_on_file(&image, &secret, world.clone(), events(|_| {})).await;
            drained(&reopened.database).await;
            bound(reopened.database.close_database()).await.unwrap();
            let pairs = world.log().into_iter().filter(|entry| entry.operation == "MessageEdit").filter_map(|entry| entry.request_id).collect::<BTreeSet<_>>();
            Cut { present_before, present_after: present(&image), saved, pairs }
        }
    }).await;
    for (index, cut) in cuts.iter().enumerate() {
        assert_eq!(cut.present_after, cut.present_before, "cut {index}: the refusal of the open operation changes whether the row exists");
        assert!(cut.pairs.len() <= 1, "cut {index}: the operation goes out with one pair: {:?}", cut.pairs);
        assert!(cut.present_before || cut.pairs.iter().all(|pair| *pair == sent_pair), "cut {index}: P-OUT-19, after the delete commit only the replay of the pair sent before it goes out: {:?} against {sent_pair}", cut.pairs);
    }
    let shown: Vec<bool> = cuts.iter().map(|cut| cut.present_before).collect();
    assert!(shown[0] && !shown.last().unwrap() && flips(&shown) == 1, "the row is there, then deleted, never back: {shown:?}");
    assert!(cuts.iter().any(|cut| cut.saved > 0 && cut.present_before), "a cut holds the open operation with the row");
    rig.database.close_now();
}
