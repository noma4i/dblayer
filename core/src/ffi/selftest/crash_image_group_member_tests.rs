//! A process killed after any commit of a discarded member or of a refused member of a batch upload leaves a file that reopens with the group whole or unsent. Requirement -> observed value -> assert.

use std::collections::HashSet;
use std::path::Path;

use super::crash_image_classes_tests::{flips, rows};
use super::crash_image_group_tests::ASSETS as ASSETS_OF_THREE;
use super::crash_image_support::{advancing, checkpoint_live, copy_tree, drained, files_of, over_images, reopen_timed, saved_work, unaccepted, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, items, ASSETS as ASSETS_OF_TWO, OPERATIONS};
use super::upload_stand::{shows, watched, Stand};
use crate::ffi::upload_wire::WireUploadBatch;
use crate::ffi::wire::WireSyncState;

const REFUSED_BLOB: &str = r#"{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"#;
const FILES: [&str; 3] = ["0.jpg", "1.jpg", "2.jpg"];

fn batch() -> WireUploadBatch { WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() } }

fn upload_status(image: &Path, row_id: &str) -> Option<String> {
    let connection = rusqlite::Connection::open(image).unwrap();
    rusqlite::OptionalExtension::optional(connection.query_row("SELECT status FROM sync_upload WHERE row_id = ?1", [row_id], |row| row.get::<_, String>(0))).unwrap()
}

fn distinct_pairs(operation: Vec<(String, Option<i64>)>) -> usize { operation.into_iter().map(|pair| pair.0).collect::<HashSet<_>>().len() }

struct Discarded {
    member_alive: bool,
    begun: bool,
    saved: i64,
    unaccepted: bool,
    after: Vec<String>,
    asked_member: usize,
    keys: Vec<String>,
    final_requests: usize,
    blob_requests: Vec<usize>,
}

/// A discard of the first member of a three file batch, while the second file's step is held, killed at any commit: the image lands the whole batch with the member, or the rest of the group without it, and a discarded member is not asked again nor named in the final request.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_discarded_group_member_reopens_with_the_rest_of_the_group_whole() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let sources = tempfile::tempdir().unwrap();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let epoch = database.session_epoch();
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    stand.script.data("CreateBlob", &blob(0));
    let held = stand.script.hold("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", ASSETS_OF_TWO);
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items(sources.path()), batch())).await.unwrap();
    let member = receipt.items[0].optimistic_value.id.clone();
    held.entered().await;
    let kept = directory.path().join("kept.files");
    copy_tree(&files_of(&live), &kept);
    bound(database.discard(epoch, "Asset".into(), member.clone(), Some("create_files".into()))).await.unwrap();
    held.release();
    crate::test_wait::settled(&database, "the rest of the group landed", || ["a1", "a2"].iter().all(|id| database.peek_row_now(epoch, "Asset".into(), id.to_string()).unwrap().is_some())).await;
    drained(&database).await;
    let (capture, _) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &kept, |_, image| {
        let (secret, member) = (secret.clone(), member.clone());
        async move {
            let before = rows(&image, "row_Asset");
            let (member_alive, begun) = (before.contains(&member), !before.is_empty());
            let (saved, _) = saved_work(&image);
            let unaccepted = unaccepted(&image);
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &OPERATIONS, |fresh| {
                for index in 0..3 { fresh.script.data("CreateBlob", &blob(index)); }
                fresh.script.data("CreateAssets", if member_alive { ASSETS_OF_THREE } else { ASSETS_OF_TWO });
            }, false).await;
            advancing(&clock, drained(&reopened)).await;
            bound(reopened.close_database()).await.unwrap();
            let mut after = rows(&image, "row_Asset");
            after.sort();
            let keys = fresh.script.variables("CreateAssets").iter().flat_map(|sent| sent["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap().to_string())).collect();
            Discarded { member_alive, begun, saved, unaccepted, after, asked_member: fresh.script.pairs("CreateBlob", FILES[0]).len(), keys, final_requests: distinct_pairs(fresh.script.operation_pairs("CreateAssets")), blob_requests: FILES.iter().map(|name| distinct_pairs(fresh.script.pairs("CreateBlob", name))).collect() }
        }
    }).await;
    for (index, cut) in cuts.iter().enumerate() {
        let (landed, named): (&[&str], &[&str]) = if cut.member_alive { (&["a0", "a1", "a2"], &["key-0", "key-1", "key-2"]) } else { (&["a1", "a2"], &["key-1", "key-2"]) };
        assert!(cut.after.is_empty() || cut.after == landed, "cut {index}: the group lands whole or not at all: {:?}", cut.after);
        assert!(cut.after == landed || !cut.begun || cut.unaccepted, "cut {index}: a begun group is finished: saved {}, rows {:?}", cut.saved, cut.after);
        assert!(cut.after.iter().all(|id| !id.starts_with("temp-")), "cut {index}: a temporary row is left: {:?}", cut.after);
        assert!(cut.keys.is_empty() || cut.keys == named, "cut {index}: the final request names the members that are left: {:?}", cut.keys);
        assert!(cut.member_alive || !cut.begun || cut.asked_member == 0, "cut {index}: a discarded member is asked again {} times", cut.asked_member);
        assert!(cut.final_requests <= 1, "cut {index}: the rows are created by one request: {}", cut.final_requests);
        assert!(cut.blob_requests.iter().all(|asked| *asked <= 1), "cut {index}: a file is asked once: {:?}", cut.blob_requests);
    }
    let alive: Vec<bool> = cuts.iter().map(|cut| cut.member_alive).collect();
    assert!(!alive[0] && !alive.last().unwrap() && flips(&alive) == 2, "the member is created, then discarded, never created again: {alive:?}");
    assert!(cuts.iter().any(|cut| cut.member_alive && cut.after.len() == 3), "a cut holds the whole group with the member");
    assert_eq!(cuts.last().unwrap().after, ["a1", "a2"]);
    database.close_now();
}

struct Refused {
    begun: bool,
    failed_in_image: bool,
    failed_after: Option<String>,
    unaccepted: bool,
    after: Vec<String>,
    calls: Vec<String>,
    blob_requests: Vec<usize>,
}

/// A group whose first member is refused by the server, killed at any commit: the image shows that member failed, keeps all three rows, and never sends the final request or a single create for a group with a failed member.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_refused_group_member_reopens_failed_and_the_group_unsent() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let sources = tempfile::tempdir().unwrap();
    let database = stand.open(Some(&live), &OPERATIONS).await;
    let epoch = database.session_epoch();
    checkpoint_live(&database, &live).await;
    let base = Capture::begin(&live);
    stand.script.data("CreateBlob", REFUSED_BLOB);
    stand.script.data("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items(sources.path()), batch())).await.unwrap();
    let ids: Vec<String> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    let shown = watched(&database, "Asset", &ids[0]);
    crate::test_wait::settled(&database, "the refused member failed and the others are ready", || shows(&shown, WireSyncState::Failed)).await;
    let (capture, files) = base.end(&live);
    let secret = stand.rig.credentials.inner.value().unwrap();

    let cuts = over_images(&capture, &files, |_, image| {
        let (secret, member) = (secret.clone(), ids[0].clone());
        async move {
            let begun = !rows(&image, "row_Asset").is_empty();
            let failed_in_image = upload_status(&image, &member).as_deref() == Some("failed");
            let unaccepted = unaccepted(&image);
            let (fresh, reopened, clock) = reopen_timed(&image, &secret, &OPERATIONS, |fresh| {
                if !failed_in_image { fresh.script.data("CreateBlob", REFUSED_BLOB); }
                fresh.script.data("CreateBlob", &blob(1));
                fresh.script.data("CreateBlob", &blob(2));
            }, false).await;
            advancing(&clock, crate::test_wait::settled(&reopened, "the work of the image is quiet", || true)).await;
            bound(reopened.close_database()).await.unwrap();
            let mut after = rows(&image, "row_Asset");
            after.sort();
            Refused { begun, failed_in_image, failed_after: upload_status(&image, &member), unaccepted, after, calls: fresh.script.names(), blob_requests: FILES.iter().map(|name| distinct_pairs(fresh.script.pairs("CreateBlob", name))).collect() }
        }
    }).await;
    let mut all = ids.clone();
    all.sort();
    for (index, cut) in cuts.iter().enumerate() {
        assert!(cut.begun || (cut.calls.is_empty() && cut.after.is_empty()), "cut {index}: an image before the batch sends and shows nothing: {:?} {:?}", cut.calls, cut.after);
        assert!(!cut.begun || cut.unaccepted || cut.failed_after.as_deref() == Some("failed"), "cut {index}: the refused member is failed: {:?}", cut.failed_after);
        assert!(!cut.begun || cut.unaccepted || cut.after == all, "cut {index}: the three rows are kept: {:?}", cut.after);
        assert!(cut.calls.iter().all(|call| call == "CreateBlob"), "cut {index}: only files are asked for in a group with a failed member: {:?}", cut.calls);
        assert!(cut.blob_requests.iter().all(|asked| *asked <= 1), "cut {index}: a file is asked once: {:?}", cut.blob_requests);
    }
    let begun: Vec<bool> = cuts.iter().map(|cut| cut.begun).collect();
    assert!(!begun[0] && *begun.last().unwrap() && flips(&begun) == 1, "{begun:?}");
    assert!(cuts.iter().any(|cut| cut.begun && !cut.failed_in_image), "a cut holds the group begun before the refusal");
    assert!(cuts.last().unwrap().failed_in_image, "the last cut holds the refusal");
    database.close_now();
}
