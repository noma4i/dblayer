use super::crash_image_support::{drained, over_images, queue_now, reopen, saved_work, Capture};
use super::rig_tests::bound;
use super::upload_batch_shift_tests::OPERATIONS;
use super::upload_stand::{plain, Stand, CREATE_MOMENT};

const CREATED: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","text":"first","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const UPDATED: &str = r#"{"data":{"updateMoment":{"__typename":"Moment","id":"m77","uuid":"u","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const REFUSED: &str = r#"{"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}"#;
const UPDATE_MOMENT: (&str, &str) = ("UpdateMoment", "mutation UpdateMoment { updateMoment { id uuid text likedByMe media createdAt } }");

/// A process killed after any commit of a row update, whether the server answered it or refused it, leaves a file whose saved operation is the one the live run had, that finishes the update exactly once (no request when the answer was saved) with the row edited or rolled back, and never goes back from the edited row to the old one.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_cut_at_any_commit_of_a_row_update_finishes_it_once_or_finds_it_never_begun() {
    let operations = [OPERATIONS[0], OPERATIONS[1], OPERATIONS[2], CREATE_MOMENT, UPDATE_MOMENT];
    for (reply, edited) in [(UPDATED, true), (REFUSED, false)] {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let live = directory.path().join("live.sqlite");
        let database = stand.open(Some(&live), &operations).await;
        let epoch = database.session_epoch();
        stand.script.data("CreateMoment", CREATED);
        let created = bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap();
        bound(database.confirmed(created, false)).await.unwrap();
        drained(&database).await;
        super::crash_image_support::checkpoint_live(&database, &live).await;
        let before = database.peek_row(epoch, "Moment".into(), "m77".into()).await.unwrap().unwrap().cells;
        let base = Capture::begin(&live);
        stand.script.data("UpdateMoment", reply);
        let receipt = bound(database.update(epoch, "Moment".into(), "m77".into(), r#"{"text":"edited"}"#.into(), "{}".into())).await.unwrap();
        let _ = bound(database.confirmed(receipt, false)).await;
        drained(&database).await;
        let after = database.peek_row(epoch, "Moment".into(), "m77".into()).await.unwrap().unwrap().cells;
        assert_eq!(after != before, edited, "the live run ends with the row edited or rolled back");
        let (capture, files) = base.end(&live);
        let secret = stand.rig.credentials.inner.value().unwrap();

        let cuts = over_images(&capture, &files, |index, image| {
            let (secret, operations, before, after) = (secret.clone(), operations, before.clone(), after.clone());
            async move {
                let (saved, answered) = saved_work(&image);
                let mut held_slot = None;
                let (fresh, reopened) = reopen(&image, &secret, &operations, |fresh| { held_slot = Some(fresh.script.hold("UpdateMoment", reply)); }).await;
                let held = held_slot.take().unwrap();
                let epoch = reopened.session_epoch();
                crate::test_wait::settled(&reopened, "opened", || true).await;
                if answered == 0 { assert_eq!(queue_now(&reopened).await as i64, saved, "cut {index}: the open keeps the saved operation until it is answered"); }
                held.release();
                drained(&reopened).await;
                let row = reopened.peek_row(epoch, "Moment".into(), "m77".into()).await.unwrap().unwrap().cells;
                let sent = fresh.script.count("UpdateMoment");
                assert_eq!(sent, usize::from(saved == 1 && answered == 0), "cut {index}: the update goes out once, never when the answer is saved");
                if saved == 1 { assert_eq!(row, after, "cut {index}: the unfinished update finishes"); } else { assert!(row == before || row == after, "cut {index}: the row is old or finished"); }
                bound(reopened.close_database()).await.unwrap();
                (row == after && edited, saved, answered)
            }
        }).await;
        let landed: Vec<bool> = cuts.iter().map(|cut| cut.0).collect();
        assert!(cuts.iter().any(|cut| (cut.1, cut.2) == (1, 0)) && cuts.iter().any(|cut| cut.2 == 1), "the cuts hold the update unanswered and answered: {cuts:?}");
        assert!(landed.windows(2).all(|pair| !pair[0] || pair[1]), "an edited row never goes back: {landed:?}");
        assert_eq!(*landed.last().unwrap(), edited, "the last cut holds the end of the update");
        database.close_now();
    }
}
