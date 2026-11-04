use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::upload_tests::{file, input, multipart_replies, rid, small, upload_rig, TWELVE};
use crate::transport::PutReply;
use crate::upload::{UploadInfo, UploadStatus};

#[tokio::test(start_paused = true)]
async fn inv_upload_15_every_step_of_an_upload_reaches_the_observer_with_current_bytes_and_a_rising_fraction() {
    let rig = upload_rig(small());
    let seen: Arc<Mutex<Vec<Option<UploadInfo>>>> = Arc::default();
    let sink = seen.clone();
    let _observer = rig.db.observe_row("moments", &rid("m1"), move |change| sink.lock().unwrap().push(change.rows.first().and_then(|row| row.sync.uploads.get("upload").cloned()))).unwrap();
    multipart_replies(&rig);
    let db = rig.db.clone();
    rig.transport.respond_with(move |_| {
        db.await_revision(db.revision_seq().unwrap());
        None
    });
    let db = rig.db.clone();
    rig.transport.on_put_progress(move || db.await_revision(db.revision_seq().unwrap()));
    for number in 1..=3 {
        rig.transport.enqueue_put(&format!("https://s3/p{number}"), PutReply::etag("e").after(Duration::from_secs(2)));
    }
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());

    let infos = seen.lock().unwrap().clone();
    let shown: Vec<UploadInfo> = infos.iter().flatten().cloned().collect();
    let rank = |status: UploadStatus| match status {
        UploadStatus::Waiting => 0,
        UploadStatus::Preparing | UploadStatus::Queued => 1,
        UploadStatus::Uploading => 2,
        _ => 3,
    };
    assert!(shown.windows(2).all(|pair| rank(pair[0].status) <= rank(pair[1].status)), "statuses went back: {shown:?}");
    assert!(shown.iter().all(|info| (info.total_bytes == 12 || info.total_bytes == 0) && (0.0..=1.0).contains(&info.fraction())), "bytes or fraction out of range: {shown:?}");
    let uploading: Vec<u64> = shown.iter().filter(|info| info.status == UploadStatus::Uploading).map(|info| info.sent_bytes).collect();
    assert!(uploading.windows(2).all(|pair| pair[0] <= pair[1]), "bytes went down: {uploading:?}");
    assert!(uploading.len() > 3 && uploading.last() == Some(&12), "bytes seen: {uploading:?}");
    assert!(shown.iter().any(|info| info.status == UploadStatus::Sending));
    assert_eq!(infos.last().cloned().unwrap(), None);
}
