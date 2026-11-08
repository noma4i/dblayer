use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, items, ASSETS, OPERATIONS};
use super::upload_stand::{clip, OpenOrder, Stand, CREATE_MOMENT};
use crate::ffi::upload_wire::WireUploadBatch;
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::settled;

async fn a_saved_step_goes_out_once(order: OpenOrder) {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let database = stand.open(Some(&path), &OPERATIONS).await;
    stand.script.data("CreateBlob", &blob(0));
    let lost = stand.script.hold("CreateBlob", &blob(1));
    let epoch = database.session_epoch();
    bound(database.upload_create_batch(epoch, "Asset".into(), items(directory.path()), WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    lost.entered().await;
    database.close_now();
    lost.release();

    stand.script.data("CreateBlob", &blob(1));
    stand.script.data("CreateBlob", &blob(2));
    stand.script.data("CreateAssets", ASSETS);
    let reopened = stand.open_ordered(Some(&path), &OPERATIONS, None, order).await;
    settled(&reopened, &format!("{order:?}: the batch reaches CreateAssets"), || stand.script.count("CreateAssets") == 1).await;

    let pairs = stand.script.pairs("CreateBlob", "1.jpg");
    assert_eq!(pairs.len(), 2, "{order:?}: the saved step goes out once more, calls {:?}", stand.script.names());
    assert_eq!(pairs[0], pairs[1], "{order:?}: the replay carries the saved pair");
    assert_eq!(stand.script.count("CreateBlob"), 4, "{order:?}: no other step is repeated");
    reopened.close_now();
}

async fn an_orphan_abort_goes_out_once(order: OpenOrder) {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&path), &operations).await;
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    database.set_online(false).unwrap();
    bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();
    presign.release();
    database.close_now();

    let abort = stand.script.hold("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    let reopened = stand.open_ordered(Some(&path), &operations, None, order).await;
    settled(&database, &format!("{order:?}: the abort reached the transport"), || stand.script.count("AbortMultipart") == 1).await;
    abort.entered().await;
    abort.release();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.count("AbortMultipart"), 1, "{order:?}: the abort goes out once, calls {:?}", stand.script.names());
    reopened.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saved_step_and_an_orphan_abort_go_out_once_in_every_open_order() {
    for order in OpenOrder::ALL {
        a_saved_step_goes_out_once(order).await;
        an_orphan_abort_goes_out_once(order).await;
    }
}
