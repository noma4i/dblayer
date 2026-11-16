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
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#);
    let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    database.set_online(false).await.unwrap();
    bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();
    presign.release();
    database.close_now();

    let abort = stand.script.hold("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    let reopened = stand.open_ordered(Some(&path), &operations, None, order).await;
    settled(&reopened, &format!("{order:?}: the abort reached the transport"), || stand.script.count("AbortMultipart") == 1).await;
    abort.entered().await;
    abort.release();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.count("AbortMultipart"), 1, "{order:?}: the abort goes out once, calls {:?}", stand.script.names());
    reopened.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saved_step_and_an_orphan_abort_go_out_once_in_every_open_order() {
    let tasks: Vec<_> = OpenOrder::ALL.into_iter().flat_map(|order| [tokio::spawn(a_saved_step_goes_out_once(order)), tokio::spawn(an_orphan_abort_goes_out_once(order))]).collect();
    for task in tasks { bound(task).await.unwrap(); }
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_reopen_discarded_batch_replays_in_every_open_order() {
    let tasks: Vec<_> = OpenOrder::ALL.into_iter().map(|order| tokio::spawn(async move {
        let stand = Stand::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let database = stand.open(Some(&path), &OPERATIONS).await;
        stand.script.data("CreateBlob", &blob(0));
        let held = std::sync::Arc::new(super::upload_stand::Hold::default());
        stand.script.enqueue("CreateBlob", super::upload_stand::Answer::Held(held.clone(), Box::new(super::upload_stand::Answer::Data(blob(1)))));
        let epoch = database.session_epoch();
        let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items(directory.path()), WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
        held.entered().await;
        bound(database.discard(epoch, "Asset".into(), receipt.items[0].optimistic_value.id.clone(), Some("create_files".into()))).await.unwrap();
        database.set_online(false).await.unwrap();
        held.release();
        held.dropped().await;
        bound(database.close_database()).await.unwrap();
        stand.script.data("CreateBlob", &blob(2));
        stand.script.data("CreateAssets", ASSETS);
        let reopened = stand.open_ordered(Some(&path), &OPERATIONS, None, order).await;
        let first = std::sync::Arc::new(crate::ffi::tape::Rows::default());
        let second = std::sync::Arc::new(crate::ffi::tape::Rows::default());
        let _first = reopened.watch(crate::ffi::ReadRequest::Entity { model: "Asset".into(), id: "a1".into(), fetch: crate::ffi::ReadFetch::Never }, first.clone());
        let _second = reopened.watch(crate::ffi::ReadRequest::Entity { model: "Asset".into(), id: "a2".into(), fetch: crate::ffi::ReadFetch::Never }, second.clone());
        settled(&reopened, &format!("{order:?}: both assets are stored"), || first.len() == 1 && second.len() == 1).await;
        let pairs = stand.script.pairs("CreateBlob", "1.jpg");
        assert_eq!(pairs.len(), 2, "{order:?}: {:?}", stand.script.names());
        assert_eq!(pairs[0], pairs[1], "{order:?}: the saved pair is replayed");
        assert_eq!(stand.script.count("CreateAssets"), 1, "{order:?}");
        let payload = stand.script.variables("CreateAssets");
        let keys: Vec<_> = payload[0]["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap()).collect();
        assert_eq!(keys, ["key-1", "key-2"], "{order:?}");
        bound(reopened.close_database()).await.unwrap();
    })).collect();
    for task in tasks { bound(task).await.unwrap(); }
}
