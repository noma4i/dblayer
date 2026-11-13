use super::crash_image_support::{drained, files_of};
use super::rig_tests::bound;
use super::upload_prepare_tests::{probed_image, send, shrink, upload_of, SOURCE};
use super::upload_stand::{Answer, ScriptedPreparer, Stand, CREATE_MOMENT};
use crate::ffi::wire::WireUploadStatus;
use crate::owners::write::upload::MEBIBYTE;

const INIT: &str = r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#;
const ABORTED: &str = r#"{"data":{"abortMultipart":{"ok":true}}}"#;

/// Requirement (D-372, D-388, D-417): every end of a transfer removes its prepared file and the copy of the library, and the discard of a multipart upload aborts it. Input: a source with prepare settings whose result goes as a multipart upload, a refusal after the multipart was created, `discard`. Observable: the abort calls of the server double, the files of the base, the queue stream. Catches: a prepare file left behind, a multipart left open.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_discard_of_a_failed_prepared_multipart_leaves_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let file = directory.path().join("clip.jpg");
    std::fs::write(&file, SOURCE).unwrap();
    let result = vec![7u8; (11 * MEBIBYTE) as usize];
    let stand = Stand::new().with_preparer(ScriptedPreparer::new(probed_image(), &result, &[]));
    let database = stand.open(Some(&live), &[CREATE_MOMENT]).await;
    let epoch = database.session_epoch();
    stand.script.data("InitMultipart", INIT);
    stand.script.enqueue("PresignParts", Answer::Status(422));
    stand.script.data("AbortMultipart", ABORTED);
    let receipt = bound(database.send(send(epoch, &file, shrink()))).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    crate::test_wait::settled(&database, "the upload failed after the multipart was created", || upload_of(&database, &id).is_some_and(|upload| upload.status == WireUploadStatus::Failed)).await;
    assert_eq!(stand.script.count("InitMultipart"), 1, "the multipart exists on the server: {:?}", stand.script.names());
    bound(database.discard(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()))).await.unwrap();
    crate::test_wait::settled(&database, &format!("the abort went ({:?})", stand.script.names()), || stand.script.count("AbortMultipart") >= 1).await;
    drained(&database).await;
    let files: Vec<_> = std::fs::read_dir(files_of(&live).join("transfers")).map_or_else(|_| Vec::new(), |found| found.map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned()).collect());
    assert_eq!(stand.script.count("AbortMultipart"), 1, "{:?}", stand.script.names());
    assert!(stand.script.variables("AbortMultipart")[0].to_string().contains("upload"), "{:?}", stand.script.variables("AbortMultipart"));
    assert!(files.is_empty(), "the directory of the base keeps {files:?}");
    assert!(upload_of(&database, &id).is_none(), "the discarded upload still shows");
    let before = stand.script.names();
    database.close_now();
    let reopened = stand.open(Some(&live), &[CREATE_MOMENT]).await;
    drained(&reopened).await;
    assert_eq!(stand.script.names(), before, "the reopened base asks the server again: nothing of the upload stays");
    assert!(upload_of(&reopened, &id).is_none(), "the discarded upload came back at the open");
    reopened.close_now();
}
