use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::upload_tests::{blob, created, file, input, rid, single_replies, small, upload_of, upload_rig};
use crate::db::AcceptedUpload;
use crate::sync::rig::Rig;
use crate::transport::{PutReply, PutTask, PutTaskState, Reply};
use crate::upload::{UploadScenario, UploadStage, UploadState};

type Accepted = Arc<Mutex<Vec<(String, Vec<AcceptedUpload>)>>>;

fn collect(rig: &Rig) -> Accepted {
    let seen: Accepted = Arc::default();
    let sink = seen.clone();
    rig.db.set_upload_accepted_sink(Arc::new(move |event| sink.lock().unwrap().push((event.model, event.items))));
    seen
}

fn finished(tag: &str, etag: &str) -> PutTask {
    PutTask { tag: tag.into(), state: PutTaskState::Finished { status: 200, etag: Some(etag.into()) } }
}

fn running(tag: &str) -> PutTask {
    PutTask { tag: tag.into(), state: PutTaskState::Running }
}

fn presign_all() -> Reply {
    Reply::data(json!({"presignParts": {"parts": [
        {"partNumber": 1, "url": "https://s3/p1"}, {"partNumber": 2, "url": "https://s3/p2"}, {"partNumber": 3, "url": "https://s3/p3"}
    ]}}))
}

fn part_stage_state() -> UploadState {
    UploadState {
        stage: UploadStage::Part, scenario: Some(UploadScenario::Multipart), total_bytes: 12,
        key: Some("k1".into()), upload_id: Some("u1".into()), ..UploadState::queued()
    }
}

#[tokio::test(start_paused = true)]
async fn an_accepted_upload_is_reported_once_with_its_exact_operation() {
    let rig = upload_rig(small());
    let seen = collect(&rig);
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue_put("https://s3/single", PutReply::status(400));
    let source = file(b"abcdef");
    let seq = rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(*seen.lock().unwrap(), vec![("moments".to_string(), vec![AcceptedUpload { seq, position: 0 }])]);
}

#[tokio::test(start_paused = true)]
async fn a_system_refusal_lands_in_the_error_of_its_own_upload_only() {
    let rig = upload_rig(small());
    let seen = collect(&rig);
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(60)));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let (first, second) = (file(b"abcdef"), file(b"fedcba"));
    rig.sync.upload("moments", &rid("m1"), "upload", input(&first)).await.unwrap();
    rig.sync.upload("moments", &rid("m2"), "upload", input(&second)).await.unwrap();
    let items = seen.lock().unwrap()[0].1.clone();
    assert_eq!(rig.db.note_background_refused(&items, "not visible").unwrap(), 1);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let refused = upload_of(&rig, "m1").unwrap();
    assert!(refused.error.is_some_and(|error| error.message.contains("not visible")), "the refusal was not kept while the run wrote its progress");
    assert!(upload_of(&rig, "m2").unwrap().error.is_none(), "the refusal reached another upload");
    let stored: i64 = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT count(*) FROM op_item WHERE background_refused IS NOT NULL", [], |row| row.get(0))?)).unwrap();
    assert_eq!(stored, 1, "the refusal was stored on more than one upload");
    rig.sync.wait_uploads().await;
}

#[tokio::test(start_paused = true)]
async fn only_the_requests_of_an_upload_carry_the_upload_mark() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateMoment", created("m8"));
    single_replies(&rig);
    rig.db.create("moments", crate::sync::rig::params(json!({"text": "plain"}))).unwrap();
    rig.sync.flush().await.unwrap();
    let plain = rig.transport.calls().len();
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let marks: Vec<_> = rig.transport.calls().into_iter().map(|call| (call.operation_name, call.upload)).collect();
    assert!(marks[..plain].iter().all(|(_, upload)| !upload), "{marks:?}");
    assert_eq!(marks[plain..], [("CreateBlob".to_string(), true), ("CreateMoment".to_string(), true)]);
}

#[tokio::test(start_paused = true)]
async fn a_single_put_sends_the_source_file_and_a_part_put_sends_its_own_range_file() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let single = rig.transport.put_calls();
    assert_eq!(single.len(), 1);
    assert_eq!((single[0].body.as_slice(), single[0].tag.rsplit('.').next()), (b"abcdef".as_slice(), Some("1")));

    let rig = upload_rig(small());
    rig.transport.enqueue("InitMultipart", Reply::data(json!({"initMultipart": {"key": "k1", "uploadId": "u1"}})));
    rig.transport.enqueue("PresignParts", presign_all());
    rig.transport.enqueue("CompleteMultipart", Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "b1"}})));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"0123456789ab");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let mut parts = rig.transport.put_calls();
    parts.sort_by(|left, right| left.url.cmp(&right.url));
    assert_eq!(parts.iter().map(|call| call.body.clone()).collect::<Vec<_>>(), [b"01234".to_vec(), b"56789".to_vec(), b"ab".to_vec()]);
    assert_eq!(parts.iter().map(|call| call.tag.rsplit('.').next().unwrap()).collect::<Vec<_>>(), ["1", "2", "3"]);
}

#[tokio::test(start_paused = true)]
async fn a_restart_with_live_and_finished_tasks_sends_no_put_for_them_and_takes_their_etags() {
    let rig = upload_rig(small());
    let source = file(b"0123456789ab");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), part_stage_state()).unwrap();
    rig.transport.enqueue("PresignParts", presign_all());
    rig.transport.enqueue("CompleteMultipart", Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "b1"}})));
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue_put("https://s3/p3", PutReply::etag("sent-3"));
    rig.sync.put_tasks(vec![finished(&format!("{seq}.0.1"), "adopted-1"), running(&format!("{seq}.0.2"))]);
    rig.sync.inner.start_uploads().unwrap();
    rig.transport.await_put_calls(1).await;
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 0, "the live task is still awaited");
    rig.sync.put_tasks(vec![finished(&format!("{seq}.0.2"), "adopted-2")]);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().iter().map(|call| call.url.as_str()).collect::<Vec<_>>(), ["https://s3/p3"]);
    let complete = rig.transport.calls().into_iter().find(|call| call.operation_name == "CompleteMultipart").unwrap();
    assert_eq!(complete.variables["parts"], json!([{"partNumber": 1, "etag": "adopted-1"}, {"partNumber": 2, "etag": "adopted-2"}, {"partNumber": 3, "etag": "sent-3"}]));
}

#[tokio::test(start_paused = true)]
async fn a_wake_waits_only_for_the_uploads_of_the_tasks_that_delivered_events() {
    let rig = upload_rig(crate::upload::UploadConfig { max_files: 2, ..small() });
    let (first, second) = (file(b"0123456789ab"), file(b"ba9876543210"));
    let waiting = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&first), part_stage_state()).unwrap();
    let woken = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m2"), "upload", &input(&second), part_stage_state()).unwrap();
    rig.transport.enqueue("PresignParts", presign_all());
    rig.transport.enqueue("CompleteMultipart", Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "b1"}})));
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.sync.put_tasks((1..=3).map(|part| running(&format!("{waiting}.0.{part}"))).collect());
    rig.sync.put_tasks((1..=3).map(|part| finished(&format!("{woken}.0.{part}"), "e")).collect());
    let tags: Vec<String> = (1..=3).map(|part| format!("{woken}.0.{part}")).collect();
    tokio::time::timeout(Duration::from_secs(30), rig.sync.resume_uploads_for(tags)).await.expect("the wake waited for an unrelated upload").unwrap();
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 1);
}
