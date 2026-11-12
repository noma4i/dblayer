use std::sync::{Arc, Mutex};

use super::rig_tests::bound;
use super::upload_stand::{clip, watched, Stand, CREATE_MOMENT};
use crate::ffi::RoundJobsSink;
use crate::test_wait::settled;

#[derive(Default)]
struct Jobs {
    asked: Mutex<Vec<(u64, u64)>>,
    ended: Mutex<Vec<u32>>,
    rounds: Mutex<Vec<u32>>,
}

impl RoundJobsSink for Jobs {
    fn schedule_job(&self, job: crate::ffi::RoundJob) { self.asked.lock().unwrap().push((job.request_id, job.epoch)); self.rounds.lock().unwrap().push(job.round_id); }
    fn end_job(&self, round_id: u32) { self.ended.lock().unwrap().push(round_id); }
}

fn refusal(rows: &crate::ffi::tape::Rows) -> Option<String> {
    rows.0.lock().unwrap().first().and_then(|row| row.uploads.iter().find(|entry| entry.action == "upload_clip")).and_then(|entry| entry.upload.error.as_ref().map(|error| error.message.clone()))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_round_shows_its_reason_on_the_upload_until_the_next_round_is_asked() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let jobs = Arc::new(Jobs::default());
    database.set_round_jobs(jobs.clone());
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip");
    std::fs::write(&source, b"clip").unwrap();
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"clip"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let rows = watched(&database, "Moment", &sent.optimistic_value.unwrap().id);
    settled(&database, "the first round is asked", || !jobs.asked.lock().unwrap().is_empty()).await;
    let (request, round_epoch) = jobs.asked.lock().unwrap()[0];

    database.round_refused(request, round_epoch, crate::ffi::RoundRefusal::JobRefused, String::new());

    settled(&database, "the row shows the refusal", || refusal(&rows).is_some_and(|message| message.contains("while the app was not visible"))).await;
    let sink = crate::ffi::OnlineSink::of_life(&database.life);
    let registration = sink.register_foreground();
    sink.enter_foreground(registration);
    settled(&database, "the next round clears the refusal", || refusal(&rows).is_none()).await;
    let rounds = jobs.rounds.lock().unwrap().clone();
    assert_eq!(rounds.len(), 2, "one round before the refusal and one after it: {rounds:?}");
    assert_eq!(rounds.iter().collect::<std::collections::BTreeSet<_>>().len(), 2, "every round asked in the process has its own id: {rounds:?}");
    database.close_now();
}

const STEPS: [(&str, &str); 6] = [
    ("CreateBlob", "mutation CreateBlob { createBlob { url headers signedId blobId } }"),
    ("InitMultipart", "mutation InitMultipart { initMultipart { key uploadId } }"),
    ("PresignParts", "mutation PresignParts { presignParts { parts { partNumber url } } }"),
    ("CompleteMultipart", "mutation CompleteMultipart { completeMultipart { signedId blobId } }"),
    ("AbortMultipart", "mutation AbortMultipart { abortMultipart { ok } }"),
    ("CreateAsset", "mutation CreateAsset { createAsset { asset { id clientKey size position } } }"),
];

#[tokio::test(flavor = "multi_thread")]
async fn a_round_that_starts_with_nothing_unfinished_is_ended_at_once_before_any_session() {
    let stand = Stand::new();
    let models = include_str!("upload_wrapper_models.json").to_string();
    let ports = crate::ffi::OpenPorts { partial_operations: true, transport: Some(stand.puts.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let database = crate::ffi::Database::open(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(models), ports).await.unwrap();
    let declared: Vec<_> = STEPS.iter().map(|(name, document)| serde_json::json!({"name": name, "document": document})).collect();
    database.register_operations(serde_json::Value::Array(declared).to_string()).await.unwrap();
    let jobs = Arc::new(Jobs::default());
    database.set_round_jobs(jobs.clone());

    database.round_started(0, 0, database.session_epoch());

    settled(&database, "the job of the empty round ends", || jobs.ended.lock().unwrap().contains(&0)).await;
    assert!(jobs.asked.lock().unwrap().is_empty(), "an idle database asked for a job");
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn round_old_epoch_after_reopen_cannot_refuse_restored_upload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("round.sqlite");
    let source = directory.path().join("clip");
    std::fs::write(&source, b"clip").unwrap();
    let stand = Stand::new();
    let first = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    first.set_online(false).unwrap();
    let old_jobs = Arc::new(Jobs::default());
    first.set_round_jobs(old_jobs.clone());
    let old_epoch = first.session_epoch();
    let _receipt = bound(first.send(clip("Moment", r#"{"text":"clip"}"#, "upload_clip", &source, old_epoch))).await.unwrap();
    settled(&first, "the old job is scheduled", || !old_jobs.asked.lock().unwrap().is_empty()).await;
    let old_request = old_jobs.asked.lock().unwrap()[0].0;
    bound(first.close_database()).await.unwrap();
    drop(first);
    let reopened = stand.open(Some(&path), &[CREATE_MOMENT]).await;
    reopened.set_online(false).unwrap();
    let jobs = Arc::new(Jobs::default());
    reopened.set_round_jobs(jobs.clone());
    let new_epoch = reopened.session_epoch();
    assert_ne!(old_epoch, new_epoch);
    settled(&reopened, "the restored upload schedules a new job", || !jobs.asked.lock().unwrap().is_empty()).await;
    let new_request = jobs.asked.lock().unwrap()[0].0;
    let new_round = jobs.rounds.lock().unwrap()[0];
    let stream = reopened.subscribe_values(new_epoch, crate::ffi::ReadRequest::Operations);
    let Some(crate::ffi::ReadValue::Operations { items, .. }) = bound(stream.pull_next()).await else { panic!("queue absent") };
    let item = items.iter().find(|item| item.action == "upload_clip").unwrap();
    let rows = watched(&reopened, "Moment", &item.row_id);
    reopened.round_refused(old_request, old_epoch, crate::ffi::RoundRefusal::JobRefused, "old".into());
    reopened.round_ended(new_round, old_epoch);
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert!(refusal(&rows).is_none(), "the old epoch changed the restored upload");
    reopened.round_refused(new_request, new_epoch, crate::ffi::RoundRefusal::JobRefused, String::new());
    settled(&reopened, "the current job refusal reaches the same upload", || refusal(&rows).is_some()).await;
    let sink = crate::ffi::OnlineSink::of_life(&reopened.life);
    let registration = sink.register_foreground();
    sink.enter_foreground(registration);
    settled(&reopened, "a new job is scheduled", || jobs.asked.lock().unwrap().len() == 2).await;
    let current_request = jobs.asked.lock().unwrap()[1].0;
    let current_round = jobs.rounds.lock().unwrap()[1];
    reopened.round_started(current_request, current_round, new_epoch);
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    reopened.round_ended(current_round, old_epoch);
    sink.enter_foreground(registration);
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(jobs.asked.lock().unwrap().len(), 2, "the old epoch must not stop the current round");
    reopened.round_ended(current_round, new_epoch);
    sink.enter_foreground(registration);
    settled(&reopened, "the current epoch can end the job", || jobs.asked.lock().unwrap().len() == 3).await;
    bound(reopened.close_database()).await.unwrap();
}
