use std::sync::{Arc, Mutex};

use super::rig_tests::bound;
use super::upload_stand::{clip, watched, Stand, CREATE_MOMENT};
use crate::ffi::RoundJobsSink;
use crate::test_wait::yield_until;

#[derive(Default)]
struct Jobs {
    asked: Mutex<Vec<(u64, u64)>>,
    ended: Mutex<Vec<u32>>,
}

impl RoundJobsSink for Jobs {
    fn schedule_job(&self, job: crate::ffi::RoundJob) { self.asked.lock().unwrap().push((job.request_id, job.epoch)); }
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
    yield_until("the first round is asked", || !jobs.asked.lock().unwrap().is_empty()).await;
    let (request, round_epoch) = jobs.asked.lock().unwrap()[0];

    database.round_refused(request, round_epoch, crate::ffi::RoundRefusal::JobRefused, String::new());

    yield_until("the row shows the refusal", || refusal(&rows).is_some_and(|message| message.contains("while the app was not visible"))).await;
    crate::ffi::OnlineSink::of_life(&database.life).enter_foreground();
    yield_until("the next round clears the refusal", || refusal(&rows).is_none()).await;
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
    let ports = crate::ffi::OpenPorts { transport: Some(stand.puts.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let database = crate::ffi::Database::open(crate::ffi::Config { path: None, data_version: None }, crate::ffi::specs(models), ports).await.unwrap();
    let declared: Vec<_> = STEPS.iter().map(|(name, document)| serde_json::json!({"name": name, "document": document})).collect();
    database.register_operations(serde_json::Value::Array(declared).to_string()).await.unwrap();
    let jobs = Arc::new(Jobs::default());
    database.set_round_jobs(jobs.clone());

    database.round_started(0, 0, database.session_epoch());

    yield_until("the job of the empty round ends", || jobs.ended.lock().unwrap().contains(&0)).await;
    assert!(jobs.asked.lock().unwrap().is_empty(), "an idle database asked for a job");
    database.close_now();
}
