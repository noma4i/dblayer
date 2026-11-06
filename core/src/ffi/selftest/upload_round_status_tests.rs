use std::sync::{Arc, Mutex};

use super::rig_tests::bound;
use super::upload_stand::{clip, watched, Stand, CREATE_MOMENT};
use crate::ffi::RoundJobsSink;
use crate::test_wait::yield_until;

#[derive(Default)]
struct Jobs {
    asked: Mutex<Vec<(u64, u64)>>,
}

impl RoundJobsSink for Jobs {
    fn schedule_job(&self, request_id: u64, epoch: u64) { self.asked.lock().unwrap().push((request_id, epoch)); }
    fn end_job(&self, _: u32) {}
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

    database.round_refused(request, round_epoch, "the app was not visible".into());

    yield_until("the row shows the refusal", || refusal(&rows).is_some_and(|message| message.contains("the app was not visible"))).await;
    crate::ffi::OnlineSink { life: Arc::downgrade(&database.life) }.enter_foreground();
    yield_until("the next round clears the refusal", || refusal(&rows).is_none()).await;
    database.close_now();
}
