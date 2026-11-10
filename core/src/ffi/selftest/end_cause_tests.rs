//! D-263 (class K-ENDCAUSE): every public wait of the bridge that ends by a cause carries that cause to the caller by one mapping; a cancelled upload ends with the outcome `Discarded` of its removal (transfer.md Removing x Committed{Cancel}). `pull`, the load wait, the write and upload outcome waits map `Closed` -> `Closed`, `SessionEnded` -> `NoSession`, `Cancelled`/`Discarded` -> `Interrupted` with the cause in the detail. `call` and `session_call` are excluded (D-253 keeps `Interrupted`). The table drives each wait to each cause through the public API on the Rig and the upload Stand.
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Wake, Waker};

use super::rig_tests::{bound, Rig, BOUND};
use super::upload_stand::{plain, request, Stand, CREATE_MOMENT};
use crate::ffi::{Database, DbError, EndCause, ReadRequest, ReadStream, ReadValue};
use crate::test_matrix::Matrix;
use crate::test_wait::within_named;

fn err_tag(error: &DbError) -> String {
    match error {
        DbError::Closed { .. } => "Closed".into(),
        DbError::NoSession { .. } => "NoSession".into(),
        DbError::Invalid { .. } => "Invalid".into(),
        DbError::Storage { .. } => "Storage".into(),
        DbError::Interrupted { detail } => {
            let lower = detail.to_lowercase();
            if lower.contains("cancel") {
                "Interrupted(cancelled)".into()
            } else if lower.contains("discard") {
                "Interrupted(discarded)".into()
            } else {
                "Interrupted(no cause)".into()
            }
        }
        DbError::Session { code, .. } => format!("Session({code:?})"),
        other => format!("{other:?}").split(|character: char| !character.is_alphanumeric()).next().unwrap_or("").to_string(),
    }
}

fn cause_tag(cause: &EndCause) -> String {
    match cause {
        EndCause::Closed => "Closed".into(),
        EndCause::SessionEnded => "NoSession".into(),
        EndCause::Cancelled => "Interrupted(cancelled)".into(),
        EndCause::Discarded => "Interrupted(discarded)".into(),
        EndCause::Invalid { .. } => "Invalid".into(),
        EndCause::Storage { .. } => "Storage".into(),
        EndCause::Internal { .. } => "Internal".into(),
    }
}

fn result_tag<T>(result: Result<T, DbError>) -> String {
    match result {
        Ok(_) => "value (no end)".into(),
        Err(error) => err_tag(&error),
    }
}

/// The end a reader sees: `Ended{cause}` carries the cause; a cancelled reader whose last value was taken gets `None`.
async fn drain_until_end(stream: &ReadStream) -> String {
    loop {
        match stream.pull(0).await {
            Some(ReadValue::Ended { cause }) => break cause_tag(&cause),
            None => break "Interrupted(cancelled)".into(),
            Some(_) => continue,
        }
    }
}

async fn refresh_profile(database: &Database, epoch: u64) -> String {
    result_tag(database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await)
}

#[derive(Clone, Copy)]
enum Wait { Run, SessionCall }

/// D-279 (bridge.md:65, calls.md:80, lifecycle.md:573): a call and a session_call held at the server end with the cause of the end, like every other wait.
async fn call_ended(wait: Wait, end: End) -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let operation = match wait { Wait::Run => "MyProfile", Wait::SessionCall => "login_link" };
    let gate = rig.world.hold_next(operation);
    let database = rig.database.clone();
    let call = rig.spawn(async move {
        match wait {
            Wait::Run => result_tag(database.run(epoch, "MyProfile".into(), "{}".into()).await),
            Wait::SessionCall => result_tag(database.session_call("login_link".into(), serde_json::json!({"account":"B"}).to_string()).await),
        }
    });
    gate.entered().await;
    match end {
        End::Close => rig.database.close_now(),
        _ => bound(rig.database.session_reset()).await.unwrap(),
    }
    gate.release();
    let observed = bound(call).await.unwrap();
    if !matches!(end, End::Close) { rig.database.close_now(); }
    observed
}

// --- load wait ---------------------------------------------------------------

async fn load_closed() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("MyProfile");
    let database = rig.database.clone();
    let call = rig.spawn(async move { refresh_profile(&database, epoch).await });
    gate.entered().await;
    rig.database.close_now();
    gate.release();
    bound(call).await.unwrap()
}

async fn load_session_ended() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("MyProfile");
    let database = rig.database.clone();
    let call = rig.spawn(async move { refresh_profile(&database, epoch).await });
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    gate.release();
    let observed = bound(call).await.unwrap();
    rig.database.close_now();
    observed
}

async fn load_cancelled() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("MyProfile");
    let request = rig.database.begin_load_all(epoch, "User".into(), Some("profile".into()), "{}".into(), true).unwrap();
    gate.entered().await;
    request.cancel();
    let observed = result_tag(within_named(BOUND, "load cancelled wait", request.result()).await);
    gate.release();
    rig.database.close_now();
    observed
}

// --- write outcome wait ------------------------------------------------------

#[derive(Clone, Copy)]
enum End {
    Close,
    SessionEnd,
    Cancel,
    Discard,
}

async fn write_outcome(end: End) -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(super::bridge_support_tests::message(epoch, "end", 3))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    match end {
        End::Close => rig.database.close_now(),
        End::SessionEnd => bound(rig.database.session_reset()).await.unwrap(),
        End::Discard => bound(rig.database.discard(epoch, "Message".into(), id, None)).await.unwrap(),
        End::Cancel => unreachable!("a write has no cancel"),
    }
    let observed = result_tag(bound(rig.database.confirmed(receipt, false)).await);
    rig.database.close_now();
    observed
}

async fn upload_outcome(end: End) -> String {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    database.set_online(false).unwrap();
    let epoch = database.session_epoch();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();
    let local = bound(database.send(plain("Moment", r#"{"text":"end"}"#, epoch))).await.unwrap().optimistic_value.unwrap();
    let id = local.id.clone();
    let receipt = bound(database.upload(epoch, "Moment".into(), id.clone(), "upload_clip".into(), request(&source))).await.unwrap();
    match end {
        End::Close => database.close_now(),
        End::SessionEnd => bound(database.session_reset()).await.unwrap(),
        End::Cancel => { let _ = bound(database.cancel_upload(epoch, "Moment".into(), id, "upload_clip".into())).await; }
        End::Discard => bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap(),
    }
    let observed = result_tag(bound(database.confirmed(receipt, false)).await);
    database.close_now();
    observed
}

// --- stream pull wait --------------------------------------------------------

async fn stream_closed() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let stream = rig.database.subscribe_values(epoch, ReadRequest::Changes);
    rig.database.close_now();
    drain_until_end(&stream).await
}

async fn stream_session_ended() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let stream = rig.database.subscribe_values(epoch, ReadRequest::Changes);
    bound(rig.database.session_reset()).await.unwrap();
    let observed = drain_until_end(&stream).await;
    rig.database.close_now();
    observed
}

async fn stream_cancelled() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let stream: Arc<ReadStream> = rig.database.subscribe_values(epoch, ReadRequest::Changes);
    let reader = stream.clone();
    let pull = rig.spawn(async move { drain_until_end(&reader).await });
    bound(stream.cancel()).await;
    let observed = bound(pull).await.unwrap();
    rig.database.close_now();
    observed
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_end_cause_per_wait() {
    let mut matrix = Matrix::new("D-263-end-cause-per-wait");
    let kind = "end cause";

    matrix.check("load wait", kind, "Closed", &cause_tag(&EndCause::Closed), &load_closed().await);
    matrix.check("load wait", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &load_session_ended().await);
    matrix.check("load wait", kind, "Cancelled", &cause_tag(&EndCause::Cancelled), &load_cancelled().await);

    matrix.check("write outcome", kind, "Closed", &cause_tag(&EndCause::Closed), &write_outcome(End::Close).await);
    matrix.check("write outcome", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &write_outcome(End::SessionEnd).await);
    matrix.check("write outcome", kind, "Discarded", &cause_tag(&EndCause::Discarded), &write_outcome(End::Discard).await);

    matrix.check("upload outcome", kind, "Closed", &cause_tag(&EndCause::Closed), &upload_outcome(End::Close).await);
    matrix.check("upload outcome", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &upload_outcome(End::SessionEnd).await);
    matrix.check("upload outcome", kind, "Cancelled", &cause_tag(&EndCause::Discarded), &upload_outcome(End::Cancel).await);
    matrix.check("upload outcome", kind, "Discarded", &cause_tag(&EndCause::Discarded), &upload_outcome(End::Discard).await);

    matrix.check("stream pull", kind, "Closed", &cause_tag(&EndCause::Closed), &stream_closed().await);
    matrix.check("stream pull", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &stream_session_ended().await);
    matrix.check("stream pull", kind, "Cancelled", &cause_tag(&EndCause::Cancelled), &stream_cancelled().await);

    matrix.check("call wait", kind, "Closed", &cause_tag(&EndCause::Closed), &call_ended(Wait::Run, End::Close).await);
    matrix.check("call wait", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &call_ended(Wait::Run, End::SessionEnd).await);
    matrix.check("session_call wait", kind, "Closed", &cause_tag(&EndCause::Closed), &call_ended(Wait::SessionCall, End::Close).await);
    matrix.check("session_call wait", kind, "SessionEnded", &cause_tag(&EndCause::SessionEnded), &call_ended(Wait::SessionCall, End::SessionEnd).await);

    matrix.finish();
}

struct AnswerWake(tokio::sync::Notify);

impl Wake for AnswerWake {
    fn wake(self: Arc<Self>) { self.0.notify_one(); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.notify_one(); }
}

fn poll_pending<F: Future>(future: Pin<&mut F>, wake: &Arc<AnswerWake>) {
    let waker = Waker::from(wake.clone());
    assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
}

async fn answer_woke(wake: &AnswerWake) { within_named(BOUND, "owner answered the public call", wake.0.notified()).await; }

async fn command_after_answer() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    rig.database.set_online(false).unwrap();
    let epoch = rig.database.session_epoch();
    let wake = Arc::new(AnswerWake(tokio::sync::Notify::new()));
    let mut call = Box::pin(rig.database.send(super::bridge_support_tests::message(epoch, "held", 3)));
    poll_pending(call.as_mut(), &wake);
    answer_woke(&wake).await;
    let writer = rig.database.db.lock();
    poll_pending(call.as_mut(), &wake);
    drop(writer);
    answer_woke(&wake).await;
    rig.database.close_now();
    result_tag(bound(call).await)
}

async fn read_after_answer() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let wake = Arc::new(AnswerWake(tokio::sync::Notify::new()));
    let writer = rig.database.db.lock();
    let mut call = Box::pin(rig.database.read_row(epoch, "User".into(), "1".into()));
    poll_pending(call.as_mut(), &wake);
    drop(writer);
    answer_woke(&wake).await;
    rig.database.close_now();
    result_tag(bound(call).await)
}

async fn load_after_answer() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("MyProfile");
    let wake = Arc::new(AnswerWake(tokio::sync::Notify::new()));
    let mut call = Box::pin(rig.database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()));
    poll_pending(call.as_mut(), &wake);
    answer_woke(&wake).await;
    poll_pending(call.as_mut(), &wake);
    gate.entered().await;
    gate.release();
    answer_woke(&wake).await;
    rig.database.close_now();
    result_tag(bound(call).await)
}

async fn outcome_after_answer() -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(super::bridge_support_tests::message(epoch, "held", 3))).await.unwrap();
    let row = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let wake = Arc::new(AnswerWake(tokio::sync::Notify::new()));
    let mut call = Box::pin(rig.database.confirmed(receipt, false));
    poll_pending(call.as_mut(), &wake);
    bound(rig.database.discard(epoch, "Message".into(), row, None)).await.unwrap();
    answer_woke(&wake).await;
    rig.database.close_now();
    result_tag(bound(call).await)
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_owner_answer_survives_close() {
    let actual = (command_after_answer().await, read_after_answer().await, load_after_answer().await, outcome_after_answer().await);
    let value = String::from("value (no end)");
    assert_eq!(actual, (value.clone(), value.clone(), value, cause_tag(&EndCause::Discarded)), "an answer the owner gave before the close reaches the caller unchanged (P-BND-11, D-281)");
}
