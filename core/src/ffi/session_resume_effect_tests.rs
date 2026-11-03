use super::sealed_errors_tests::{session, signed_in};
use super::session_guards_tests::Rig;
use super::SessionCode;
use crate::operation::OperationStatus;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};
use std::time::Duration;
use crate::pacing::ManualClock;

fn mutation_effects(rig: &Rig) -> usize {
    rig.server.executed().iter().filter(|body| body.get("extensions").is_some()).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_permanently_refused_pending_owner_is_failed_by_resume() {
  for resume in [false, true] {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    if resume { rig.server.interrupt_next_request(); } else { rig.server.status_next(400); }
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    database.flush().await.unwrap();
    if resume {
        assert_eq!(database.db.outbox().unwrap()[0].operation.status, OperationStatus::DeliveryUnknown);
        rig.server.status_next(400);
    }
    assert_eq!(database.session_resume().await.unwrap(), None);
    let entries = database.db.outbox().unwrap();
    assert_eq!(entries[0].operation.status, OperationStatus::Failed);
    assert_eq!(entries[0].operation.error.as_ref().unwrap().kind, crate::syncstate::FailureKind::Rejected);
    assert!(database.settled("Moment".into(), row.id, None).await.is_err());
    assert_eq!(database.session_resume().await.unwrap(), None);
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_create_refused_without_a_sequence_is_executed_once_by_resume_and_retry() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);

    let refused = database.settled("Moment".into(), row.id.clone(), None).await.unwrap_err();
    let effects_when_refused = mutation_effects(&rig);
    database.session_resume().await.unwrap();
    database.retry("Moment".into(), row.id.clone(), None).await.unwrap();
    let settled = database.settled("Moment".into(), row.id.clone(), None).await;

    assert_eq!((session(refused).0, effects_when_refused), (SessionCode::AccountUnavailable, 0));
    assert_eq!(mutation_effects(&rig), 1);
    assert!(settled.is_ok(), "{settled:?}");
    let states: Vec<OperationStatus> = database.db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect();
    assert_eq!(states, []);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_create_whose_answer_was_lost_is_executed_once_by_resume_and_the_next_flush() {
  for concurrent in [false, true] {
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
    let rig = Rig { server: crate::transport::encrypted::fake_server_tests::FakeServer::new(clock.clone()), credentials: Arc::default(), retries: super::RetryProgress::new() };
    let database = super::Database::start_with(
        super::Config { path: None, data_version: None, cache_rows: 8 },
        serde_json::to_string(&vec![super::sealed_errors_tests::moment_spec()]).unwrap(), None,
        clock.clone(),
        |db, clock| {
            use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
            let config = crate::transport::EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::protocol_tests::config() };
            let client = Arc::new(crate::transport::EncryptedClient::new(&config, rig.server.clone(), Arc::new(crate::transport::encrypted::fake_server_tests::MemoryCredentials::default()), db.clone(), clock.clone()).unwrap());
            let rest = Arc::new(super::transport::ForeignTransport(Arc::new(super::encrypted_tests::SubscriptionsOnly)));
            Ok((Some(Arc::new(crate::transport::EncryptedTransport::new(client.clone(), rest))), Some(client)))
        },
    ).unwrap();
    database.register_operations(super::sealed_errors_tests::OPERATIONS.into()).unwrap();
    database.session_call("social".into(), super::session_guards_tests::SOCIAL_BODY.into()).await.unwrap();
    rig.server.register("mutation MakeMoment { makeMoment { id text } }");
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    rig.server.interrupt_next_answer();
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();

    database.flush().await.unwrap();
    let effects_when_lost = mutation_effects(&rig);
    let competing = Arc::new(Mutex::new(None));
    if concurrent {
        let sync = database.sync.as_ref().unwrap().clone();
        let competing = competing.clone();
        let first = std::sync::atomic::AtomicBool::new(true);
        rig.server.on_request(move || {
            if !first.swap(false, std::sync::atomic::Ordering::SeqCst) { return; }
            clock.advance(Duration::from_secs(10));
            let sync = sync.clone();
            let mut flush = Box::pin(async move { sync.flush().await });
            assert!(flush.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            *competing.lock().unwrap() = Some(tokio::spawn(flush));
        });
    }
    database.session_resume().await.unwrap();
    let competing = competing.lock().unwrap().take();
    if let Some(flush) = competing { flush.await.unwrap().unwrap(); }
    database.flush().await.unwrap();
    let settled = database.settled("Moment".into(), row.id.clone(), None).await;

    assert_eq!(effects_when_lost, 1);
    assert_eq!(mutation_effects(&rig), 1);
    assert!(settled.is_ok(), "{settled:?}");
    assert_eq!(database.db.outbox().unwrap().len(), 0);
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_request_after_a_logout_reopens_the_background() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.interrupt_next_request();
    database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    database.flush().await.unwrap();
    crate::test_wait::within(database.logout()).await.unwrap();
    assert!(!database.streams.snapshot().signed_in());
    database.session_resume().await.unwrap();
    assert!(database.streams.snapshot().signed_in(), "a resumed request with the same credential did not reopen the background");
    database.shutdown();
}
