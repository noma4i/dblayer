use super::sealed_errors_tests::{session, signed_in};
use super::session_guards_tests::Rig;
use super::SessionCode;
use crate::commit::OperationStatus;

fn mutation_effects(rig: &Rig) -> usize {
    rig.server.executed().iter().filter(|body| body.get("extensions").is_some()).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_create_refused_without_a_sequence_is_executed_once_by_resume_and_retry() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);

    let refused = database.settled("Moment".into(), row.id.clone()).await.unwrap_err();
    let effects_when_refused = mutation_effects(&rig);
    database.session_resume().await.unwrap();
    database.retry("Moment".into(), row.id.clone()).await.unwrap();
    let settled = database.settled("Moment".into(), row.id.clone()).await;

    assert_eq!((session(refused).0, effects_when_refused), (SessionCode::AccountUnavailable, 0));
    assert_eq!(mutation_effects(&rig), 1);
    assert!(settled.is_ok(), "{settled:?}");
    let states: Vec<OperationStatus> = database.db.outbox().unwrap().iter().map(|entry| entry.operation.status).collect();
    assert_eq!(states, []);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_create_whose_answer_was_lost_is_executed_once_by_resume_and_the_next_flush() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    rig.server.lose_next_answer();

    database.flush().await.unwrap();
    let effects_when_lost = mutation_effects(&rig);
    database.session_resume().await.unwrap();
    database.flush().await.unwrap();
    let settled = database.settled("Moment".into(), row.id.clone()).await;

    assert_eq!(effects_when_lost, 1);
    assert_eq!(mutation_effects(&rig), 1);
    assert!(settled.is_ok(), "{settled:?}");
    assert_eq!(database.db.outbox().unwrap().len(), 0);
}
