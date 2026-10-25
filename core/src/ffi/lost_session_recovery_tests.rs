use super::sealed_errors_tests::{session, signed_in};
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::{DbError, SessionCode};

fn mutation_effects(rig: &Rig) -> usize {
    rig.server.executed().iter().filter(|body| body.get("extensions").is_some()).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_mutation_refused_without_a_sequence_needs_session_reset_then_sign_in_then_retry_and_runs_once() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create("Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap();
    rig.server.refuse_next("UNAUTHENTICATED", false);

    let refused = database.settled("Moment".into(), row.id.clone(), None).await.unwrap_err();
    let blocked = database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap_err();
    database.session_reset().unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    database.retry("Moment".into(), row.id.clone(), None).await.unwrap();
    let settled = database.settled("Moment".into(), row.id.clone(), None).await;

    assert_eq!(session(refused).0, SessionCode::Unauthenticated);
    assert!(matches!(blocked, DbError::Network { .. }), "{blocked:?}");
    assert!(settled.is_ok(), "{settled:?}");
    assert_eq!(mutation_effects(&rig), 1);
}
