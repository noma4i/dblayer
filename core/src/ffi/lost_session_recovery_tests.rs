use super::sealed_errors_tests::signed_in;
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::SessionCode;

fn mutation_effects(rig: &Rig) -> usize {
    rig.server.executed().iter().filter(|body| body.get("extensions").is_some()).count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_mutation_refused_without_a_sequence_keeps_the_session_and_a_reset_clears_the_mutation() {
    let rig = Rig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(serde_json::json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create_durable("Moment", r#"{"text":"hello"}"#).await;
    rig.server.refuse_next("UNAUTHENTICATED", false);

    let refused = database.refusal_of(row).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    database.session_reset().await.unwrap();
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    let outbox = database.db.outbox().unwrap().len();

    assert_eq!(refused.session_code, Some(SessionCode::Unauthenticated));
    assert_eq!(outbox, 0, "the session reset left the refused mutation of the ended session in the outbox");
    assert_eq!(mutation_effects(&rig), 0);
}
