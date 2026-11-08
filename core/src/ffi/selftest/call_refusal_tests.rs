use serde_json::json;
use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::{Database, DbError};

async fn run_unregister(rig: &Rig) -> Result<(), DbError> {
    let epoch = rig.database.session_epoch();
    bound(rig.database.run(epoch, "UnregisterDevice".into(), "{}".into())).await.map(|_| ())
}

async fn ends(database: &Database) { bound(crate::ffi::settled(database)).await.unwrap(); }

/// D-153: a mutation call whose request fails at the transport answers `Network`.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_call_with_a_transport_failure_answers_network_and_is_sent_once() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let hits = rig.world.log().len();
    rig.world.fail_next("UnregisterDevice", Fault::Down);
    let refused = run_unregister(&rig).await.unwrap_err();
    assert!(matches!(&refused, DbError::Network { .. }), "{refused:?}");
    ends(&rig.database).await;
    assert_eq!(rig.world.log().len(), hits + 1);
    rig.database.close_now();
}

/// D-153: a mutation call whose answer carries a temporary GraphQL code answers that error and is sent once.
#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_call_refused_with_a_temporary_code_answers_the_graphql_error_and_is_sent_once() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let hits = rig.world.log().len();
    rig.world.fail_next("UnregisterDevice", Fault::Answer(json!({"errors": [{"message": "Busy", "extensions": {"code": "SERVICE_UNAVAILABLE"}}]})));
    let DbError::Graphql { messages, extensions } = run_unregister(&rig).await.unwrap_err() else { panic!("expected a Graphql error") };
    assert_eq!(messages, vec!["Busy".to_string()]);
    assert_eq!(extensions.and_then(|extensions| extensions.code), Some("SERVICE_UNAVAILABLE".to_string()));
    ends(&rig.database).await;
    assert_eq!(rig.world.log().len(), hits + 1);
    rig.database.close_now();
}
