//! D-214 / D-204: the code of a refusal and the consumption of the number are decided by one classifier, whatever channel carries the code.
use serde_json::{json, Value};

use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::DbError;
use crate::transport::SessionCode;

fn in_errors(code: &str) -> Value { json!({"errors": [{"message": code, "extensions": {"code": code}}]}) }

async fn call(rig: &Rig) -> Result<String, DbError> {
    let epoch = rig.database.session_epoch();
    bound(rig.database.run(epoch, "UnregisterDevice".into(), "{}".into())).await
}

fn last_sequence(rig: &Rig) -> Option<i64> { rig.world.log().last().and_then(|entry| entry.sequence) }

/// `RATE_LIMITED` is a session refusal and an unknown name is a GraphQL error carrying the name, in `error` and in `errors[].extensions.code`; the number is spent only by a confirmed answer; a temporary code with a confirmed number is not sent again.
#[tokio::test(flavor = "multi_thread")]
async fn wire_session_code_and_consumption() {
    type Check = fn(&DbError) -> bool;
    let session_rate: Check = |error| matches!(error, DbError::Session { code: SessionCode::RateLimited, .. });
    let graphql_unknown: Check = |error| matches!(error, DbError::Graphql { extensions: Some(extensions), .. } if extensions.code.as_deref() == Some("SOME_NEW_CODE"));
    let cases: Vec<(&str, Fault, bool, Check)> = vec![
        ("RATE_LIMITED in error, number not spent", Fault::Refuse { code: "RATE_LIMITED", confirmed: false }, false, session_rate),
        ("RATE_LIMITED in error, number confirmed", Fault::Refuse { code: "RATE_LIMITED", confirmed: true }, true, session_rate),
        ("RATE_LIMITED in errors", Fault::Answer(in_errors("RATE_LIMITED")), true, session_rate),
        ("unknown name in error, number not spent", Fault::Refuse { code: "SOME_NEW_CODE", confirmed: false }, false, graphql_unknown),
        ("unknown name in error, number confirmed", Fault::Refuse { code: "SOME_NEW_CODE", confirmed: true }, true, graphql_unknown),
        ("unknown name in errors", Fault::Answer(in_errors("SOME_NEW_CODE")), true, graphql_unknown),
    ];
    for (name, fault, spent, check) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let hits = rig.world.log().len();
        rig.world.fail_next("UnregisterDevice", fault);
        let refused = call(&rig).await.unwrap_err();
        assert!(check(&refused), "{name}: {refused:?}");
        let first = last_sequence(&rig).expect("the call carried a number");
        assert_eq!(rig.world.log().len(), hits + 1, "{name}: sent once");
        let _ = call(&rig).await;
        let second = last_sequence(&rig).expect("the next call carried a number");
        assert_eq!(second, if spent { first + 1 } else { first }, "{name}: the number of the next pair");
        rig.database.close_now();
    }
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let hits = rig.world.log().len();
    rig.world.fail_next("UnregisterDevice", Fault::Answer(in_errors("INTERNAL_SERVER_ERROR")));
    let _ = call(&rig).await;
    assert_eq!(rig.world.log().len(), hits + 1, "a temporary code with a confirmed number is not sent again");
    let first = last_sequence(&rig).unwrap();
    let _ = call(&rig).await;
    assert_eq!(last_sequence(&rig), Some(first + 1), "the next pair takes the next number");
    rig.database.close_now();
}
