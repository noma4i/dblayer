//! Matrix A: a request path against a kind of answer of `wire.md` «Вид ответа». The cell runs one call through the public API on the server double with one scripted fault, drives the clock through the waits of the retries, and compares the public outcome and the number of sends with the table of the spec. Paths without a way to script the fault in this rig are listed as unclassified.
use serde_json::{json, Value};

use super::bridge_support_tests::message;
use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::{DbError, WireOperationOutcome};
use crate::test_matrix::Matrix;

#[derive(Clone, Copy, PartialEq)]
enum Path {
    CallMutation,
    CallQuery,
    OutboxSend,
}

impl Path {
    fn name(self) -> &'static str { match self { Self::CallMutation => "call mutation", Self::CallQuery => "call query", Self::OutboxSend => "outbox send" } }
    fn operation(self) -> &'static str { match self { Self::CallMutation => "UnregisterDevice", Self::CallQuery => "MyProfile", Self::OutboxSend => "SendMessage" } }
    fn sequenced(self) -> bool { self != Self::CallQuery }
}

fn in_errors(code: &str) -> Value { json!({"errors": [{"message": code, "extensions": {"code": code}}]}) }

/// A kind: the fault the double plays once, and the public result and the sends the spec asks for on a sequenced and on a plain path (`None` = the spec has no row for it on that path).
struct Kind {
    name: &'static str,
    fault: fn() -> Fault,
    sequenced: Option<&'static str>,
    plain: Option<&'static str>,
}

fn kinds() -> Vec<Kind> {
    vec![
        Kind { name: "transport failure without an answer", fault: || Fault::Down, sequenced: Some("Ok|2"), plain: Some("Ok|2") },
        Kind { name: "UNAUTHENTICATED", fault: || Fault::Refuse { code: "UNAUTHENTICATED", confirmed: false }, sequenced: Some("Err:Unauthorized|1"), plain: Some("Err:Unauthorized|1") },
        Kind { name: "HTTP 401", fault: || Fault::Http(401), sequenced: Some("Err:Unauthorized|1"), plain: Some("Err:Unauthorized|1") },
        Kind { name: "REPLAYED_REQUEST", fault: || Fault::Refuse { code: "REPLAYED_REQUEST", confirmed: true }, sequenced: Some("Ok|1"), plain: Some("Ok|2") },
        Kind { name: "sequence and a session code", fault: || Fault::Refuse { code: "RATE_LIMITED", confirmed: true }, sequenced: Some("Err:Session(RateLimited)|1"), plain: None },
        Kind { name: "sequence and a retryable errors code", fault: || Fault::Answer(in_errors("INTERNAL_SERVER_ERROR")), sequenced: Some("Err:Graphql(INTERNAL_SERVER_ERROR)|1"), plain: Some("Ok|2") },
        Kind { name: "other errors code", fault: || Fault::Answer(in_errors("FORBIDDEN")), sequenced: Some("Err:Graphql(FORBIDDEN)|1"), plain: Some("Err:Graphql(FORBIDDEN)|1") },
        Kind { name: "unknown code in errors", fault: || Fault::Answer(in_errors("SOME_NEW_CODE")), sequenced: Some("Err:Graphql(SOME_NEW_CODE)|1"), plain: Some("Err:Graphql(SOME_NEW_CODE)|1") },
        Kind { name: "unknown code in error", fault: || Fault::Refuse { code: "SOME_NEW_CODE", confirmed: true }, sequenced: Some("Err:Graphql(SOME_NEW_CODE)|1"), plain: Some("Err:Graphql(SOME_NEW_CODE)|1") },
        Kind { name: "code without a sequence", fault: || Fault::Refuse { code: "RATE_LIMITED", confirmed: false }, sequenced: Some("Err:Session(RateLimited)|1"), plain: None },
        Kind { name: "HTTP 503", fault: || Fault::Http(503), sequenced: Some("Ok|2"), plain: Some("Ok|2") },
        Kind { name: "HTTP 400", fault: || Fault::Http(400), sequenced: Some("Err:Http(400)|1"), plain: Some("Err:Http(400)|1") },
    ]
}

fn tag(error: &DbError) -> String {
    match error {
        DbError::Session { code, .. } => format!("Session({code:?})"),
        DbError::Graphql { extensions, .. } => format!("Graphql({})", extensions.as_ref().and_then(|extensions| extensions.code.clone()).unwrap_or_default()),
        DbError::Http { status, .. } => format!("Http({status})"),
        other => format!("{other:?}").split(|character: char| !character.is_alphanumeric()).next().unwrap_or("").to_string(),
    }
}

async fn drive(path: Path, fault: Fault) -> String {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let hits = rig.world.log().iter().filter(|entry| entry.operation == path.operation()).count();
    rig.world.fail_next(path.operation(), fault);
    let database = rig.database.clone();
    let mut call = match path {
        Path::CallMutation | Path::CallQuery => rig.spawn(async move { match database.run(epoch, path.operation().into(), "{}".into()).await { Ok(_) => "Ok".to_string(), Err(error) => format!("Err:{}", tag(&error)) } }),
        Path::OutboxSend => rig.spawn(async move {
            let receipt = match database.send(message(epoch, "m", 1)).await { Ok(receipt) => receipt, Err(error) => return format!("Err:{}", tag(&error)) };
            match database.outcome(receipt).await { Ok(WireOperationOutcome::Confirmed { .. }) => "Ok".to_string(), Ok(WireOperationOutcome::Rejected { .. }) => "Err:Rejected".to_string(), Ok(WireOperationOutcome::Interrupted) => "Err:Interrupted".to_string(), Err(error) => format!("Err:{}", tag(&error)) }
        }),
    };
    let mut armed = rig.clock.armed();
    let result = loop {
        tokio::select! {
            result = &mut call => break result.unwrap_or_else(|error| format!("task failed: {error}")),
            () = rig.clock.await_armed(armed) => { armed = rig.clock.armed(); rig.clock.advance(120_000); }
        }
    };
    let sent = rig.world.log().iter().filter(|entry| entry.operation == path.operation()).count() - hits;
    rig.database.close_now();
    format!("{result}|{sent}")
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_answer_path_kind_call_mutation() { cells(Path::CallMutation, "A-answer-call-mutation").await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_answer_path_kind_call_query() { cells(Path::CallQuery, "A-answer-call-query").await; }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_answer_path_kind_outbox_send() { cells(Path::OutboxSend, "A-answer-outbox-send").await; }

async fn cells(path: Path, name: &'static str) {
    let mut matrix = Matrix::new(name);
    for kind in kinds() {
        let expected = if path.sequenced() { kind.sequenced } else { kind.plain };
        match expected {
            Some(expected) => {
                let wanted = if path == Path::OutboxSend { wanted_outbox(expected) } else { expected.to_string() };
                let observed = bound(drive(path, (kind.fault)())).await;
                let observed = if path == Path::OutboxSend { wanted_outbox(&observed) } else { observed };
                matrix.check(path.name(), kind.name, if path.sequenced() { "sequenced" } else { "plain" }, &wanted, &observed);
            }
            None => matrix.check(path.name(), kind.name, if path.sequenced() { "sequenced" } else { "plain" }, "N/A{the spec has no row for this input on this path}", "N/A{the spec has no row for this input on this path}"),
        }
    }
    matrix.unclassified(path.name(), "PERSISTED_QUERY_NOT_FOUND, unreadable 2xx body, revoked, SEQUENCE_CONFLICT", "-", "the rig double cannot script these kinds on a call; they need the wire recorder double");
    matrix.finish();
}

/// An Outbox operation reports `Confirmed` or `Rejected`: the class of the error is the failure of the operation, so only Ok or Err and the sends are compared.
fn wanted_outbox(cell: &str) -> String {
    let (result, sends) = cell.split_once('|').unwrap_or((cell, ""));
    format!("{}|{sends}", if result == "Ok" { "Ok" } else { "Err" })
}
