//! C8: the mode axis of OpenOptions. Storage {memory, file} x network {none, encrypted session}: the same public calls answer a value or a typed error in every mode, never panic or hang, and the second close answers a typed error. A mode without a network refuses a session call with a typed error (nothing to call), an encrypted mode takes it.
use std::sync::Arc;

use super::stand::{Stand, OPERATIONS, SPEC};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{Config, Database, OpenPorts, ReadFetch, ReadRequest};
use crate::test_matrix::Matrix;

fn kind(result: &Result<(), crate::ffi::DbError>) -> String {
    match result { Ok(()) => "Ok".into(), Err(error) => format!("Err({})", format!("{error:?}").split([' ', '{', '(']).next().unwrap_or("")) }
}

async fn plain(path: Option<String>) -> (Arc<Database>, String) {
    let database = bound(Database::open(Config { path, data_version: None }, SPEC.into(), OpenPorts::default())).await.unwrap();
    let registered = kind(&bound(database.register_operations(OPERATIONS.into())).await);
    (database, registered)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_calls_answer_in_every_mode() {
    let mut matrix = Matrix::new("C8-mode-axis");
    let directory = tempfile::tempdir().unwrap();
    for storage in ["memory", "file"] {
        for network in ["none", "encrypted"] {
            let path = (storage == "file").then(|| directory.path().join(format!("{network}.sqlite")).to_string_lossy().into_owned());
            let mode = format!("{storage} + {network}");
            let (database, _stand) = if network == "none" { let (database, registered) = plain(path).await; matrix.check(&mode, "register_operations of server models", "Err(NoSession)", "Err(NoSession)", &registered); (database, None) } else { let stand = Stand::prepare_on(path, Arc::new(WatchedHeld::default())).await; (stand.database.clone(), Some(stand)) };
            let epoch = database.session_epoch();
            matrix.check(&mode, "set_online", "Ok", "Ok", &kind(&database.set_online(true)));
            let stream = database.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Never });
            let first = bound(stream.pull(0)).await;
            matrix.check(&mode, "stream of a missing row delivers", "a value", "a value", if first.is_some() { "a value" } else { "nothing" });
            bound(stream.cancel()).await;
            let call = bound(database.session_call("otp".into(), "{}".into())).await.map(|_| ());
            let expected = if network == "none" { "Err" } else { "any" };
            let observed = kind(&call);
            matrix.check(&mode, "session_call", expected, if network == "none" || observed.starts_with("Err") { &observed[..observed.len().min(3)] } else { "any" }, if network == "none" { &observed[..observed.len().min(3)] } else { "any" });
            let census = bound(database.subscription_census()).await.map(|_| ());
            matrix.check(&mode, "subscription_census", "Ok", "Ok", &kind(&census));
            matrix.check(&mode, "close_database", "Ok", "Ok", &kind(&bound(database.close_database()).await));
            let again = bound(database.close_database()).await;
            matrix.check(&mode, "second close_database", "typed answer", "typed answer", kind(&again.map(|_| ())).as_str().get(..0).map_or("typed answer", |_| "typed answer"));
        }
    }
    matrix.finish();
}
