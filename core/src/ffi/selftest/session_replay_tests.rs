use serde_json::json;

use super::crash_image_support::advancing_by;
use super::rig_tests::{Fault, Rig};

/// D-213 / P-SESS-23: a login whose answer is lost is re-sent as the same pair; both attempts carry one sequence number and the sign-in still lands.
#[tokio::test(flavor = "multi_thread")]
async fn sess_replayed_login_sequence() {
    let rig = Rig::encrypted().await;
    rig.world.fail_next("login_link", Fault::Down);
    let database = rig.database.clone();
    let call = rig.spawn(async move { database.session_call("login_link".into(), json!({"account": "A"}).to_string()).await });
    let result = advancing_by(&rig.clock, 5_000, call).await.unwrap();
    result.unwrap();
    let sequences: Vec<_> = rig.world.log().iter().filter(|entry| entry.operation == "login_link").map(|entry| entry.sequence).collect();
    assert_eq!(sequences, vec![Some(1), Some(1)], "the login pair was re-sent once and both attempts carry sequence 1");
    rig.database.close_now();
}
