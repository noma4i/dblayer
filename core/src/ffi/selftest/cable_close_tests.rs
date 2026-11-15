//! D-479: the server closes the cable: 4401 ends the session, 4429 waits for the server pause before the next socket.

use serde_json::json;

use super::rig_events::events;
use super::rig_tests::{Rig, World};
use crate::ffi::SessionStanding;

/// A signed rig whose cable is open for message events.
async fn subscribed() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(|_| {})).await;
    rig.land("Message", json!({"id": "m1", "chatId": "c1", "text": "hi", "at": 1})).await;
    rig
}

/// D-479 (1): close code 4401 ends the session like a `Revoked` answer, and no new socket opens.
#[tokio::test(flavor = "multi_thread")]
async fn cable_close_4401_ends_the_session_without_a_new_socket() {
    let rig = subscribed().await;
    let opened = rig.close_cable(4401, "session_unknown").await;
    crate::test_wait::settled(&rig.database, "the session ended", || rig.database.session_status().is_ok_and(|status| status.phase == SessionStanding::None)).await;
    rig.clock.advance(60_000);
    rig.settle().await;
    assert_eq!(rig.cable_connects(), opened, "no socket opened after 4401");
    rig.database.close_now();
}

/// D-479 (2): after close code 4429 the next socket waits for `retryAfter` of the close reason, or 5 s without it, whatever the backoff says.
#[tokio::test(flavor = "multi_thread")]
async fn cable_close_4429_waits_for_the_server_pause() {
    let mut missed = Vec::new();
    for (reason, pause) in [("hello_blocked", 5_000u64), (r#"{"code":"hello_blocked","retryAfter":20}"#, 20_000)] {
        let rig = subscribed().await;
        let armed = rig.clock.armed();
        let opened = rig.close_cable(4429, reason).await;
        rig.clock.await_armed(armed).await;
        rig.clock.advance(pause - 1);
        rig.settle().await;
        if rig.cable_connects() != opened { missed.push(format!("{reason}: a socket opened {} ms after 4429", pause - 1)); }
        rig.clock.advance(1);
        rig.settle().await;
        if rig.cable_connects() != opened + 1 { missed.push(format!("{reason}: sockets after the pause = {}, wanted {}", rig.cable_connects(), opened + 1)); }
        assert_eq!(rig.database.session_status().unwrap().phase, SessionStanding::Active, "{reason}: 4429 keeps the session");
        rig.database.close_now();
    }
    assert!(missed.is_empty(), "{missed:#?}");
}

/// D-479 (3): a sealed `RATE_LIMITED` with `retryAfter` fails the call at once and holds every request of the session until the pause ends.
#[tokio::test(flavor = "multi_thread")]
async fn sealed_rate_limit_holds_the_session_requests_until_retry_after() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |_| {}).await;
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("Thread", super::rig_tests::Fault::Answer(json!({"error": "RATE_LIMITED", "retryAfter": 30})));
    let refused = super::rig_tests::bound(rig.database.load(epoch, "Message".into(), "thread".into(), json!({"chatId": "c1"}).to_string())).await;
    assert!(matches!(refused, Err(crate::ffi::DbError::Session { code: crate::ffi::SessionCode::RateLimited, .. })), "the refused call ends with RateLimited: {refused:?}");
    let sent = rig.world.hits();
    let waiting = { let database = rig.database.clone(); rig.spawn(async move { database.load(epoch, "Message".into(), "thread".into(), json!({"chatId": "c2"}).to_string()).await }) };
    rig.clock.advance(29_999);
    rig.settle().await;
    assert_eq!(rig.world.hits(), sent, "no request leaves during the pause");
    rig.clock.advance(1);
    super::rig_tests::bound(waiting).await.unwrap().expect("the request after the pause is answered");
    assert_eq!(rig.world.hits(), sent + 1, "the waiting request leaves once after the pause");
    rig.database.close_now();
}

/// A singleton `Viewer` whose `registered` field opens the cable (`cable_when`), read by `ViewerMe`.
fn viewer_gate(models: &mut serde_json::Value) {
    use crate::schema::{FieldSpec, ModelSpec, RelationSpec, RemoteShape};
    let mut viewer = ModelSpec::new("Viewer", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("registered", crate::value::ScalarKind::Bool).nullable().optional()]);
    let mut me = RelationSpec::global("me");
    me.remote = Some(crate::schema_fixtures::remote("ViewerMe", RemoteShape::Single, "5.minutes"));
    viewer.relations.push(me);
    let mut viewer = serde_json::to_value(viewer).unwrap();
    viewer["cable_when"] = json!("registered");
    models.as_array_mut().unwrap().push(viewer);
}

/// D-479 (4): with `cable_when` the cable opens only while the stored singleton field is true; an event reader before that waits without a socket.
#[tokio::test(flavor = "multi_thread")]
async fn cable_opens_only_while_the_declared_singleton_field_is_true() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, events(viewer_gate)).await;
    let mut operations: Vec<serde_json::Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "ViewerMe", "document": "query ViewerMe { viewer { id registered } }"}));
    super::rig_tests::bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    let epoch = rig.database.session_epoch();
    let reader = rig.database.subscribe_values(epoch, crate::ffi::ReadRequest::Events { model: "Message".into() });
    assert!(matches!(reader.claim_reader().await, crate::ffi::ReadClaim::Claimed { .. }), "the event reader is claimed");
    rig.settle().await;
    let mut seen = vec![("no viewer row", rig.cable_connects())];
    for registered in [false, true, false] {
        rig.world.answer_with("ViewerMe", json!({"data": {"viewer": {"__typename": "Viewer", "id": "v1", "registered": registered}}}));
        super::rig_tests::bound(rig.database.refresh(epoch, "Viewer".into(), Some("me".into()), "{}".into())).await.unwrap();
        rig.settle().await;
        seen.push((if registered { "registered" } else { "not registered" }, rig.cable_connects()));
    }
    assert_eq!(seen, vec![("no viewer row", 0), ("not registered", 0), ("registered", 1), ("not registered", 1)], "sockets opened after each stored value");
    rig.database.close_now();
}
