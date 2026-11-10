use super::rig_tests::{bound, Rig};
use crate::test_wait::settled;

fn profile_requests(rig: &Rig) -> usize { rig.world.log().iter().filter(|entry| entry.operation == "MyProfile").count() }

#[tokio::test(flavor = "multi_thread")]
async fn a_load_whose_request_is_at_the_server_waits_for_the_network_and_ends_with_the_new_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let before = profile_requests(&rig);
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("MyProfile");
    let loading = { let database = rig.database.clone(); rig.spawn(async move { database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await }) };
    held.entered().await;
    rig.database.set_online(false).unwrap();
    held.release();
    rig.settle().await;
    assert!(!loading.is_finished(), "the load ended while the network was away");
    assert_eq!(profile_requests(&rig), before + 1, "a request went out while the network was away");
    rig.database.set_online(true).unwrap();
    settled(&rig, "the load goes out again", || profile_requests(&rig) == before + 2).await;
    bound(loading).await.unwrap().unwrap();
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_refresh_failure_keeps_value_without_extra_requests() {
    use std::sync::Arc;
    use super::rig_tests::Fault;
    use crate::ffi::{QuerySpec, ReadRequest};
    use crate::ffi::tape::Rows;

    let rig = Rig::encrypted_edited(false, false, false, |models| {
        let user = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "User").unwrap();
        user["changes"] = serde_json::json!([{"area":"COUNTERS","relation":"profile"},{"area":"GIFT","relation":"profile"}]);
        user["change_feed"] = serde_json::json!({"subscription":"DblayerViewerChanges","root":"viewerChanges.changes","area":"area","kind":"kind","id":"entityId"});
    }).await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let mut operations: Vec<serde_json::Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(serde_json::json!({"name":"DblayerViewerChanges", "document":"subscription DblayerViewerChanges { viewerChanges { changes { area kind entityId } } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    let rows = Arc::new(Rows::default());
    let gate = rig.world.hold_next("MyProfile");
    let reader = rig.database.watch(ReadRequest::List { query: QuerySpec::json(r#"{"model":"User","relation":"profile","params":{}}"#) }, rows.clone());
    gate.entered().await;
    let db = rig.database.clone();
    let first = rig.spawn(async move { db.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await });
    rig.settle().await;
    gate.release();
    bound(first).await.unwrap().unwrap();
    settled(&rig, "the profile is shown", || rows.len() == 1).await;
    assert_eq!(profile_requests(&rig), 1, "first load: {:?}", rig.world.log());
    let initial = rows.0.lock().unwrap()[0].clone();
    rig.world.fail_next("MyProfile", Fault::Down);
    rig.world.fail_next("MyProfile", Fault::Http(400));
    let database = rig.database.clone();
    let mut refresh = rig.spawn(async move { database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await });
    let mut armed = rig.clock.armed();
    let result = loop {
        tokio::select! {
            result = &mut refresh => break result.unwrap(),
            () = rig.clock.await_armed(armed) => { armed = rig.clock.armed(); rig.clock.advance(120_000); }
        }
    };
    assert!(result.is_err(), "the refused refresh succeeded");
    settled(&rig, "the refresh error is shown", || rows.last().is_some_and(|value| value.error.is_some())).await;
    assert_eq!(rows.0.lock().unwrap()[0].id, initial.id);
    let calls: Vec<_> = rig.world.log().into_iter().filter(|entry| entry.operation == "MyProfile").collect();
    assert_eq!(calls.len(), 3, "profile requests: {calls:?}; cable opens: {}", rig.switch.events.peer.connects());
    drop(reader);
    rig.database.close_now();
}
