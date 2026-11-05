use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;

use super::bridge_tests::{group, member_list, message, seed_members, strings};
use super::rig_tests::{bound, Fault, Rig, World};
use crate::test_wait::yield_until;

async fn confirmed_group(rig: &Rig) -> crate::ffi::WireRow {
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    bound(rig.database.read_query(epoch, r#"{"model":"Group"}"#.into())).await.unwrap().into_iter().next().unwrap()
}

async fn remove_member(rig: &Rig, id: &str) {
    let epoch = rig.database.session_epoch();
    let receipt = bound(rig.database.act(epoch, "Group".into(), id.into(), "remove_member".into(), "{}".into(), json!({"userIds":["u4"]}).to_string())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn joining_multiple_members_keeps_each_once_in_order() {
    let world = World::new("server", "A", true);
    let rig = Rig::plain(world.clone(), false).await;
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    let params = json!({"groupId": group.id, "order": "JOINED"});
    world.set_members("JOINED", &["u1", "u2", "u3", "u4", "u5"]);
    bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string())).await.unwrap();
    let removed = bound(rig.database.act(epoch, "Group".into(), group.id.clone(), "remove_member".into(), "{}".into(), json!({"userIds":["u4","u5"]}).to_string())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(removed)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    let hold = world.hold_next("AddMember");
    let receipt = bound(rig.database.act(epoch, "Group".into(), group.id.clone(), "add_member".into(), "{}".into(), json!({"userIds":["u4","u5","u4"]}).to_string())).await.unwrap();
    hold.entered_for("the join").await;
    let query = json!({"model":"Member","relation":"group_members","params":params});
    let ids: Vec<_> = bound(rig.database.read_query(epoch, query.to_string())).await.unwrap().into_iter().map(|row| row.id).collect();
    assert_eq!(ids, ["u1", "u2", "u3", "u4", "u5"]);
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operation_outcome_can_be_read_twice_from_its_receipt() {
    let world = World::new("server", "A", true);
    let rig = Rig::plain(world.clone(), false).await;
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    let hold = world.hold_next("AddMember");
    let receipt = bound(rig.database.act(epoch, "Group".into(), group.id.clone(), "add_member".into(), "{}".into(), json!({"userIds":["u9"]}).to_string())).await.unwrap();
    hold.entered_for("the add").await;
    hold.release();
    let first = bound(rig.database.outcome(receipt.clone())).await.unwrap();
    let leaving = bound(rig.database.act(epoch, "Group".into(), group.id, "leave".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(leaving)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    let second = bound(rig.database.outcome(receipt)).await.unwrap();
    assert!(matches!(first, crate::ffi::WireOperationOutcome::Confirmed { value: Some(_), .. }));
    assert_eq!(first, second);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_destroy_restores_the_row_according_to_its_declaration() {
    for fault in [Fault::Http(422), Fault::Business] {
        let world = World::new("server", "A", true);
        let rig = Rig::plain_with_group_destroy(world.clone(), false, false, false, true).await;
        let epoch = rig.database.session_epoch();
        let group = confirmed_group(&rig).await;
        let hold = world.hold_then("DeleteGroup", fault);
        let receipt = bound(rig.database.destroy(epoch, "Group".into(), group.id.clone())).await.unwrap();
        hold.entered_for("destroy").await;
        assert!(bound(rig.database.read_row(epoch, "Group".into(), group.id.clone())).await.unwrap().is_none());
        hold.release();
        assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Rejected { .. }));
        assert!(bound(rig.database.read_row(epoch, "Group".into(), group.id)).await.unwrap().is_some());
        rig.database.close_now();
    }
}

struct Deliveries(tokio::sync::mpsc::UnboundedSender<crate::ffi::tape::StreamDelivery>);

impl crate::ffi::tape::Reader for Deliveries {
    fn delivered(&self, delivery: crate::ffi::tape::StreamDelivery) { let _ = self.0.send(delivery); }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_event_keeps_the_open_join_in_the_list() {
    let world = World::new("server", "A", true);
    let rig = Rig::plain_with_member_events(world.clone(), false, false, true).await;
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    world.set_members("JOINED", &["u1", "u2", "u3", "u4"]);
    let params = json!({"groupId": group.id, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string())).await.unwrap();
    remove_member(&rig, &group.id).await;
    world.set_members("JOINED", &["u1", "u2", "u3"]);
    let query = json!({"model":"Member","relation":"group_members","params":params});
    let (sender, mut deliveries) = tokio::sync::mpsc::unbounded_channel();
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query_json: query.to_string() }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.switch.subscription_opened().await;
    let hold = world.hold_next("AddMember");
    let adding = { let database = rig.database.clone(); let id = group.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "add_member".into(), "{}".into(), json!({"userIds":["u4"]}).to_string()).await }) };
    hold.entered_for("the join").await;
    rig.switch.emit(json!({"data":{"memberChanged":{"id":"u1","name":"updated"}}}).to_string()).unwrap();
    let applied = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while let Some(delivery) = crate::test_wait::within_secs(20, deliveries.recv()).await {
            let rows = match delivery.value {
                crate::ffi::ReadValue::Snapshot { rows, .. } | crate::ffi::ReadValue::Delta { rows, .. } => rows,
                _ => continue,
            };
            if rows.iter().any(|row| row.id == "u1" && row.cells.contains(&crate::ffi::Cell::Text { value: "updated".into() })) { return true; }
        }
        false
    }).await.unwrap_or(false);
    assert!(applied, "the member event was not delivered");
    let ids: Vec<_> = bound(rig.database.read_query(epoch, query.to_string())).await.unwrap().into_iter().map(|row| row.id).collect();
    assert!(ids.contains(&"u4".to_string()));
    hold.release();
    let receipt = bound(adding).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_outcome_arrives_before_a_held_list_and_the_automatic_reread_shows_the_added_member() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    rig.world.set_members("JOINED", &["u1", "u2", "u3"]);
    let params = json!({"groupId": group.id, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string())).await.unwrap();
    let query = json!({"model": "Member", "relation": "group_members", "params": params});
    let (sender, mut deliveries) = tokio::sync::mpsc::unbounded_channel();
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query_json: query.to_string() }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    let old = rig.world.hold_next("GroupMembers");
    let refreshing = { let database = rig.database.clone(); let params = params.clone(); tokio::spawn(async move { database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string()).await }) };
    old.entered_for("the old list").await;
    rig.world.set_members("JOINED", &["u1", "u2", "u3", "u9"]);
    let mutation = rig.world.hold_next("AddMember");
    let acting = { let database = rig.database.clone(); let id = group.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "add_member".into(), "{}".into(), json!({"userIds":["u9"]}).to_string()).await }) };
    mutation.entered_for("the add").await;
    let receipt = bound(acting).await.unwrap().unwrap();
    mutation.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    old.release();
    bound(refreshing).await.unwrap().unwrap();
    let shown = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while let Some(delivery) = crate::test_wait::within_secs(20, deliveries.recv()).await {
            let rows = match delivery.value {
                crate::ffi::ReadValue::Snapshot { rows, .. } | crate::ffi::ReadValue::Delta { rows, .. } => rows,
                _ => continue,
            };
            if rows.iter().any(|row| row.id == "u9") { return true; }
        }
        false
    }).await.unwrap_or(false);
    assert!(shown, "the automatic reread did not show u9");
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn react_then_undo_keeps_the_other_users_reaction() {
  for plain in [true, false] {
    let rig = if plain { Rig::plain_for_reactions(World::new("server", "A", true)).await } else { Rig::encrypted_for_reactions(false, false, true).await };
    rig.world.seed_reacted_message();
    if !plain { rig.login_ios("A").await; }
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "Message".into(), "thread".into(), json!({"chatId":"c1"}).to_string())).await.unwrap();
    let initial = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    let other = crate::ffi::Cell::Seq { items: vec![crate::ffi::Cell::Seq { items: vec![crate::ffi::Cell::Text { value: "B".into() }, crate::ffi::Cell::Text { value: "u2".into() }] }] };
    assert_eq!(initial.cells[3], other);
    let answer = |reactions: serde_json::Value| json!({"data":{"messageReact":{"__typename":"Message","id":"m1","chatId":"c1","text":"hello","at":1,"reactions":reactions}}});
    let first = rig.world.hold_then("MessageReact", Fault::Answer(answer(json!([{"body":"B","userId":"u2"},{"body":"A","userId":"u1"}]))));
    let second = rig.world.hold_then("MessageReact", Fault::Answer(answer(json!([{"body":"B","userId":"u2"}]))));
    let patch = json!({"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"body"},"userId":{"$arg":"userId"}}}}}).to_string();
    let first_receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "react".into(), patch.clone(), json!({"body":"A","userId":"u1"}).to_string())).await.unwrap();
    first.entered_for("react").await;
    let second_receipt = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "react".into(), patch, json!({"body":"","userId":"u1"}).to_string())).await.unwrap();
    let optimistic = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    assert_eq!(optimistic.cells[3], other);
    first.release();
    assert!(matches!(bound(rig.database.outcome(first_receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    second.entered_for("undo").await;
    second.release();
    assert!(matches!(bound(rig.database.outcome(second_receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    let row = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    assert_eq!(row.cells[3], other);
    rig.database.close_now();
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_durable_send_keeps_its_row_when_declared() {
  for retry in [false, true] {
    let rig = Rig::encrypted_with_options(false, true).await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let hold = rig.world.hold_then("SendMessage", Fault::Http(422));
    let receipt = bound(rig.database.send(message(epoch, "held", 2))).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    hold.entered_for("the durable send").await;
    assert!(bound(rig.database.read_row(epoch, "Message".into(), id.clone())).await.unwrap().is_some());
    hold.release();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Rejected { .. }));
    assert!(bound(rig.database.read_row(epoch, "Message".into(), id.clone())).await.unwrap().is_some());
    if retry {
        let retry_receipt = bound(rig.database.retry(epoch, "Message".into(), id.clone(), None, None)).await.unwrap();
        assert!(matches!(bound(rig.database.outcome(retry_receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    } else {
        bound(rig.database.discard(epoch, "Message".into(), id, None)).await.unwrap();
    }
    let rows = bound(rig.database.read_query(epoch, r#"{"model":"Message"}"#.into())).await.unwrap();
    assert_eq!(rows.len(), usize::from(retry));
    rig.database.close_now();
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_later_confirmed_membership_change_survives_an_earlier_refusal() {
  for (plain, first_join) in [(true, false), (true, true), (false, false), (false, true)] {
    let rig = if plain { Rig::plain(World::new("server", "A", true), false).await } else { let rig = Rig::encrypted().await; rig.login_ios("A").await; rig };
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    rig.world.set_members("JOINED", &["u1", "u2", "u3", "u4"]);
    let params = json!({"groupId": group.id, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string())).await.unwrap();
    if first_join { remove_member(&rig, &group.id).await; }
    if first_join { rig.world.set_members("JOINED", &["u1", "u2", "u3"]); }
    let old = rig.world.hold_next("GroupMembers");
    let reading = { let database = rig.database.clone(); let params = params.clone(); tokio::spawn(async move { database.refresh(epoch, "Member".into(), "group_members".into(), params.to_string()).await }) };
    old.entered_for("the old member list").await;
    let first = rig.world.hold_then(if first_join { "AddMember" } else { "RemoveMember" }, Fault::Refuse { code: "FORBIDDEN", confirmed: true });
    let second = rig.world.hold_next(if first_join { "RemoveMember" } else { "AddMember" });
    let start = |action: &'static str| { let database = rig.database.clone(); let id = group.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": ["u4"]}).to_string()).await }) };
    let (earlier, later) = if first_join { ("add_member", "remove_member") } else { ("remove_member", "add_member") };
    let joining = start(earlier);
    first.entered_for("the first change").await;
    let joining = bound(joining).await.unwrap().unwrap();
    first.release();
    assert!(matches!(bound(rig.database.outcome(joining)).await.unwrap(), crate::ffi::WireOperationOutcome::Rejected { .. }));
    let leaving = start(later);
    second.entered_for("the second change").await;
    second.release();
    let leaving = bound(leaving).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.outcome(leaving)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    old.release();
    bound(reading).await.unwrap().unwrap();
    let query = json!({"model": "Member", "relation": "group_members", "params": params}).to_string();
    let ids: Vec<_> = bound(rig.database.read_query(epoch, query)).await.unwrap().into_iter().map(|row| row.id).collect();
    assert_eq!(ids.contains(&"u4".to_string()), !first_join);
    rig.database.close_now();
  }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_membership_change_restores_both_public_lists() {
    for removing in [false, true] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let group = confirmed_group(&rig).await;
        let orders = [("JOINED", ["u1", "u2", "u3", "u4"]), ("OWNER_FIRST", ["u2", "u1", "u3", "u4"])];
        for (order, members) in orders {
            rig.world.set_members(order, &members);
            let params = json!({"groupId": group.id, "order": order}).to_string();
            bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params)).await.unwrap();
        }
        if !removing {
            remove_member(&rig, &group.id).await;
        }
        let action = if removing { "remove_member" } else { "add_member" };
        let gate = rig.world.hold_then(if removing { "RemoveMember" } else { "AddMember" }, Fault::Refuse { code: "FORBIDDEN", confirmed: true });
        let acting = { let database = rig.database.clone(); let id = group.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": ["u4"]}).to_string()).await }) };
        gate.entered_for(action).await;
        for (order, _) in orders {
            let query = json!({"model": "Member", "relation": "group_members", "params": {"groupId": group.id, "order": order}}).to_string();
            let ids: Vec<_> = bound(rig.database.read_query(epoch, query)).await.unwrap().into_iter().map(|row| row.id).collect();
            assert_eq!(ids.contains(&"u4".to_string()), !removing, "{action}: before refusal in {order}");
        }
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Rejected { .. }));
        for (order, original) in orders {
            let query = json!({"model": "Member", "relation": "group_members", "params": {"groupId": group.id, "order": order}}).to_string();
            let ids: Vec<_> = bound(rig.database.read_query(epoch, query)).await.unwrap().into_iter().map(|row| row.id).collect();
            let expected: Vec<_> = original.iter().filter(|id| removing || **id != "u4").map(|id| id.to_string()).collect();
            assert_eq!(ids, expected, "{action}: after refusal in {order}");
        }
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn forward_returns_its_optimistic_value_before_the_server_answers() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "hello", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "Message".into(), "thread".into(), json!({"chatId": "c1"}).to_string())).await.unwrap();
    let source = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    let hold = rig.world.hold_next("MessageForward");
    let mut forward = { let database = rig.database.clone(); tokio::spawn(async move { database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c2"}).to_string()).await }) };
    hold.entered_for("forward").await;
    let returned = tokio::time::timeout(std::time::Duration::from_secs(5), &mut forward).await;
    let result = match returned {
        Ok(result) => result.unwrap().unwrap(),
        Err(_) => { hold.release(); let _ = bound(forward).await; panic!("forward did not return before the server answer") }
    };
    let optimistic = result.optimistic_value.as_ref().unwrap();
    assert_ne!(optimistic.id, "forwarded-1");
    let written = bound(rig.database.read_row(epoch, "Message".into(), optimistic.id.clone())).await.unwrap().unwrap();
    assert_eq!(result.revision, written.revision);
    let after = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    assert_eq!(source.cells, after.cells);
    hold.release();
    let outcome = bound(rig.database.outcome(result)).await.unwrap();
    match outcome {
        crate::ffi::WireOperationOutcome::Confirmed { value: Some(row), .. } => assert_eq!(row.id, "forwarded-1"),
        other => panic!("forward did not return its confirmed row: {other:?}"),
    }
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_action_is_not_sent_after_reconnect_but_a_message_is_sent_once() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let group = confirmed_group(&rig).await;
    rig.database.set_online(false).unwrap();
    let refused = bound(rig.database.act(epoch, "Group".into(), group.id, "add_member".into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await;
    assert!(refused.is_err());
    let receipt = bound(rig.database.send(message(epoch, "offline", 2))).await.unwrap();
    assert!(receipt.optimistic_value.is_some());
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.set_online(true).unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.world.count("SendMessage"), 1);
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "AddMember").count(), 0);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_create_refusal_follows_the_declared_policy() {
    for (fault, keep) in [(Fault::Http(422), false), (Fault::Business, false), (Fault::Http(422), true), (Fault::Business, true)] {
        let world = World::new("server", "A", true);
        let rig = Rig::plain_with_create_policy(world.clone(), false, keep).await;
        world.fail_next("CreateGroup", fault);
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
        assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Rejected { .. }));
        assert_eq!(world.log().iter().filter(|entry| entry.operation == "CreateGroup").count(), 1);
        let rows = bound(rig.database.read_query(epoch, r#"{"model":"Group"}"#.into())).await.unwrap();
        assert_eq!(rows.len(), usize::from(keep), "the retained rows disagree with the declared policy");
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_acknowledged_membership_change_survives_an_older_list_answer() {
    for (removing, answer_first, page) in [(false, false, false), (false, true, false), (true, false, false), (true, true, false), (false, false, true), (false, true, true), (true, false, true), (true, true, true)] {
        let rig = Rig::encrypted_with_member_page(page).await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let created = confirmed_group(&rig).await;
        rig.world.set_members("JOINED", &["u1", "u2", "u3", "u4"]);
        let params = json!({"groupId": created.id, "order": "JOINED"}).to_string();
        let query = json!({"model": "Member", "relation": "group_members", "params": {"groupId": created.id, "order": "JOINED"}}).to_string();
        bound(rig.database.refresh(epoch, "Member".into(), "group_members".into(), params.clone())).await.unwrap();
        if !removing {
            remove_member(&rig, &created.id).await;
            rig.world.set_members("JOINED", &["u1", "u2", "u3"]);
        }
        let held = rig.world.hold_next("GroupMembers");
        let reading = { let database = rig.database.clone(); let params = params.clone(); tokio::spawn(async move { database.refresh(epoch, "Member".into(), "group_members".into(), params).await }) };
        held.entered_for("the old member list").await;
        let action = if removing { "remove_member" } else { "add_member" };
        let mutation = rig.world.hold_next(if removing { "RemoveMember" } else { "AddMember" });
        let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": ["u4"]}).to_string()).await }) };
        mutation.entered_for("the membership mutation").await;
        let receipt = bound(acting).await.unwrap().unwrap();
        if answer_first { mutation.release(); }
        if answer_first { assert!(matches!(bound(rig.database.outcome(receipt.clone())).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. })); }
        held.release();
        bound(reading).await.unwrap().unwrap();
        let ids: Vec<_> = bound(rig.database.read_query(epoch, query)).await.unwrap().into_iter().map(|row| row.id).collect();
        assert_eq!(ids.contains(&"u4".to_string()), !removing, "{action}: an older read changed confirmed membership");
        if !answer_first { mutation.release(); assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. })); }
        rig.database.close_now();
    }
}

/// A member whose row is not cached is skipped by the optimistic join: the call reaches the server and ends with the answer, never with a missing-row error.
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_member_whose_row_is_not_cached_ends_with_the_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let created = confirmed_group(&rig).await;
    seed_members(&rig, &created.id);
    let result = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u4", "u9"]}).to_string())).await;
    assert!(result.is_ok(), "the call failed: {:?}", result.as_ref().err());
    rig.settle().await;
    assert_eq!(rig.world.count("AddMember"), 1, "the add did not reach the server");
    assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (strings(&["u1", "u2", "u3", "u4"]), strings(&["u2", "u1", "u3", "u4"])), "the cached row joined, the missing one did not");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_read_between_its_confirming_commit_and_the_announcement_is_delivered() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    let shared = rig.database.db.shared.clone();
    let (in_window, read_done) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let (probe_window, probe_done) = (in_window.clone(), read_done.clone());
    *shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.replaced.is_empty() { return; }
        probe_window.store(true, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !probe_done.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline, "the read of the command never came");
            std::thread::yield_now();
        }
    }));
    rig.database.set_online(true).unwrap();
    yield_until("the confirming commit is visible and not announced", || in_window.load(Ordering::SeqCst)).await;
    let reading = {
        let (database, receipt, done) = (rig.database.clone(), receipt.clone(), read_done.clone());
        tokio::spawn(async move {
            done.store(true, Ordering::SeqCst);
            database.outcome(receipt).await
        })
    };
    assert!(matches!(bound(reading).await.unwrap().unwrap(), crate::ffi::WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "m1"));
    *shared.store.after_commit.lock().unwrap() = None;
    rig.database.close_now();
}
