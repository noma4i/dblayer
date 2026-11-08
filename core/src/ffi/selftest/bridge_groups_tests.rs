use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use serde_json::json;
use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::tests::lifecycle_tests::named;
use crate::ffi::{Database, DbError, ReadRequest, WireOperationOutcome};
use crate::test_wait::yield_until;
use super::bridge_support_tests::*;

fn groups() -> ReadRequest { ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Group","relation":"all","params":{}}"#) } }

/// K32: a member added or removed ends with the member lists of the server in both orders: the added user appears in the lists the screen reads and the removed one leaves them, without a read by the app.
#[tokio::test(flavor = "multi_thread")]
async fn adding_and_removing_a_member_ends_with_the_member_lists_of_the_server_in_both_orders() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    let orders = [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])];
    for (order, ids) in orders { rig.world.set_members(order, &ids); }
    let mut readers = Vec::new();
    for (order, _) in orders {
        let (reader, query) = (Reader::open(), json!({"model": "Member", "relation": "group_members", "params": {"groupId": created.id, "order": order}}).to_string());
        let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        readers.push(reader);
    }
    let shown = |readers: &[Arc<Reader>]| readers.iter().map(|reader| reader.rows().into_iter().map(|row| row.id).collect::<Vec<_>>()).collect::<Vec<_>>();
    assert_eq!(shown(&readers), [strings(&["u1", "u2", "u3"]), strings(&["u2", "u1", "u3"])], "the lists before the add");
    type Case = (&'static str, [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [("add_member", [&["u1", "u2", "u3", "u9"], &["u2", "u1", "u3", "u9"]]), ("remove_member", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]])];
    for (action, ended) in cases {
        for (index, (order, _)) in orders.iter().enumerate() { rig.world.set_members(order, ended[index]); }
        let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), action.into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await;
        assert!(acted.is_ok(), "{action}: the call failed: {:?}", acted.as_ref().err());
        yield_until(&format!("{action}: both lists end with the lists of the server"), || shown(&readers) == [strings(ended[0]), strings(ended[1])]).await;
    }
    rig.database.close_now();
}

/// K13 (B6, C05; old lines: no rule for these actions): add member and remove member change both member lists (JOINED and OWNER_FIRST) at once, before the server answers. The answer keeps the change, a refusal restores the ids and the order of both lists.
#[tokio::test(flavor = "multi_thread")]
async fn member_actions_change_both_member_lists_before_the_answer_and_a_refusal_restores_them() {
    let joined = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    type Case = (&'static str, &'static str, &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("add_member", "AddMember", "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("remove_member", "RemoveMember", "u2", [&["u1", "u3"], &["u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (action, operation, user, changed, original) in cases {
        for refusal in [false, true] {
            let case = format!("{action} {}", if refusal { "refused" } else { "answered" });
            let rig = Rig::encrypted().await;
            rig.login_ios("A").await;
            let epoch = rig.database.session_epoch();
            let (values, args) = group("friends");
            let created = confirmed_group(&rig, epoch, values, args).await;
            seed_members(&rig, &created.id);
            let gate = if refusal { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
            let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": [user]}).to_string()).await }) };
            gate.entered().await;
            let receipt = bound(acting).await.unwrap().unwrap();
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(changed[0]), joined(changed[1])), "{case}: the lists before the answer");
            gate.release();
            let result = bound(rig.database.outcome(receipt)).await.unwrap();
            settle(&rig).await;
            let expected = if refusal { original } else { changed };
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(expected[0]), joined(expected[1])), "{case}: the lists after the outcome");
            assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
            rig.database.close_now();
        }
    }
}

/// K13 (B6, C05): leave removes the group from the list before the answer. The answer keeps it away, a refusal puts it back.
#[tokio::test(flavor = "multi_thread")]
async fn leave_removes_the_group_from_its_list_before_the_answer_and_a_refusal_restores_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, groups(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the list shows the group", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        let gate = if refusal { rig.world.hold_then("LeaveGroup", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("LeaveGroup") };
        let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
        gate.entered().await;
        yield_until("the group left the list before the answer", || ids_of(reader.rows()).is_empty()).await;
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        if refusal {
            yield_until("the refusal put the group back", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        } else {
            assert!(ids_of(reader.rows()).is_empty(), "{case}: the group came back after the answer");
        }
        rig.database.close_now();
    }
}

/// K13 (B6, C05): forward puts a local copy of the message into the thread of the target chat before the answer. The answer replaces the copy with its own row, a refusal removes the copy.
#[tokio::test(flavor = "multi_thread")]
async fn forward_shows_a_local_copy_in_the_target_thread_and_a_refusal_removes_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || ids_of(reader.rows()) == strings(&["m1"])).await;
        let gate = if refusal { rig.world.hold_then("MessageForward", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("MessageForward") };
        let acting = { let database = rig.database.clone(); tokio::spawn(async move { database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c1"}).to_string()).await }) };
        gate.entered().await;
        yield_until("the thread shows the copy before the answer", || ids_of(reader.rows()).len() == 2).await;
        let during = ids_of(reader.rows());
        assert!(during.contains(&"m1".to_string()), "{case}: the source left the thread: {during:?}");
        let copy = during.iter().find(|id| id.as_str() != "m1").unwrap();
        assert!(crate::identity::RowId::new(copy).unwrap().is_temp(), "{case}: the copy {copy} is not a temporary row");
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        let expected = if refusal { strings(&["m1"]) } else { strings(&["m1", "forwarded-1"]) };
        crate::test_wait::yield_until_showing(&format!("{case}: expected {expected:?}, the thread shows "), || format!("{:?}", ids_of(reader.rows())), || ids_of(reader.rows()) == expected).await;
        rig.database.close_now();
    }
}

type Call = for<'a> fn(&'a Database, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbError>> + 'a>>;

fn case(name: &'static str, call: Call) -> (&'static str, Call) { (name, call) }

/// K17 every entry of the database names the epoch of its caller and is refused after that session ended. Nothing reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn every_entry_of_a_left_account_is_refused_and_reaches_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let before = rig.world.log().len();
    let calls = vec![
        case("run", |db, old| Box::pin(async move { bound(db.run(old, "MyProfile".into(), "{}".into())).await.map(drop) })),
        case("load", |db, old| Box::pin(async move { bound(db.load(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh", |db, old| Box::pin(async move { bound(db.refresh(old, "User".into(), Some("all".into()), "{}".into())).await })),
        case("load_all", |db, old| Box::pin(async move { bound(db.load_all(old, "User".into(), Some("all".into()), "{}".into(), true)).await })),
        case("load_more", |db, old| Box::pin(async move { bound(db.load_more(old, "User".into(), Some("all".into()), "{}".into())).await.map(drop) })),
        case("node", |db, old| Box::pin(async move { bound(db.node(old, "User".into(), "node".into(), "user".into(), true)).await.map(drop) })),
        case("nodes", |db, old| Box::pin(async move { bound(db.nodes(old, "User".into(), "nodes".into(), vec!["user".into()])).await.map(drop) })),
        case("edge_values", |db, old| Box::pin(async move { bound(db.edge_values(old, "User".into(), Some("all".into()), "{}".into(), vec!["user".into()])).await.map(drop) })),
        case("peek_row", |db, old| Box::pin(async move { db.peek_row(old, "User".into(), "user".into()).map(drop) })),
        case("cancel_upload", |db, old| Box::pin(async move { bound(db.cancel_upload(old, "User".into(), "user".into(), "upload".into())).await.map(drop) })),
    ];
    for (name, call) in calls {
        let result = call(&rig.database, old).await;
        assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "{name} of a left account was not refused: {result:?}");
    }
    assert_eq!(rig.world.log().len(), before, "a refused entry reached the server");
    rig.database.close_now();
}

/// K09 a list stream opened with refresh on connect loads again once when the network returns, without a call of the app. A stream without the flag asks nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_refreshes_on_connect_loads_again_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let list = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"refreshOnConnect":true}"#) }, list.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    settle(&rig).await;
    let before = rig.world.count("Thread");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the list loaded again", || rig.world.count("Thread") > before).await;
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "the stream loaded more or less than once on reconnect");
    rig.database.cancel_stream(stream);
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "a cancelled stream loaded on reconnect");
    rig.database.close_now();
}

/// K10 (out_sending_temp, out_sending_tick): a command whose request failed on the network is sent again by the backoff, the clock delivers it, and one pair reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn a_command_that_failed_on_the_network_is_delivered_by_the_backoff_and_the_clock() {
    for failures in [1, 2] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        for _ in 0..failures { rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down); }
        let mut armed = rig.clock.armed();
        let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let waiting = { let database = rig.database.clone(); let done = done.clone(); tokio::spawn(async move { let outcome = database.outcome(receipt).await; done.store(true, Ordering::SeqCst); outcome }) };
        for retry in 0..failures {
            rig.clock.await_armed(armed).await;
            armed = rig.clock.armed();
            rig.clock.advance(crate::runtime::RetryPolicy::Outbox.backoff().delay(retry).as_nanos().div_ceil(1_000_000) as u64);
        }
        yield_until("the command is delivered", || done.load(Ordering::SeqCst)).await;
        let outcome = bound(waiting).await.unwrap().unwrap();
        assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "{failures} failures: the command was not delivered: {outcome:?}");
        let sequences: std::collections::BTreeSet<_> = rig.world.log().iter().filter(|entry| entry.operation == "SendMessage").map(|entry| entry.sequence).collect();
        assert_eq!(sequences.len(), 1, "one pair reaches the server: {:?}", rig.world.log());
        rig.database.close_now();
    }
}

/// K23 (D01, old lines ffi/command.rs serve_in_epoch and sync/land.rs land_rows): a request that was accepted in the epoch of A, and whose answer waits at the server while another identity starts a session, lands nothing and ends refused. The epoch of A does not change, the generation does not either.
#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_request_whose_answer_lands_after_the_next_account_started_writes_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into())).await.unwrap();
    let before = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().expect("the row of A");
    assert!(named(&before, "A"));
    let gate = rig.world.hold_next("MyProfile");
    gate.as_account("late");
    let running = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), Some("profile".into()), "{}".into()).await }) };
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    assert_ne!(rig.database.session_epoch(), epoch);
    gate.release();
    let result = bound(running).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "the request of the epoch that left ended with {result:?}");
    settle(&rig).await;
    assert_eq!(rig.database.db.row_count("User").unwrap(), 0, "the answer of the epoch that left was written");
    rig.database.close_now();
}

/// Fable review 3 (old lines sync/send/flush.rs land_response): a create the server confirmed keeps its row when the cache of another model clears between the answer and its landing. The epoch stays, only the generation moves.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_create_keeps_its_row_when_another_model_resets_before_it_lands() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("SendMessage");
    let receipt = bound(rig.database.send(message(epoch, "kept", 9))).await.unwrap();
    gate.entered().await;
    let generation = rig.database.db.generation();
    bound(rig.database.reset_models_of(&["User"])).await;
    assert_ne!(rig.database.db.generation(), generation, "the reset of another model did not move the generation");
    assert_eq!(rig.database.session_epoch(), epoch, "the reset of another model ended the epoch");
    gate.release();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    let WireOperationOutcome::Confirmed { value: Some(row), .. } = outcome else { panic!("the confirmed create ended with {outcome:?}") };
    settle(&rig).await;
    let found = rig.database.db.find("Message", &crate::identity::RowId::new(&row.id).unwrap()).unwrap();
    assert!(found.is_some(), "the row {} the server confirmed is gone", row.id);
    assert!(crate::ffi::tests::queue_view_tests::queued(&rig.database).is_empty(), "the confirmed create is still queued");
    rig.database.close_now();
}

/// K25 (D04, old lines db/roll_back.rs restore and db/prior.rs reappearing): two operations change the same member of the same lists and both are refused. The lists end the way they were, in both orders of the two changes, the first refusal arriving while the second still holds its answer.
#[tokio::test(flavor = "multi_thread")]
async fn two_refused_changes_of_one_member_restore_the_original_lists_in_both_orders() {
    type Case = (&'static str, [(&'static str, &'static str); 2], &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("remove then add a member", [("remove_member", "RemoveMember"), ("add_member", "AddMember")], "u2", [&["u1", "u3", "u2"], &["u1", "u3", "u2"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("add then remove a member", [("add_member", "AddMember"), ("remove_member", "RemoveMember")], "u4", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (case, [first, second], user, while_second, original) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let (first_gate, second_gate) = (rig.world.hold_then(first.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }), rig.world.hold_then(second.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }));
        let args = json!({"userIds": [user]}).to_string();
        let start = |action: &'static str| { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start(first.0);
        first_gate.entered().await;
        let second_call = start(second.0);
        yield_until("the second change is queued", || crate::ffi::tests::queue_view_tests::queued(&rig.database).len() == 2).await;
        first_gate.release();
        second_gate.entered().await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let expected = |lists: [&[&str]; 2]| (strings(lists[0]), strings(lists[1]));
        assert_eq!(lists(&rig), expected(while_second), "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let first = bound(first_call).await.unwrap().unwrap();
        let second = bound(second_call).await.unwrap().unwrap();
        assert!(matches!(bound(rig.database.outcome(first)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        assert!(matches!(bound(rig.database.outcome(second)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        settle(&rig).await;
        assert_eq!(lists(&rig), expected(original), "{case}: the lists after both refusals");
        rig.database.close_now();
    }
}

/// K26 (E04 and the outcome of an accepted call, old lines db/local_write.rs join_scope_rows and leave_scope_rows, sync/send/mod.rs settled): the same change of a member made twice, once the first and once the second refused, leaves the member the way the accepted change wants, also while the second still holds its answer. The accepted call returns its success and the refused call its refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_change_of_a_member_does_not_undo_the_same_change_accepted_and_each_call_gets_its_own_outcome() {
    type Case = (&'static str, (&'static str, &'static str), &'static str, [&'static [&'static str]; 2], bool);
    let cases: [Case; 4] = [
        ("add twice, first refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], true),
        ("remove twice, first refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], true),
        ("add twice, second refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], false),
        ("remove twice, second refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], false),
    ];
    for (case, (action, operation), user, expected, first_refused) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let held = |refused: bool| if refused { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
        let first_gate = held(first_refused);
        let second_gate = held(!first_refused);
        let args = json!({"userIds": [user]}).to_string();
        let start = || { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start();
        first_gate.entered().await;
        let second_call = start();
        yield_until("the second change is queued", || crate::ffi::tests::queue_view_tests::queued(&rig.database).len() == 2).await;
        first_gate.release();
        second_gate.entered().await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let wanted = (strings(expected[0]), strings(expected[1]));
        assert_eq!(lists(&rig), wanted, "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let (first, second) = (bound(first_call).await.unwrap().unwrap(), bound(second_call).await.unwrap().unwrap());
        let (first, second) = (bound(rig.database.outcome(first)).await.unwrap(), bound(rig.database.outcome(second)).await.unwrap());
        assert_eq!((matches!(first, WireOperationOutcome::Rejected { .. }), matches!(second, WireOperationOutcome::Rejected { .. })), (first_refused, !first_refused), "{case}: the outcomes differ");
        settle(&rig).await;
        assert_eq!(lists(&rig), wanted, "{case}: the lists after both answers");
        rig.database.close_now();
    }
}
