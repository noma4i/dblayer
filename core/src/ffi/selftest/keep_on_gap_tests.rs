use std::sync::Arc;

use serde_json::{json, Value};

use super::delivery_reactivity_tests::{feed_database_at, FeedTransport};
use super::rig_tests::{bound, World};
use crate::ffi::tape::Rows;
use crate::ffi::{Database, ReadRequest, ReadValue};
use crate::test_wait::{settled, signalled};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Break {
    Cable,
    Network,
    Restart,
}

fn page(ids: &[&str], title: &str) -> Value {
    let chats: Vec<Value> = ids.iter().map(|id| json!({"id": id, "title": title, "revision": 1, "lists": ["PRIMARY", "SECONDARY"]})).collect();
    json!({"data": {"chats": chats}})
}

fn requests(world: &World, list: &str) -> usize {
    world.log().iter().filter(|entry| entry.operation == "ChatList" && serde_json::from_str::<Value>(&entry.body).ok().and_then(|body| body.pointer("/variables/list").and_then(Value::as_str).map(str::to_string)).as_deref() == Some(list)).count()
}

fn ids(rows: &Rows) -> Vec<String> { rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

fn shows(rows: &Rows, text: &str) -> bool { rows.0.lock().unwrap().iter().any(|row| row.cells.contains(&crate::ffi::Cell::Text { value: text.into() })) }

async fn open_at(path: Option<String>, held: Arc<crate::ffi::tests::session_guards_tests::WatchedHeld>, world: &Arc<World>) -> (Arc<Database>, Arc<FeedTransport>) {
    let listen = json!({"subscription":"RecordEvents", "root":"recordEvents.item", "apply":{"effect":"upsert"}});
    feed_database_at(path, held, world.clone(), Some(listen), None, false, vec![json!({"area":"MESSAGE","relation":"node","payload":"chat","removes_on":["DELETED"]})], (true, true)).await
}

fn watch(database: &Database, relation: &str, list: &str) -> (Arc<Rows>, Arc<crate::ffi::tape::Watched>) {
    let rows = Arc::new(Rows::default());
    let query = json!({"model":"Chat","relation":relation,"params":{"list":list}}).to_string();
    let handle = database.watch(ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, rows.clone());
    (rows, handle)
}

/// Requirement P-FEED-44 (D-421): a break of the connection does not reread the window of a scope that declares `keep_on_gap`; the window of the same model without the declaration is reread once.
/// Observable: the requests of each window and its rows after the break (cable closed, network flap, restart of the base), and the one event after the reconnect.
/// Defect caught: the step of the break marks every window of the model, so the declared window loads a new first page.
#[tokio::test(flavor = "multi_thread")]
async fn gap_keeps_the_window_of_a_scope_that_declares_it() {
    let mut table = Vec::new();
    for case in [Break::Cable, Break::Network, Break::Restart] {
        let world = World::new("server", "A", true);
        world.answer_with("ChatList", page(&["c1", "c2"], "old"));
        let directory = tempfile::tempdir().unwrap();
        let path = matches!(case, Break::Restart).then(|| directory.path().join("db").to_string_lossy().into_owned());
        let held: Arc<crate::ffi::tests::session_guards_tests::WatchedHeld> = Arc::default();
        let (database, transport) = open_at(path.clone(), held.clone(), &world).await;
        let events = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
        assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
        let (plain, plain_handle) = watch(&database, "list", "PRIMARY");
        let (kept, kept_handle) = watch(&database, "pinned", "SECONDARY");
        let mut handles = Some((plain_handle, kept_handle));
        settled(&database, "both windows show their first page", || ids(&plain) == ["c1", "c2"] && ids(&kept) == ["c1", "c2"]).await;
        signalled(&transport.opened, "the subscription opens", || transport.token.lock().unwrap().is_some()).await;
        let token = transport.token.lock().unwrap().take().unwrap();
        world.answer_with("ChatList", page(&["c3", "c1"], "new"));
        if matches!(case, Break::Restart) {
            handles = None;
            settled(&database, "the readers left", || true).await;
        }
        match case {
            Break::Network => {
                database.set_online(false).await.unwrap();
                database.set_online(true).await.unwrap();
            }
            Break::Cable | Break::Restart => {
                assert!(transport.cable.close(&token));
                settled(&database, "the closed cable is seen", || true).await;
                transport.clock.advance(120_000);
            }
        }
        loop {
            if matches!(bound(events.pull_next()).await, Some(ReadValue::Gap)) { break; }
        }
        let mut after_event = None;
        match case {
            Break::Cable | Break::Network => {
                signalled(&transport.opened, "the subscription opens again", || transport.token.lock().unwrap().is_some()).await;
                settled(&database, "the window without the declaration is reread", || ids(&plain) == ["c3", "c1"]).await;
                if matches!(case, Break::Cable) {
                    let token = transport.token.lock().unwrap().clone().unwrap();
                    assert!(transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"c1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY","SECONDARY"]}}]}}})));
                    settled(&database, "the event changes both windows", || shows(&plain, "after") && shows(&kept, "after")).await;
                    after_event = Some((shows(&plain, "after"), shows(&kept, "after")));
                }
                table.push((case, requests(&world, "PRIMARY"), requests(&world, "SECONDARY"), ids(&plain), ids(&kept), after_event));
            }
            Break::Restart => {
                bound(database.close_database()).await.unwrap();
                let (database, _transport) = open_at(path, held, &world).await;
                let (plain, _plain_handle) = watch(&database, "list", "PRIMARY");
                let (kept, _kept_handle) = watch(&database, "pinned", "SECONDARY");
                settled(&database, "both windows show their rows after the restart", || ids(&plain) == ["c3", "c1"] && !ids(&kept).is_empty()).await;
                table.push((case, requests(&world, "PRIMARY"), requests(&world, "SECONDARY"), ids(&plain), ids(&kept), None));
                bound(database.close_database()).await.unwrap();
                continue;
            }
        }
        drop(handles);
        bound(database.close_database()).await.unwrap();
    }
    let rows = |first: &[&str]| first.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    let wanted = vec![
        (Break::Cable, 2, 1, rows(&["c3", "c1"]), rows(&["c1", "c2"]), Some((true, true))),
        (Break::Network, 2, 1, rows(&["c3", "c1"]), rows(&["c1", "c2"]), None),
        (Break::Restart, 2, 1, rows(&["c3", "c1"]), rows(&["c1", "c2"]), None),
    ];
    assert_eq!(table, wanted, "(break, requests of the window without the declaration, requests of the window with it, their rows, the event shown by both)");
}

/// net_background_closes_the_cable (D-444): after a stay in the background, a watched window reads once on the return, with or without `refresh_on_foreground`.
/// Observable: the requests of each window between the return and the reopened cable.
/// Defect caught: the load of the foreground and the gap of the reopened cable both reread the declared window.
#[tokio::test(flavor = "multi_thread")]
async fn net_background_return_loads_each_window_once() {
    let world = World::new("server", "A", true);
    world.answer_with("ChatList", page(&["c1", "c2"], "old"));
    let held: Arc<crate::ffi::tests::session_guards_tests::WatchedHeld> = Arc::default();
    let (database, transport) = open_at(None, held, &world).await;
    let events = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Chat".into() });
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Ready)));
    let (plain, _plain_handle) = watch(&database, "list", "PRIMARY");
    let (declared, _declared_handle) = watch(&database, "foreground", "SECONDARY");
    settled(&database, "both windows show their first page", || ids(&plain) == ["c1", "c2"] && ids(&declared) == ["c1", "c2"]).await;
    signalled(&transport.opened, "the subscription opens", || transport.token.lock().unwrap().is_some()).await;
    transport.token.lock().unwrap().take();
    let before = (requests(&world, "PRIMARY"), requests(&world, "SECONDARY"));

    let sink = crate::ffi::OnlineSink::of_life(&database.life);
    let registration = sink.register_foreground();
    sink.enter_background(registration);
    settled(&database, "the background reached the owners", || true).await;
    sink.enter_foreground(registration);
    signalled(&transport.opened, "the subscription opens again", || transport.token.lock().unwrap().is_some()).await;
    settled(&database, "the windows were read after the return", || true).await;

    assert_eq!((requests(&world, "PRIMARY") - before.0, requests(&world, "SECONDARY") - before.1), (1, 1), "(window without the declaration, window with it)");
    database.close_now();
}
