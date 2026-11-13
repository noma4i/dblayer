use std::future::Future;
use std::sync::Arc;

use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::{Database, DbError};

const FEED: &str = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }";
const NODE: &str = "query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title } } }";
const NODES: &str = "query ContainersByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Container { id title } } }";

fn feed_page(id: &str) -> Value {
    json!({"data":{"feed":{"nodes":[{"__typename":"Moment","id":id,"uuid":id,"likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":format!("after-{id}")}}}})
}

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(json!([{"name":"MomentsFeed","document":FEED},{"name":"ContainerNode","document":NODE},{"name":"ContainersByIds","document":NODES}]).to_string()).await.unwrap();
    rig.world.answer_with("MomentsFeed", feed_page("1"));
    rig.world.answer_with("ContainerNode", json!({"data":{"node":{"__typename":"Container","id":"k1","title":"x"}}}));
    rig.world.answer_with("ContainersByIds", json!({"data":{"nodes":[{"__typename":"Container","id":"k1","title":"x"}]}}));
    rig
}

fn requests(rig: &Rig, operation: &str) -> usize { rig.world.log().iter().filter(|entry| entry.operation == operation).count() }

/// L4 the second call of a scope registers while the first one is held at the server, then the first call is cancelled: the held request goes on for the second call.
async fn second_call_survives_cancel_of_first<F, Fut>(rig: &Rig, operation: &str, call: F, after_first_entered: impl FnOnce())
where
    F: Fn(Arc<Database>) -> Fut,
    Fut: Future<Output = Result<(), DbError>> + Send + 'static,
{
    let held = rig.world.hold_next(operation);
    let first = tokio::spawn(call(rig.database.clone()));
    held.entered().await;
    after_first_entered();
    let mut second = Box::pin(call(rig.database.clone()));
    std::future::poll_fn(|cx| { drop(second.as_mut().poll(cx)); std::task::Poll::Ready(()) }).await;
    crate::ffi::owners_drained(&rig.database).await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn(second);
    crate::ffi::owners_drained(&rig.database).await.unwrap();
    held.release();
    bound(second).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_node_and_nodes_survive_the_cancel_of_an_earlier_call_of_the_same_scope() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "MomentsFeed", move |db| async move { db.refresh(epoch, "Moment".into(), Some("feed".into()), "{}".into()).await }, || ()).await;
    assert_eq!(requests(&rig, "MomentsFeed"), 1, "the second refresh sent a request of its own");
    rig.database.close_now();
    let rig = self::rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "ContainerNode", move |db| async move { db.node(epoch, "Container".into(), "node".into(), "k1".into(), true).await.map(drop) }, || ()).await;
    assert_eq!(requests(&rig, "ContainerNode"), 1, "the second node sent a request of its own");
    rig.database.close_now();
    let rig = self::rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "ContainersByIds", move |db| async move { db.nodes(epoch, "Container".into(), "nodes".into(), vec!["k1".into()]).await.map(drop) }, || ()).await;
    assert_eq!(requests(&rig, "ContainersByIds"), 1, "the second nodes sent a request of its own");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn load_more_waits_for_the_page_that_an_earlier_cancelled_load_more_started() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap().result()).await.unwrap();
    rig.world.answer_with("MomentsFeed", feed_page("2"));
    let world = rig.world.clone();
    second_call_survives_cancel_of_first(&rig, "MomentsFeed", move |db| async move { db.load_more(epoch, "Moment".into(), Some("feed".into()), "{}".into()).await.map(drop) }, move || world.answer_with("MomentsFeed", feed_page("3"))).await;
    assert!(rig.database.peek_row(epoch, "Moment".into(), "2".into()).await.unwrap().is_some(), "the page of the held request did not land: the cancel of the first call stopped it");
    assert!(rig.database.peek_row(epoch, "Moment".into(), "3".into()).await.unwrap().is_some(), "the second load more did not read its own page");
    rig.database.close_now();
}

/// boundary_node_read_epoch: a node whose request is held while the session ends gets the end of its epoch.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_node_read_epoch() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("ContainerNode");
    let reading = { let database = rig.database.clone(); rig.spawn(async move { database.node(epoch, "Container".into(), "node".into(), "k1".into(), true).await }) };
    held.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    held.release();
    let result = bound(reading).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "{result:?}");
    rig.database.close_now();
}

fn group_names(rig: &Rig) -> Vec<String> {
    rig.world.log().iter().filter(|entry| entry.operation == "CreateGroup").map(|entry| {
        let body: Value = serde_json::from_str(&entry.body).unwrap();
        body.pointer("/variables/name").or_else(|| body.pointer("/variables/input/name")).and_then(Value::as_str).unwrap_or_else(|| panic!("no group name in {body}")).to_string()
    }).collect()
}

fn permutation(seed: u64, count: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..count).collect();
    match seed {
        0 => order.reverse(),
        _ => {
            let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            for index in (1..count).rev() {
                state ^= state << 13; state ^= state >> 7; state ^= state << 17;
                order.swap(index, (state % (index as u64 + 1)) as usize);
            }
        }
    }
    order
}

/// Requirement P-BR-10: calls that started in an order enter the owners in that order, whatever the order of their tasks.
/// Observable: the mutation requests at the transport carry the names of the calls in the order of their places.
/// Defect caught: the body enters the owner in the order the tasks run, not in the order of the places (D-419).
#[test]
fn bridge_prop_calls_enter_in_the_order_they_started() {
    let seeds: Vec<_> = (0..6u64).map(|seed| std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap().block_on(async move {
            let rig = Rig::encrypted().await;
            rig.login_ios("A").await;
            let epoch = rig.database.session_epoch();
            let places: Vec<_> = (0..12).map(|_| rig.database.in_order()).collect();
            let mut tasks = Vec::new();
            for index in permutation(seed, 12) {
                let place = places[index].clone();
                let (values, args) = super::bridge_support_tests::group(&format!("call-{index:02}"));
                tasks.push(tokio::spawn(async move { place.create(epoch, "Group".into(), values, args).await }));
            }
            for task in tasks { bound(task).await.unwrap().unwrap(); }
            rig.settle().await;
            let expected: Vec<String> = (0..12).map(|index| format!("call-{index:02}")).collect();
            assert_eq!(group_names(&rig), expected, "seed {seed}: the requests left in the order of the tasks");
            rig.database.close_now();
        });
    })).collect();
    for seed in seeds { crate::test_wait::join_within(seed); }
}

/// Requirement P-BR-10: a place that never reaches an owner does not hold the next one.
/// Observable: the call of the next place ends and its request leaves in all 3 cases, and a pull of a stream gets its value while a place of the database is held.
/// Defect caught: a place without a pass stops the queue.
#[tokio::test(flavor = "multi_thread")]
async fn bridge_call_place_never_holds_the_next() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let create = |name: &str| super::bridge_support_tests::group(name);
    let (first, second) = (rig.database.in_order(), rig.database.in_order());
    drop(first);
    let (values, args) = create("after-dropped-descriptor");
    bound(second.create(epoch, "Group".into(), values, args)).await.unwrap();
    let (first, second) = (rig.database.in_order(), rig.database.in_order());
    let (values, args) = create("never-polled");
    let never_polled = async move { first.create(epoch, "Group".into(), values, args).await };
    drop(never_polled);
    let (values, args) = create("after-dropped-future");
    bound(second.create(epoch, "Group".into(), values, args)).await.unwrap();
    let base_place = rig.database.in_order();
    let stream = rig.database.subscribe_values(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Group"}"#) });
    assert!(matches!(bound(stream.pull_next()).await, Some(crate::ffi::ReadValue::Snapshot { .. })), "a pull of a stream waited for a place of the database that never entered an owner");
    drop(base_place);
    let held = rig.world.hold_next("CreateGroup");
    let (holder, waiting, next) = (rig.database.in_order(), rig.database.in_order(), rig.database.in_order());
    let (values, args) = create("holder");
    let holder = bound(holder.create(epoch, "Group".into(), values, args)).await.unwrap();
    held.entered().await;
    let temp = holder.optimistic_value.unwrap().id;
    let waiting = tokio::spawn(async move { waiting.act(epoch, "Group".into(), temp, "add_member".into(), "{}".into(), json!({"userIds":["u1"]}).to_string()).await });
    let (values, args) = create("after-waiting-call");
    bound(next.create(epoch, "Group".into(), values, args)).await.unwrap();
    held.release();
    bound(waiting).await.unwrap().unwrap();
    rig.settle().await;
    let names = group_names(&rig);
    for sent in ["after-dropped-descriptor", "after-dropped-future", "holder", "after-waiting-call"] {
        assert!(names.iter().any(|name| name == sent), "the request of {sent} did not leave: {names:?}");
    }
    assert!(!names.iter().any(|name| name == "never-polled"), "a dropped call sent a request: {names:?}");
    rig.database.close_now();
}

/// Requirement P-BR-10 (5): a call that follows a dropped waiting call of the same reader enters the owner after the cancel of the dropped one.
/// Observable: the pull that starts right after the drop gets the end of its stream by the cancel (`Cancelled`), not the refusal of a second waiting pull.
/// Defect caught: the abort of the dropped call is asynchronous, so the new pull reaches the owner before the cancel of the old one (D-420B).
/// Hold: both workers of the executor stand in blocking tasks, so the abort of the first task cannot run before the second call is queued; one worker is released and takes the queued second call first.
#[tokio::test(flavor = "multi_thread")]
async fn bridge_call_after_a_dropped_call_enters_after_its_cancel() {
    let mut note = crate::schema::ModelSpec::new("Note", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    note.storage = crate::schema::Storage::Local;
    let database = Database::open(crate::ffi::Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![note]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap();
    let stream = database.subscribe_values(database.session_epoch(), crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Note"}"#) });
    assert!(matches!(crate::test_wait::within(stream.pull_next()).await, Some(crate::ffi::ReadValue::Snapshot { .. })));
    let mut pending = Box::pin(stream.pull_next());
    assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())).await);
    crate::ffi::owners_drained(&database).await.unwrap();
    let executor = crate::ffi::database::executor().unwrap().handle().clone();
    let stands: Vec<_> = (0..2).map(|_| (Arc::new(crate::test_wait::Flag::default()), Arc::new(crate::test_wait::Flag::default()))).collect();
    for (entered, release) in &stands {
        let (entered, release) = (entered.clone(), release.clone());
        drop(crate::task::spawn_on(&executor, async move { entered.set(); release.wait_blocking("release the worker"); }));
    }
    for (entered, _) in &stands { entered.wait("a worker of the executor stands").await; }
    let next = stream.clone();
    let second = crate::task::spawn_on(&executor, async move { next.pull_next().await });
    drop(pending);
    stands[0].1.set();
    let value = crate::test_wait::within(second).await.unwrap();
    stands[1].1.set();
    assert!(matches!(value, Some(crate::ffi::ReadValue::Ended { cause: crate::ffi::EndCause::Cancelled })), "the pull after the dropped pull entered before its cancel: {value:?}");
    crate::test_wait::within(database.close_database()).await.unwrap();
}
