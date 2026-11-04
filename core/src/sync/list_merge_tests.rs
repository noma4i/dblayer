use super::load::LoadOptions;
use super::rig::*;
use crate::db::fixtures::{calls, changes, recorder, rid};
use crate::query::Query;
use crate::transport::Reply;
use serde_json::{json, Map, Value};

fn user(id: &str) -> Map<String, Value> {
    params(json!({"userId": id}))
}

fn list(items: &[(&str, &str)]) -> Reply {
    Reply::data(json!({"moments": items.iter().map(|(id, text)| moment(id, text)).collect::<Vec<_>>()}))
}

async fn load_list(rig: &Rig, owner: &str, items: &[(&str, &str)]) {
    let reply = list(items);
    rig.transport.respond_with(move |request| (request.operation_name == "MomentsByUser").then(|| reply.clone()));
    crate::test_wait::within(rig.sync.load(&rig.db.admit().unwrap(), "moments", "by_user", &user(owner), LoadOptions { force: true })).await.unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
}

async fn load_feed(rig: &Rig, items: &[&str], has_next: bool) {
    let reply = feed_reply(items, has_next, "c1");
    rig.transport.respond_with(move |request| (request.operation_name == "MomentsFeed").then(|| reply.clone()));
    crate::test_wait::within(rig.sync.load(&rig.db.admit().unwrap(), "moments", "feed", &Map::new(), LoadOptions { force: true })).await.unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
}

fn observe(rig: &Rig, relation: &str, params: &Map<String, Value>) -> (crate::db::fixtures::ChangeLog, crate::db::ObserverHandle) {
    let key = rig.db.spec("moments").unwrap().selection_key(relation, params).unwrap();
    let (log, callback) = recorder();
    let subscription = rig.db.observe(Query::new("moments").in_scope(key), callback).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    (log, subscription)
}

#[test]
fn inv_identity_01_a_pending_row_of_another_model_with_the_same_id_does_not_keep_a_row_in_a_reloaded_list() { crate::test_wait::paused(async {
    let rig = rig();
    load_list(&rig, "u1", &[("same", "server")]).await;
    rig.db.create(&rig.db.admit().unwrap(), "users", params(json!({"id": "same", "name": "pending"}))).unwrap();
    load_list(&rig, "u1", &[]).await;
    assert_eq!(member_ids(&rig, "moments", "by_user", &user("u1")), Vec::<String>::new());
    assert!(rig.db.find("users", &rid("same")).unwrap().is_some());
}) }

#[test]
fn inv_merge_04_inv_observe_05_three_reloads_give_one_delivery_each_and_unchanged_rows_stay_out_of_updated() { crate::test_wait::paused(async {
    let rig = rig();
    load_list(&rig, "u1", &[("m1", "one"), ("m2", "two")]).await;
    let (log, _subscription) = observe(&rig, "by_user", &user("u1"));
    for step in [vec![("m1", "one"), ("m2", "two!"), ("m3", "three")], vec![("m1", "uno"), ("m2", "two!"), ("m3", "three")], vec![("m3", "three"), ("m1", "uno"), ("m2", "two!")]] {
        load_list(&rig, "u1", &step).await;
    }
    let all = changes(&log);
    assert_eq!(all.len(), 4);
    assert_eq!((all[1].diff.updated.clone(), all[1].diff.inserted.clone()), (vec![rid("m2")], vec![rid("m3")]));
    assert_eq!(all[2].diff.updated, vec![rid("m1")]);
    assert_eq!((all[3].diff.moved.len(), all[3].diff.updated.len()), (1, 0));
}) }

#[test]
fn inv_merge_07_a_page_answer_keeps_the_tail_after_its_anchor_and_drops_it_without_one() { crate::test_wait::paused(async {
    for (answer, expected) in [(vec!["m1", "m3"], vec!["m1", "m3", "m4", "m5"]), (vec!["m9", "m8"], vec!["m9", "m8"])] {
        let rig = rig();
        load_feed(&rig, &["m1", "m2", "m3", "m4", "m5"], false).await;
        load_feed(&rig, &answer, true).await;
        assert_eq!(member_ids(&rig, "moments", "feed", &Map::new()), expected, "{answer:?}");
        assert!(rig.db.find("moments", &rid("m2")).unwrap().is_some());
    }
}) }

#[test]
fn inv_merge_08_a_reload_changes_only_its_own_list() { crate::test_wait::paused(async {
    let rig = rig();
    load_list(&rig, "u1", &[("m1", "1"), ("m2", "2")]).await;
    load_list(&rig, "u2", &[("m3", "3")]).await;
    let (other, _subscription) = observe(&rig, "by_user", &user("u2"));
    load_list(&rig, "u1", &[("m2", "2")]).await;
    assert_eq!(member_ids(&rig, "moments", "by_user", &user("u2")), ["m3"]);
    assert_eq!(calls(&other), vec![1]);
    assert_eq!(member_ids(&rig, "moments", "by_user", &user("u1")), ["m2"]);
}) }
