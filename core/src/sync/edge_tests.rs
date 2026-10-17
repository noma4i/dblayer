use serde_json::{json, Map, Value};

use super::rig::*;
use super::{LoadOptions, SyncConfig};
use crate::db::Db;
use crate::transport::{MockTransport, Reply};

fn none() -> Map<String, Value> {
    Map::new()
}

fn vibe(user: &str) -> Map<String, Value> {
    params(json!({"vibeId": user}))
}

fn edge(id: &str, uuid: Value, score: Value) -> Value {
    json!({"impressionUuid": uuid, "impressionScore": score, "node": {"id": id, "text": id}})
}

fn page(edges: Vec<Value>) -> Reply {
    Reply::data(json!({"feed": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": "c"}}}))
}

async fn refresh(rig: &Rig, relation: &str, params: &Map<String, Value>) {
    rig.sync.load("moments", relation, params, LoadOptions { force: true }).await.unwrap();
}

fn read(rig: &Rig, relation: &str, params: &Map<String, Value>, id: &str) -> Option<Map<String, Value>> {
    rig.sync.edge_values("moments", relation, params, &[id.to_string()]).unwrap().remove(id)
}

fn values(uuid: &str, score: i64) -> Option<Map<String, Value>> {
    Some(params(json!({"impressionUuid": uuid, "impressionScore": score})))
}

#[tokio::test(start_paused = true)]
async fn a_value_from_another_connection_does_not_replace_the_main_feed_value() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!(null), json!(null))]));
    refresh(&rig, "feed", &none()).await;
    refresh(&rig, "vibe_feed", &vibe("v1")).await;
    assert_eq!(read(&rig, "feed", &none(), "m1"), values("imp-1", 5));
    assert_eq!(read(&rig, "vibe_feed", &vibe("v1"), "m1"), Some(Map::new()));
}

#[tokio::test(start_paused = true)]
async fn connections_keep_their_own_non_null_values_by_key() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("vibe-a"), json!(7))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("vibe-b"), json!(9))]));
    refresh(&rig, "feed", &none()).await;
    refresh(&rig, "vibe_feed", &vibe("a")).await;
    refresh(&rig, "vibe_feed", &vibe("b")).await;
    assert_eq!(read(&rig, "feed", &none(), "m1"), values("imp-1", 5));
    assert_eq!(read(&rig, "vibe_feed", &vibe("a"), "m1"), values("vibe-a", 7));
    assert_eq!(read(&rig, "vibe_feed", &vibe("b"), "m1"), values("vibe-b", 9));
}

#[tokio::test(start_paused = true)]
async fn the_same_connection_replaces_its_value_with_a_new_one_and_with_null() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-2"), json!(6))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!(null), json!(null))]));
    refresh(&rig, "feed", &none()).await;
    refresh(&rig, "feed", &none()).await;
    assert_eq!(read(&rig, "feed", &none(), "m1"), values("imp-2", 6));
    refresh(&rig, "feed", &none()).await;
    assert_eq!(read(&rig, "feed", &none(), "m1"), Some(Map::new()));
}

#[tokio::test(start_paused = true)]
async fn an_answer_without_edges_leaves_the_connection_values_alone() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5))]));
    rig.transport.enqueue("MomentsByUser", Reply::data(json!({"moments": [{"id": "m1", "text": "plain", "userId": "u1"}]})));
    refresh(&rig, "feed", &none()).await;
    rig.sync.load("moments", "by_user", &params(json!({"userId": "u1"})), LoadOptions { force: true }).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1").as_deref(), Some("plain"));
    assert_eq!(read(&rig, "feed", &none(), "m1"), values("imp-1", 5));
}

#[tokio::test(start_paused = true)]
async fn a_row_that_leaves_the_connection_loses_its_value() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5)), edge("m2", json!("imp-2"), json!(6))]));
    rig.transport.enqueue("MomentsFeed", page(vec![edge("m2", json!("imp-2"), json!(6))]));
    refresh(&rig, "feed", &none()).await;
    refresh(&rig, "feed", &none()).await;
    assert_eq!(read(&rig, "feed", &none(), "m1"), None);
    assert_eq!(read(&rig, "feed", &none(), "m2"), values("imp-2", 6));
}

#[tokio::test(start_paused = true)]
async fn the_values_survive_a_restart_and_a_cache_reset_clears_them() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("edge.sqlite");
    let transport = MockTransport::new();
    transport.enqueue("MomentsFeed", page(vec![edge("m1", json!("imp-1"), json!(5))]));
    {
        let rig = rig_on(Db::open(&path, specs(), None).unwrap(), transport.clone(), SyncConfig::default());
        refresh(&rig, "feed", &none()).await;
    }
    let rig = rig_on(Db::open(&path, specs(), None).unwrap(), transport, SyncConfig::default());
    assert_eq!(read(&rig, "feed", &none(), "m1"), values("imp-1", 5));
    rig.db.reset(&["moments"]).unwrap();
    assert_eq!(read(&rig, "feed", &none(), "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn one_read_costs_the_same_at_20_and_2000_list_rows() {
    let mut reads = Vec::new();
    for size in [20, 2000] {
        let rig = rig();
        let edges = (0..size).map(|number| edge(&format!("m{number}"), json!(format!("imp-{number}")), json!(number))).collect();
        rig.transport.enqueue("MomentsFeed", page(edges));
        refresh(&rig, "feed", &none()).await;
        let before = rig.sync.counters();
        assert_eq!(read(&rig, "feed", &none(), "m7").map(|found| found.len()), Some(2));
        let after = rig.sync.counters();
        reads.push((after.edge_read_steps - before.edge_read_steps, after.edge_read_scans - before.edge_read_scans));
    }
    assert!(reads[0].0 > 0, "the read was counted: {reads:?}");
    assert_eq!(reads[0], reads[1], "steps and full scans of one read at 20 and 2000 rows");
    assert_eq!(reads[1].1, 0, "no table scan");
}
