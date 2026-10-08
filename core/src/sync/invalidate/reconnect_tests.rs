use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;
use crate::transport::{Reply, TransportError};

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn reconnect_sends_the_queue_then_rereads_watched_windows() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m9", "m1"], false, "c2"));
    rig.sync.load("moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let local = rig.db.create("moments", params(json!({"text": "t"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m9", "text": "t", "userId": null}})));
    assert_eq!(rig.sync.flush().await.unwrap().committed(), 0);
    assert_eq!(rig.db.outbox().unwrap().len(), 1);
    assert_eq!(rig.sync.reconnect().await.unwrap().flush.committed(), 0);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(rig.db.retry("moments", &local.id).unwrap(), 1);
    let report = rig.sync.reconnect().await.unwrap();
    assert_eq!(report.flush.committed(), 1);
    assert_eq!(report.resume.failed, 0);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 2);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m9", "m1"]);
}
