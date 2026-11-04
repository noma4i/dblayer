use std::time::Duration;

use serde_json::Map;

use super::super::rig::*;
use crate::sync::LoadOptions;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn inv_fresh_02_an_invalidation_during_a_flight_gets_exactly_one_repeat_that_the_older_answer_does_not_satisfy() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.transport.enqueue("MomentsFeed", crate::transport::Reply::data(feed_page(vec![moment("m1", "m1")], false, "c2")).after(Duration::from_secs(5)));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c3"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    let invalidate = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        crate::sync::rig::app(&rig.sync, rig.sync.inner.reload_lists_of_absent_parent(&rig.db.admit().unwrap(), "moments")).await.unwrap();
    };
    let no_params = none();
    let (loaded, ()) = tokio::join!(async { rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &no_params, LoadOptions { force: true }).await }, invalidate);
    loaded.unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    let key = rig.sync.window_key("moments", "feed", &none()).unwrap();
    assert_eq!(rig.transport.call_count("MomentsFeed"), 3);
    assert!(!rig.db.shared.store.window_freshness(&key).unwrap().unwrap().invalidated);
    assert_eq!(member_ids(&rig, "moments", "feed", &none()), ["m1", "m2"]);
}
