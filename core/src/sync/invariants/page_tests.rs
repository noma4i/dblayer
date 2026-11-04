use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[test]
fn inv_dsl_03_the_page_size_of_a_remote_declaration_reaches_the_request() { crate::test_wait::paused(async {
    use super::super::SyncConfig;
    use crate::db::Db;
    use crate::transport::MockTransport;
    let mut moments = moments_spec();
    for relation in moments.relations.iter_mut().filter(|relation| relation.name == "feed") {
        relation.remote.as_mut().unwrap().page_size = Some(7);
    }
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c1"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &none(), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.calls()[0].variables["first"], json!(7));
}) }
