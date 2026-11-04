use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use crate::query::Query;
use crate::sync::LoadOptions;
use crate::transport::Reply;

#[test]
fn inv_id_01_a_numeric_id_and_the_same_id_as_text_are_one_row_in_the_model_the_list_and_the_membership() { crate::test_wait::paused(async {
    let rig = rig();
    let params = serde_json::Map::new();
    for id in [json!(54), json!("54")] {
        rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![json!({"id": id, "text": "x", "userId": null})], false, "c")));
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &params, LoadOptions { force: true }).await.unwrap();
        let ids: Vec<String> = rig.db.query(&Query::new("moments")).unwrap().iter().map(|row| row.id.to_string()).collect();
        assert_eq!(ids, ["54"], "landed {id}");
        assert!(rig.db.find("moments", &rid("54")).unwrap().is_some(), "landed {id}");
        let members = rig.db.shared.store.scope_members("moments", &feed_key(&rig)).unwrap();
        assert_eq!(members.iter().map(|(row, _)| row.as_str()).collect::<Vec<_>>(), ["54"], "landed {id}");
    }
}) }

fn feed_key(rig: &Rig) -> String {
    let spec = rig.db.spec("moments").unwrap();
    let relation = spec.relations.iter().find(|relation| relation.name == "feed").unwrap();
    relation.scope_key("moments", &Default::default()).unwrap().as_str().to_string()
}
