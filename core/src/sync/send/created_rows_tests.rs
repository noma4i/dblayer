use serde_json::json;

use crate::commit::ScopeRef;
use crate::db::Db;
use crate::identity::ModelKey;
use crate::schema::ModelSpec;
use crate::transport::MockTransport;

use super::super::rig::*;
use super::super::SyncConfig;

fn rig_where_feed_takes_created_rows() -> Rig {
    let mut moments = moments_spec();
    moments.relations.iter_mut().find(|relation| relation.name == "feed").unwrap().joins_created = true;
    let specs: Vec<ModelSpec> = vec![users_spec(), moments, vibes_spec()];
    rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default())
}

fn members_of(rig: &Rig, relation: &str) -> Vec<String> {
    let spec = rig.db.spec("moments").unwrap();
    let relation = spec.relations.iter().find(|candidate| candidate.name == relation).unwrap();
    let scope = ScopeRef::new(ModelKey::new("moments"), relation.scope_key("moments", &Default::default()).unwrap());
    rig.db.shared.store.scope_members("moments", scope.key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

#[tokio::test(start_paused = true)]
async fn a_created_row_joins_the_param_less_list_that_declares_created_rows_and_no_other() {
    let rig = rig_where_feed_takes_created_rows();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();

    assert_eq!(members_of(&rig, "feed"), vec![local.id.to_string()]);
    assert!(members_of(&rig, "history").is_empty());
}
