use std::sync::Arc;

use serde_json::Map;

use super::load_tests::rig;
use super::stream::tape::Rows;
use super::{LivePhase, StreamEnd};
use crate::sync::LoadPhase;

#[tokio::test]
async fn inv_loadm_12_a_query_the_core_rejects_tells_its_subscriber_in_the_state_ends_the_stream_and_leaves_the_relation_window_alone() {
    let (db, _) = rig().await;
    let sync = db.sync.as_ref().unwrap();
    let state = sync.relation_state("Moment", "feed", &Map::new()).await.unwrap();
    let query = r#"{"model":"Moment","relation":"feed","params":{},"where":{"type":"equal","field":"nope","value":1}}"#;
    let rows = Arc::new(Rows::default());
    let rejected = db.watch_query(query, rows.clone());
    crate::test_wait::yield_until("the rejected query ended", || !rows.ended().is_empty()).await;
    let first = rows.state().unwrap();
    assert_eq!((first.phase, first.error_kind.as_deref(), rows.len()), (Some(LivePhase::Failed), Some("invalid"), 0));
    assert_eq!(rows.ended(), vec![StreamEnd::Invalid], "a rejected query ends with one outcome");
    assert_eq!(db.stream_census().open, 0, "an ended stream leaves the table");
    assert!(rejected.wait_for_initial_load().await.is_err());
    let untouched = state.borrow().clone();
    assert_eq!((untouched.phase, untouched.error.map(|error| error.kind)), (LoadPhase::NotLoaded, None));
}
