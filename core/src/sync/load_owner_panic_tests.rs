use serde_json::Map;

use super::load_machine::{Event, LoadPhase};
use super::rig::*;

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_11_a_panic_in_one_window_event_fails_that_window_with_an_internal_error_and_keeps_the_owner() {
    let rig = rig();
    let feed = rig.sync.relation_state("moments", "feed", &none()).await.unwrap();
    let current = rig.sync.relation_state("users", "current", &none()).await.unwrap();
    let window = rig.sync.inner.resolve(crate::lifecycle::Origin::Observed, "moments", "feed", &none()).unwrap().window_key.clone();
    let owner = rig.sync.inner.owner.clone();
    owner.panic_on(&window);
    owner.submit(Event::Abandoned { window: window.clone(), flight: 99 });
    let failed = feed.borrow().clone();
    assert_eq!((failed.phase, failed.error.map(|error| error.kind)), (LoadPhase::Failed, Some("internal")));
    assert_eq!(current.borrow().phase, LoadPhase::NotLoaded);
    owner.submit(Event::ReadOk { window, has_rows: true });
    let recovered = feed.borrow().clone();
    assert_eq!((recovered.phase, recovered.error), (LoadPhase::NotLoaded, None));
}
