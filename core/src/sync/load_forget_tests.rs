use serde_json::json;

use super::load::LoadOptions;
use super::rig::*;

fn vibe(number: usize) -> serde_json::Map<String, serde_json::Value> {
    params(json!({"vibeId": format!("v{number}")}))
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_19_a_reset_forgets_the_windows_that_nobody_observes_and_keeps_the_observed_one() {
    let rig = rig();
    for number in 0..30 {
        drop(rig.sync.relation_state("moments", "vibe_feed", &vibe(number)).unwrap());
    }
    let kept = rig.sync.relation_state("moments", "vibe_feed", &vibe(99)).unwrap();
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 31);
    rig.db.reset_all().unwrap();
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 1);
    assert_eq!(kept.borrow().phase, super::load_machine::LoadPhase::NotLoaded);
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_19_a_reset_keeps_a_window_with_a_flight() {
    let rig = rig();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["a"], false, "c1").after(std::time::Duration::from_secs(30)));
    let sync = rig.sync.clone();
    let flight = tokio::spawn(async move { sync.load("moments", "vibe_feed", &vibe(1), LoadOptions::default()).await });
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    rig.db.reset_all().unwrap();
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 1);
    flight.abort();
}
