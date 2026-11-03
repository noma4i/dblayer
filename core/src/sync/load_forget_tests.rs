use serde_json::json;

use super::load::LoadOptions;
use super::rig::*;
use crate::test_wait::join_within;

fn vibe(number: usize) -> serde_json::Map<String, serde_json::Value> {
    params(json!({"vibeId": format!("v{number}")}))
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_19_unobserved_windows_are_forgotten_without_waiting_for_reset() {
    let rig = rig();
    for number in 0..30 {
        drop(rig.sync.relation_state("moments", "vibe_feed", &vibe(number)).await.unwrap());
    }
    let kept = rig.sync.relation_state("moments", "vibe_feed", &vibe(99)).await.unwrap();
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 1);
    let second = kept.clone();
    drop(kept);
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 1);
    rig.db.reset_all().unwrap();
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 1);
    assert_eq!(second.borrow().phase, super::load_machine::LoadPhase::NotLoaded);
    drop(second);
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 0);
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
    assert!(flight.await.unwrap_err().is_cancelled());
    assert_eq!(rig.sync.inner.owner.tracked_windows(), 0);
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_19_a_new_observer_of_a_window_whose_last_watch_drops_concurrently_stays_live() {
    let rig = rig();
    let owner = rig.sync.inner.owner.clone();
    for number in 0..200 {
        let resolved = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "vibe_feed", &vibe(number)).unwrap();
        let (window, target) = (resolved.window_key.clone(), resolved.target());
        let last = owner.observe(&window, target.clone(), None);
        let gate = std::sync::Arc::new(crate::test_wait::Rendezvous::new(2));
        let dropper = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.meet();
                drop(last);
            })
        };
        gate.meet();
        let fresh = owner.observe(&window, target.clone(), None);
        join_within(dropper);
        owner.begin_flight(&window, target, false, None);
        assert_eq!(fresh.borrow().phase, super::load_machine::LoadPhase::Loading, "window {number} lost its observer");
    }
}

fn stored(order: i64, total: Option<i64>, data: super::load_machine::Data) -> super::load_machine::Stored {
    let landed = matches!(data, super::load_machine::Data::Landed { .. });
    super::load_machine::Stored { epoch: 0, order: super::load_machine::Order { applied: order, pages: i64::from(landed) }, data, has_rows: landed && total.is_some(), revision: 0 }
}

#[tokio::test(start_paused = true)]
async fn inv_loadm_19_a_late_seed_never_replaces_what_a_flight_already_landed() {
    use super::load_machine::{Data, LoadPhase};
    let rig = rig();
    let owner = rig.sync.inner.owner.clone();
    let resolved = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "vibe_feed", &vibe(1)).unwrap();
    let (window, target) = (resolved.window_key.clone(), resolved.target());
    let watch = owner.observe(&window, target.clone(), None);
    let landed = || stored(5, Some(3), Data::Landed { has_more: false, total: Some(3), loaded_at: 7 });
    let cold = || stored(-1, None, Data::NotLoaded);
    let ticket = super::load_owner::FlightTicket::new(&owner, &window, target, false, Some(landed()));
    owner.seed_observed(&window, resolved.model.clone(), Some(cold()));
    assert_eq!((watch.borrow().phase, watch.borrow().total), (LoadPhase::Loading, Some(3)), "a late seed after Start");
    ticket.landed(Some(landed()));
    owner.seed_observed(&window, resolved.model.clone(), Some(cold()));
    assert_eq!((watch.borrow().phase, watch.borrow().total, watch.borrow().loaded_at), (LoadPhase::Loaded, Some(3), Some(7)), "a late seed after Landed");
}
