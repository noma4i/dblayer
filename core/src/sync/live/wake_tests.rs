use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use serde_json::json;
use tokio::runtime::Handle;

use super::{live_key, receiver_key, EventEffect, SubscriptionDef};
use crate::schema::UpdatesSpec;
use crate::sync::locked;
use crate::sync::rig::*;
use crate::test_wait::{mutex_free, yield_until, ProbeWaker};
use crate::transport::EventSpec;

const OPERATION: &str = "MomentEvents";

fn updates() -> UpdatesSpec {
    serde_json::from_value(json!({"subscription": OPERATION})).unwrap()
}

fn definition() -> SubscriptionDef {
    SubscriptionDef::new(EventSpec::new(OPERATION), OPERATION, "moments", EventEffect::Upsert)
}

fn probe_for(rig: &Rig) -> Arc<ProbeWaker> {
    let inner = rig.sync.inner.clone();
    ProbeWaker::new(move || mutex_free(&inner.lives) && inner.live_effects.is_free())
}

fn poll_changed<F: Future>(changed: &mut std::pin::Pin<Box<F>>, probe: &Arc<ProbeWaker>) -> Poll<F::Output> {
    changed.as_mut().poll(&mut Context::from_waker(&Waker::from(probe.clone())))
}

#[tokio::test]
async fn live_ready_wake_has_no_guards() {
    let rig = rig();
    let inner = rig.sync.inner.clone();
    let key = live_key(OPERATION, &json!({}));
    let mut ready = {
        let mut lives = locked(&inner.lives);
        lives.entry(key.clone()).or_default().ready.subscribe()
    };
    let probe = probe_for(&rig);
    let mut changed = Box::pin(ready.changed());
    assert!(poll_changed(&mut changed, &probe).is_pending());
    inner.acquire_one("moments", updates(), None);
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "the reset of ready wakes its waiter with the lives guard free");
}

#[test]
fn live_removal_drops_the_live_outside_the_guard() { crate::test_wait::paused(async {
    for lingering in [false, true] {
        let rig = rig();
        let inner = rig.sync.inner.clone();
        let key = live_key(OPERATION, &json!({}));
        let receiver = receiver_key("moments", &updates(), &json!({}));
        let mut ready = {
            let mut lives = locked(&inner.lives);
            let live = lives.entry(key.clone()).or_default();
            live.readers = 1;
            live.receivers = vec![(receiver.clone(), definition(), 1)];
            live.runtime = lingering.then(Handle::current);
            live.ready.subscribe()
        };
        let probe = probe_for(&rig);
        let mut changed = Box::pin(ready.changed());
        assert!(poll_changed(&mut changed, &probe).is_pending());
        inner.release_live(&receiver);
        if lingering {
            assert!(!locked(&inner.lives).is_empty(), "the live lingers first");
            tokio::task::yield_now().await;
            crate::test_wait::pass(inner.config.live_linger + Duration::from_millis(1)).await;
            yield_until("the linger ended", || locked(&inner.lives).is_empty()).await;
        }
        assert!(locked(&inner.lives).is_empty(), "lingering={lingering}: the last reader removed the live");
        assert_eq!((probe.wakes(), probe.violations()), (1, 0), "lingering={lingering}: dropping the live closes ready with the lives guard free");
    }
}) }
