use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::{Sync, SyncConfig};
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::transport::{EventSpec, MockTransport};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn a_dropped_subscription_reconnects_only_when_the_database_clock_passes_its_delay() { crate::test_wait::paused(async {
    let transport = MockTransport::new();
    let clock = ManualClock::new(Duration::from_millis(START_MS));
    let sync = Sync::new(Db::open_in_memory(specs(), None).unwrap(), Arc::new(transport.clone()), Arc::new(clock.clone()), SyncConfig::default());
    sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let _subscription = sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "moments", EventEffect::Upsert)).await.unwrap();
    transport.await_calls("MomentChanged", 1).await;

    transport.disconnect("MomentChanged");
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(transport.call_count("MomentChanged"), 1, "reconnected while the database clock stood still");
    clock.advance(Duration::from_secs(1));
    transport.await_calls("MomentChanged", 2).await;
    assert_eq!(transport.call_count("MomentChanged"), 2);
}) }
