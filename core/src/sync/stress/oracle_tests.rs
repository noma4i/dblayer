use super::*;
use workload::Op;
use crate::sync::rig::rig_on;

#[tokio::test]
async fn inv_stress_01_oracle_reports_unreadable_rows() {
    for check in [oracle::settled_client, oracle::reloaded_client] {
        let transport = MockTransport::new();
        let findings = Findings::default();
        let server = FakeServer::install(&transport, 7, findings.clone());
        let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig::default());
        let scratch = scratch_dir();
        let world = World::new(rig, server, findings.clone(), Universe::default(), Aliases::default(), scratch.path().to_path_buf());
        world.rig.db.execute_for_test("DROP TABLE row_moments");
        check(&world);
        assert!(findings.all().iter().any(|entry| entry.starts_with("[oracle-read]") && entry.contains("row_moments")), "{:?}", findings.all());
    }
}

#[tokio::test]
async fn cancelled_local_writes_are_not_lost_but_pending_writes_are() {
    let transport = MockTransport::new();
    let findings = Findings::default();
    let server = FakeServer::install(&transport, 7, findings.clone());
    let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig::default());
    let scratch = scratch_dir();
    let world = World::new(rig, server, findings.clone(), Universe::default(), Aliases::default(), scratch.path().to_path_buf());
    world.go_offline(true);
    let mut worker = Worker::new(0, 7);
    worker.run(&world, Op::Create).await;
    worker.run(&world, Op::Update).await;
    worker.run(&world, Op::Destroy).await;
    assert!(world.rig.db.outbox().unwrap().is_empty());
    assert_eq!(world.server.received(), 0);
    oracle::lost_writes(&world);
    assert!(findings.all().is_empty(), "{:?}", findings.all());
    worker.run(&world, Op::Create).await;
    oracle::lost_writes(&world);
    assert_eq!(findings.all().len(), 1);
}

#[tokio::test]
async fn reset_cancels_previous_issues_but_not_later_ones() {
    let transport = MockTransport::new();
    let findings = Findings::default();
    let server = FakeServer::install(&transport, 7, findings.clone());
    let rig = rig_on(Db::open_in_memory(specs(), None).unwrap(), transport, SyncConfig::default());
    let scratch = scratch_dir();
    let world = World::new(rig, server, findings.clone(), Universe::default(), Aliases::default(), scratch.path().to_path_buf());
    world.go_offline(true);
    let mut worker = Worker::new(0, 7);
    worker.run(&world, Op::Create).await;
    world.reset(&["moments"]);
    assert!(world.rig.db.outbox().unwrap().is_empty());
    oracle::lost_writes(&world);
    assert!(findings.all().is_empty(), "{:?}", findings.all());
    worker.run(&world, Op::Create).await;
    oracle::lost_writes(&world);
    assert_eq!(findings.all().len(), 1);
}
