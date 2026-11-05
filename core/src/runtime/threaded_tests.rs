use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;

use super::testing::*;
use super::*;

const WAIT: Duration = Duration::from_secs(5);

fn reporting(report: mpsc::Sender<u32>) -> ToyRunner { ToyRunner { report: Some(report), ..ToyRunner::default() } }

#[test]
fn idle_runtime_has_zero_wakeups_and_schedule_fires_once() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::new();
    let toy = builder.add::<Toy>("toy", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    sleep(Duration::from_millis(150));
    assert_eq!(runtime.wakeups(toy), 0);
    runtime.start_epoch(1);
    runtime.send(toy, Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(60), 9));
    assert_eq!(fired.recv_timeout(WAIT), Ok(9));
    sleep(Duration::from_millis(50));
    let settled = runtime.wakeups(toy);
    sleep(Duration::from_millis(200));
    assert_eq!(runtime.wakeups(toy), settled);
    assert!(fired.try_recv().is_err());
}

#[test]
fn owners_on_separate_threads_exchange_values() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::new();
    let receiver = builder.add::<Toy>("receiver", ToyState::default(), reporting(report));
    let sender = builder.add::<Toy>("sender", ToyState { on_go: Some((receiver, ToyInput::Fire(3))), ..ToyState::default() }, ToyRunner::default());
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.send(sender, Stamp::Epoch(1), ToyInput::Go);
    assert_eq!(fired.recv_timeout(WAIT), Ok(3));
}

#[test]
fn threaded_executor_drops_input_of_another_epoch() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::new();
    let toy = builder.add::<Toy>("toy", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.start_epoch(2);
    runtime.send(toy, Stamp::Epoch(1), ToyInput::Fire(1));
    runtime.send(toy, Stamp::Epoch(2), ToyInput::Fire(2));
    assert_eq!(fired.recv_timeout(WAIT), Ok(2));
    sleep(Duration::from_millis(100));
    assert!(fired.try_recv().is_err());
}

#[test]
fn asynchronous_effect_result_returns_through_the_mailbox_of_its_owner() {
    let (report, fired) = mpsc::channel();
    let pending = Pending::default();
    let mut builder = RuntimeBuilder::new();
    let pump = builder.add::<Toy>("pump", ToyState::default(), ToyRunner { pending: Some(pending.clone()), ..ToyRunner::default() });
    let ledger = builder.add::<Toy>("ledger", ToyState { pump: Some(pump), ..ToyState::default() }, ToyRunner { pending: Some(pending), report: Some(report), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.send(ledger, Stamp::Epoch(1), ToyInput::WorkAsync(4));
    assert_eq!(fired.recv_timeout(WAIT), Ok(8));
}

#[test]
fn lifecycle_broadcast_reaches_every_owner_on_threads() {
    let (report, reports) = mpsc::channel();
    let mut builder = RuntimeBuilder::new();
    let hub = builder.add::<Hub>("hub", (), HubRunner);
    builder.add::<Toy>("first", ToyState::default(), reporting(report.clone()));
    builder.add::<Toy>("second", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.send(hub, Stamp::Control, HubInput::Begin(1));
    runtime.send(hub, Stamp::Control, HubInput::Finish(1, Cause::SessionReset));
    runtime.send(hub, Stamp::Control, HubInput::Shutdown);
    let mut got: Vec<u32> = (0..4).map(|_| reports.recv_timeout(WAIT).unwrap()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 1, CLOSED, CLOSED]);
}
