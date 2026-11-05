use std::sync::{mpsc, Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use super::toys_tests::*;
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
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(60), 9));
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
    runtime.post(sender, EpochStamp::Epoch(1), ToyInput::Go);
    assert_eq!(fired.recv_timeout(WAIT), Ok(3));
}

#[test]
fn threaded_executor_drops_input_of_another_epoch() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::new();
    let toy = builder.add::<Toy>("toy", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.start_epoch(2);
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::Fire(1));
    runtime.post(toy, EpochStamp::Epoch(2), ToyInput::Fire(2));
    assert_eq!(fired.recv_timeout(WAIT), Ok(2));
    sleep(Duration::from_millis(100));
    assert!(fired.try_recv().is_err());
}

#[test]
fn asynchronous_effect_result_returns_through_the_mailbox_of_its_owner() {
    let (report, fired) = mpsc::channel();
    let pending = ParkedBacks::default();
    let mut builder = RuntimeBuilder::new();
    let pump = builder.add::<Toy>("pump", ToyState::default(), ToyRunner { pending: Some(pending.clone()), ..ToyRunner::default() });
    let ledger = builder.add::<Toy>("ledger", ToyState { pump: Some(pump), ..ToyState::default() }, ToyRunner { pending: Some(pending), report: Some(report), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(ledger, EpochStamp::Epoch(1), ToyInput::WorkAsync(4));
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
    runtime.post(hub, EpochStamp::Control, HubInput::Begin(1));
    runtime.post(hub, EpochStamp::Control, HubInput::Finish(1, Cause::SessionReset));
    runtime.post(hub, EpochStamp::Control, HubInput::Shutdown);
    let mut got: Vec<u32> = (0..4).map(|_| reports.recv_timeout(WAIT).unwrap()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 1, CLOSED, CLOSED]);
}

#[test]
fn a_failed_start_of_an_owner_stops_the_owners_already_started() {
    let ended = Arc::new(Mutex::new(Vec::new()));
    let mut builder = RuntimeBuilder::new();
    for name in ["first", "second", "third"] {
        builder.add::<Toy>(name, ToyState::default(), ToyRunner::default());
    }
    let mut spawned = 0;
    let result = builder.start_with(|name, body| {
        spawned += 1;
        let which = spawned;
        if which == 3 {
            return Err(std::io::Error::other("no thread for the third owner"));
        }
        let ended = ended.clone();
        crate::task::spawn_thread(crate::task::Carry::Internal, name, move || {
            body();
            ended.lock().unwrap().push(which);
        })
    });
    assert!(result.is_err());
    let mut ended = ended.lock().unwrap().clone();
    ended.sort_unstable();
    assert_eq!(ended, vec![1, 2], "the two threads that started ended before start returned");
}
