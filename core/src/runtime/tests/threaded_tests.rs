use std::sync::{mpsc, Arc, Mutex};

use super::toys_tests::*;
use super::*;


fn reporting(report: mpsc::Sender<u32>) -> ToyRunner { ToyRunner { report: Some(report), ..ToyRunner::default() } }

struct OrderedRecipient;

#[derive(Debug)]
enum OrderedRecipientInput { Hold, Notice, Idle }

#[derive(Debug)]
enum OrderedRecipientEffect { Hold }

impl StateOwner for OrderedRecipient {
    const EPOCHED: bool = false;
    type State = ();
    type Input = OrderedRecipientInput;
    type Effect = OrderedRecipientEffect;
    fn step(_: &mut (), input: Self::Input) -> Effects<Self> {
        match input { OrderedRecipientInput::Hold => vec![Effect::Run(OrderedRecipientEffect::Hold)], _ => Vec::new() }
    }
    fn tick(_: Time) -> Self::Input { OrderedRecipientInput::Idle }
    fn epoch_ended(_: Epoch, _: Cause) -> Self::Input { OrderedRecipientInput::Idle }
    fn closing() -> Self::Input { OrderedRecipientInput::Idle }
}

struct HoldRecipient { entered: mpsc::Sender<()>, release: mpsc::Receiver<()> }

impl EffectRunner<OrderedRecipient> for HoldRecipient {
    fn run(&mut self, _: EpochStamp, _: OrderedRecipientEffect, _: &Back<OrderedRecipient>) {
        self.entered.send(()).unwrap();
        crate::test_wait::recv_within(&self.release);
    }
}

struct OrderedSource;

#[derive(Debug)]
enum OrderedSourceInput { Go, Idle }

#[derive(Debug)]
enum OrderedSourceEffect { Observe }

impl StateOwner for OrderedSource {
    const EPOCHED: bool = false;
    type State = Address<OrderedRecipient>;
    type Input = OrderedSourceInput;
    type Effect = OrderedSourceEffect;
    fn step(target: &mut Self::State, input: Self::Input) -> Effects<Self> {
        match input {
            OrderedSourceInput::Go => vec![Effect::Send(Outbound::new(*target, OrderedRecipientInput::Notice)), Effect::Run(OrderedSourceEffect::Observe), Effect::Broadcast(Control::Closing), Effect::Run(OrderedSourceEffect::Observe)],
            OrderedSourceInput::Idle => Vec::new(),
        }
    }
    fn tick(_: Time) -> Self::Input { OrderedSourceInput::Idle }
    fn epoch_ended(_: Epoch, _: Cause) -> Self::Input { OrderedSourceInput::Idle }
    fn closing() -> Self::Input { OrderedSourceInput::Idle }
}

struct ObservePending { handle: Handle, count: mpsc::Sender<usize> }

impl EffectRunner<OrderedSource> for ObservePending {
    fn run(&mut self, _: EpochStamp, _: OrderedSourceEffect, _: &Back<OrderedSource>) { self.count.send(self.handle.pending()).unwrap(); }
}

struct ObserveRuns(Arc<Mutex<usize>>);

impl EffectRunner<OrderedSource> for ObserveRuns {
    fn run(&mut self, _: EpochStamp, _: OrderedSourceEffect, _: &Back<OrderedSource>) { *self.0.lock().unwrap() += 1; }
}

#[test]
fn mixed_effects_enqueue_before_the_next_run_in_the_scheduler() {
    let (entered, _held) = mpsc::channel();
    let (_release, release_rx) = mpsc::channel();
    let mut det = DetScheduler::new(0);
    let recipient = det.add::<OrderedRecipient>("recipient", (), HoldRecipient { entered, release: release_rx });
    let observed = Arc::new(Mutex::new(0));
    let source = det.add::<OrderedSource>("source", recipient, ObserveRuns(observed.clone()));
    det.post(source, EpochStamp::Control, OrderedSourceInput::Go);
    let choose = |det: &mut DetScheduler, wanted| {
        let index = det.choices().iter().position(|choice| *choice == wanted).unwrap();
        assert!(det.step_choice(index));
    };
    choose(&mut det, Choice::Step(source.index()));
    choose(&mut det, Choice::Emit(source.index()));
    assert!(det.choices().contains(&Choice::Step(recipient.index())));
    assert_eq!(*observed.lock().unwrap(), 0);
    choose(&mut det, Choice::Emit(source.index()));
    assert_eq!(*observed.lock().unwrap(), 1);
    choose(&mut det, Choice::Emit(source.index()));
    choose(&mut det, Choice::Step(recipient.index()));
    choose(&mut det, Choice::Step(recipient.index()));
    let inputs: Vec<_> = det.history(recipient).into_iter().map(|entry| entry.input.as_str()).collect();
    assert_eq!(inputs, ["Notice", "Idle"]);
    assert_eq!(*observed.lock().unwrap(), 1);
    choose(&mut det, Choice::Emit(source.index()));
    assert_eq!(*observed.lock().unwrap(), 2);
}

#[test]
fn mixed_effects_enqueue_before_the_next_run_on_threads() {
    let (entered, held) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let (count, counts) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let handle = builder.handle();
    let recipient = builder.add::<OrderedRecipient>("recipient", (), HoldRecipient { entered, release: release_rx });
    let source = builder.add::<OrderedSource>("source", recipient, ObservePending { handle, count });
    let runtime = builder.start().unwrap();
    runtime.post(recipient, EpochStamp::Control, OrderedRecipientInput::Hold);
    crate::test_wait::recv_within(&held);
    runtime.post(source, EpochStamp::Control, OrderedSourceInput::Go);
    assert_eq!(crate::test_wait::recv_within(&counts), 3, "Send entered the blocked recipient mailbox");
    assert_eq!(crate::test_wait::recv_within(&counts), 5, "Broadcast entered both mailboxes before the next Run");
    release.send(()).unwrap();
    runtime.barrier();
}

#[test]
fn hang_report_identifies_held_input_and_queued_work() {
    let (entered, held) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let recipient = builder.add::<OrderedRecipient>("recipient", (), HoldRecipient { entered, release: release_rx });
    let runtime = builder.start().unwrap();
    runtime.post(recipient, EpochStamp::Control, OrderedRecipientInput::Hold);
    crate::test_wait::recv_within(&held);
    runtime.post(recipient, EpochStamp::Control, OrderedRecipientInput::Notice);
    let report = crate::test_wait::hang_report();
    release.send(()).unwrap();
    runtime.barrier();
    assert!(report.contains("recipient: queued 1 [Notice "), "{report}");
    assert!(report.contains("active [Hold "), "{report}");
}

#[test]
fn idle_runtime_has_zero_wakeups_and_schedule_fires_once() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let toy = builder.add::<Toy>("toy", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.barrier();
    assert_eq!(runtime.wakeups(toy), 0);
    runtime.start_epoch(1);
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(60), 9));
    assert_eq!(crate::test_wait::recv_within(&fired), 9);
    runtime.barrier();
    let settled = runtime.wakeups(toy);
    runtime.barrier();
    assert_eq!(runtime.wakeups(toy), settled);
    assert!(fired.try_recv().is_err());
}

#[test]
fn owners_on_separate_threads_exchange_values() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let receiver = builder.add::<Toy>("receiver", ToyState::default(), reporting(report));
    let sender = builder.add::<Toy>("sender", ToyState { on_go: Some((receiver, ToyInput::Fire(3))), ..ToyState::default() }, ToyRunner::default());
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(sender, EpochStamp::Epoch(1), ToyInput::Go);
    assert_eq!(crate::test_wait::recv_within(&fired), 3);
}

#[test]
fn threaded_executor_drops_input_of_another_epoch() {
    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let toy = builder.add::<Toy>("toy", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.start_epoch(2);
    runtime.post(toy, EpochStamp::Epoch(1), ToyInput::Fire(1));
    runtime.post(toy, EpochStamp::Epoch(2), ToyInput::Fire(2));
    assert_eq!(crate::test_wait::recv_within(&fired), 2);
    runtime.barrier();
    assert!(fired.try_recv().is_err());
}

#[test]
fn asynchronous_effect_result_returns_through_the_mailbox_of_its_owner() {
    let (report, fired) = mpsc::channel();
    let pending = ParkedBacks::default();
    let mut builder = RuntimeBuilder::for_tests();
    let pump = builder.add::<Toy>("pump", ToyState::default(), ToyRunner { pending: Some(pending.clone()), ..ToyRunner::default() });
    let ledger = builder.add::<Toy>("ledger", ToyState { pump: Some(pump), ..ToyState::default() }, ToyRunner { pending: Some(pending), report: Some(report), ..ToyRunner::default() });
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(ledger, EpochStamp::Epoch(1), ToyInput::WorkAsync(4));
    assert_eq!(crate::test_wait::recv_within(&fired), 8);
}

#[test]
fn lifecycle_broadcast_reaches_every_owner_on_threads() {
    let (report, reports) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let hub = builder.add::<Hub>("hub", (), HubRunner);
    builder.add::<Toy>("first", ToyState::default(), reporting(report.clone()));
    builder.add::<Toy>("second", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.post(hub, EpochStamp::Control, HubInput::Begin(1));
    runtime.post(hub, EpochStamp::Control, HubInput::Finish(1, Cause::SessionReset));
    runtime.post(hub, EpochStamp::Control, HubInput::Shutdown);
    let mut got: Vec<u32> = (0..4).map(|_| crate::test_wait::recv_within(&reports)).collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 1, CLOSED, CLOSED]);
}

#[test]
fn a_failed_start_of_an_owner_stops_the_owners_already_started() {
    let ended = Arc::new(Mutex::new(Vec::new()));
    let mut builder = RuntimeBuilder::for_tests();
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
        crate::task::spawn_thread(name, move || {
            body();
            ended.lock().unwrap().push(which);
        })
    });
    assert!(result.is_err());
    let mut ended = ended.lock().unwrap().clone();
    ended.sort_unstable();
    assert_eq!(ended, vec![1, 2], "the two threads that started ended before start returned");
}

#[test]
fn rt_prop_addresses_reserved() {
    let mut missing = RuntimeBuilder::for_tests();
    missing.reserve::<Toy>();
    assert!(missing.start().is_err());

    let (report, fired) = mpsc::channel();
    let mut builder = RuntimeBuilder::for_tests();
    let sender = builder.reserve::<Toy>();
    let receiver = builder.reserve::<Toy>();
    builder.install(sender, "sender", ToyState { on_go: Some((receiver, ToyInput::Fire(3))), ..ToyState::default() }, ToyRunner::default());
    builder.install(receiver, "receiver", ToyState::default(), reporting(report));
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(sender, EpochStamp::Epoch(1), ToyInput::Go);
    assert_eq!(crate::test_wait::recv_within(&fired), 3);
}
