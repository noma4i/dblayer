use super::toys_tests::*;
use super::*;

#[test]
fn rt_prop_overload_bounded() {
    const FRAMES: u32 = 10 * 256;
    const CALLS: u32 = 2 * 8;
    const OFFLINE: u32 = 1_000_000;
    const ONLINE: u32 = 1_000_001;
    let parked = ParkedBacks::default();
    let mut builder = RuntimeBuilder::for_tests();
    let store = builder.add::<Toy>("store", ToyState::default(), ToyRunner { pending: Some(parked), ..ToyRunner::default() });
    let feed = builder.add::<Toy>("feed", ToyState::default(), ToyRunner::default());
    builder.record();
    let runtime = builder.start().unwrap();
    runtime.start_epoch(1);
    runtime.post(store, EpochStamp::Epoch(1), ToyInput::WorkAsync(7));
    runtime.barrier();
    for frame in 0..FRAMES {
        runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Deposit(1));
        if frame % (FRAMES / CALLS) == 0 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::WorkRequest(frame)); }
        if frame == FRAMES / 3 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Note(OFFLINE)); }
        if frame == 2 * FRAMES / 3 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Note(ONLINE)); }
    }
    runtime.post(store, EpochStamp::Epoch(1), ToyInput::Release);
    runtime.barrier();
    assert!(runtime.history(store).iter().any(|entry| entry.input_is::<ToyInput>(|input| *input == ToyInput::Result(14)) && !entry.dropped), "the held answer of the store arrives when it is released");
    runtime.end_epoch(1, Cause::SessionReset);
    runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Deposit(1));
    runtime.start_epoch(2);
    runtime.barrier();
    assert_eq!(runtime.pending(), 0, "no input is left behind");
    let kept: Vec<ToyInput> = runtime.history(feed).into_iter().filter(|entry| !entry.dropped).filter_map(|entry| entry.typed?.typed::<ToyInput>()).collect();
    assert_eq!(kept.iter().filter(|input| **input == ToyInput::Deposit(1)).count(), FRAMES as usize, "every frame before the end of the epoch reached the owner, the late one of the old epoch did not");
    assert_eq!(kept.iter().filter(|input| matches!(input, ToyInput::Result(_))).count(), CALLS as usize, "every accepted call has its outcome");
    let signals: Vec<ToyInput> = kept.into_iter().filter(|input| matches!(input, ToyInput::Note(OFFLINE | ONLINE) | ToyInput::EpochStarted(_) | ToyInput::EpochEnded(..))).collect();
    assert_eq!(signals, [ToyInput::EpochStarted(1), ToyInput::Note(OFFLINE), ToyInput::Note(ONLINE), ToyInput::EpochEnded(1, Cause::SessionReset), ToyInput::EpochStarted(2)], "the signals and the epochs keep their order");
}
