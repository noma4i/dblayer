use std::time::Duration;

use super::toys_tests::*;
use super::*;

const WAIT: Duration = Duration::from_secs(5);

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
    assert!(runtime.barrier(WAIT));
    let began = std::time::Instant::now();
    for frame in 0..FRAMES {
        runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Deposit(1));
        if frame % (FRAMES / CALLS) == 0 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::WorkRequest(frame)); }
        if frame == FRAMES / 3 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Note(OFFLINE)); }
        if frame == 2 * FRAMES / 3 { runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Note(ONLINE)); }
    }
    assert!(began.elapsed() < WAIT, "a held Written of the store never blocks a sender");
    runtime.post(store, EpochStamp::Epoch(1), ToyInput::Release);
    assert!(runtime.barrier(WAIT));
    assert!(runtime.history(store).iter().any(|entry| entry.input == "Result(14)" && !entry.dropped), "the held answer of the store arrives when it is released");
    runtime.end_epoch(1, Cause::SessionReset);
    runtime.post(feed, EpochStamp::Epoch(1), ToyInput::Deposit(1));
    runtime.start_epoch(2);
    assert!(runtime.barrier(WAIT));
    assert_eq!(runtime.pending(), 0, "no input is left behind");
    let kept: Vec<String> = runtime.history(feed).into_iter().filter(|entry| !entry.dropped).map(|entry| entry.input).collect();
    assert_eq!(kept.iter().filter(|input| input.as_str() == "Deposit(1)").count(), FRAMES as usize, "every frame before the end of the epoch reached the owner, the late one of the old epoch did not");
    assert_eq!(kept.iter().filter(|input| input.starts_with("Result(")).count(), CALLS as usize, "every accepted call has its outcome");
    let signals: Vec<&str> = kept.iter().map(String::as_str).filter(|input| *input == format!("Note({OFFLINE})") || *input == format!("Note({ONLINE})") || input.starts_with("Epoch")).collect();
    assert_eq!(signals, ["EpochStarted(1)".to_string(), format!("Note({OFFLINE})"), format!("Note({ONLINE})"), "EpochEnded(1, SessionReset)".to_string(), "EpochStarted(2)".to_string()], "the signals and the epochs keep their order");
}
