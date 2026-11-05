use super::ref_runtime::{Command, Observed, RefOwner};
use crate::runtime::toys_tests::*;
use crate::runtime::{Cause, DetScheduler, EpochStamp};

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn command(state: &mut u64) -> Command {
    let epoch = splitmix(state) % 3 + 1;
    match splitmix(state) % 4 {
        0 => Command::Start(epoch),
        1 => Command::End(epoch),
        2 => Command::Deposit { epoch: Some(epoch), amount: (splitmix(state) % 9) as u32 + 1 },
        _ => Command::Deposit { epoch: None, amount: 1 },
    }
}

#[test]
fn rt_ref_the_queue_of_an_owner_with_epochs_follows_the_table_of_admission() {
    for seed in 0..300u64 {
        let mut state = seed;
        let mut det = DetScheduler::new(seed);
        let toy = det.add::<Toy>("toy", ToyState::default(), ToyRunner::default());
        let mut model = RefOwner::default();
        for step in 0..40 {
            let next = command(&mut state);
            match next {
                Command::Start(epoch) => det.start_epoch(epoch),
                Command::End(epoch) => det.end_epoch(epoch, Cause::SessionReset),
                Command::Deposit { epoch, amount } => det.post(toy, epoch.map_or(EpochStamp::Control, EpochStamp::Epoch), ToyInput::Deposit(amount)),
            }
            det.run_until_quiet(10).unwrap();
            let real = det.state(toy).unwrap();
            let seen = Observed { total: real.total, ended: real.ended.iter().map(|(epoch, _)| *epoch).collect(), dropped: det.dropped(toy) };
            assert_eq!(seen, model.apply(next), "seed {seed} step {step} after {next:?}");
        }
    }
}
