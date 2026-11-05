use super::cell::Cell;
use super::toys_tests::*;
use super::*;

fn drain(cell: &mut Cell<Toy>) {
    let mut runner = ToyRunner::default();
    let back = Back::<Toy>::new(|_, _| {});
    while cell.run_one("toy", &mut runner, &back, false).is_some() {}
}

#[test]
fn due_entries_are_handed_out_one_at_a_time_in_time_order() {
    let mut cell = Cell::<Toy>::new(ToyState::default());
    cell.start_epoch(1);
    cell.push(EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(30), 3));
    cell.push(EpochStamp::Epoch(1), ToyInput::ScheduleAt(2, Time(10), 1));
    cell.push(EpochStamp::Epoch(1), ToyInput::ScheduleAt(3, Time(20), 2));
    drain(&mut cell);
    assert_eq!(cell.next_due(), Some(Time(10)));
    cell.set_now(Time(25));
    assert!(cell.fire_next_due());
    drain(&mut cell);
    assert_eq!(cell.state.fired, vec![1], "the second due entry waits for its own Fire");
    assert!(cell.fire_next_due());
    drain(&mut cell);
    assert_eq!(cell.state.fired, vec![1, 2]);
    assert!(!cell.fire_next_due());
    assert_eq!(cell.next_due(), Some(Time(30)));
}

#[test]
fn schedule_with_same_id_replaces_the_entry() {
    let mut cell = Cell::<Toy>::new(ToyState::default());
    cell.start_epoch(1);
    cell.push(EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 1));
    cell.push(EpochStamp::Epoch(1), ToyInput::ScheduleAt(1, Time(50), 2));
    drain(&mut cell);
    cell.set_now(Time(20));
    assert!(!cell.fire_next_due());
    cell.set_now(Time(50));
    assert!(cell.fire_next_due());
    drain(&mut cell);
    assert_eq!(cell.state.fired, vec![2]);
    assert_eq!(cell.next_due(), None);
}

#[test]
fn start_of_a_new_epoch_waits_behind_the_end_of_the_old_one() {
    let mut cell = Cell::<Toy>::new(ToyState::default());
    cell.start_epoch(1);
    cell.push(EpochStamp::Epoch(1), ToyInput::Deposit(2));
    cell.end_epoch(1, Cause::SessionReset);
    cell.start_epoch(2);
    cell.push(EpochStamp::Epoch(2), ToyInput::Deposit(3));
    cell.push(EpochStamp::Epoch(1), ToyInput::Deposit(100));
    drain(&mut cell);
    assert_eq!(cell.state.ended, vec![(1, Cause::SessionReset)]);
    assert_eq!(cell.state.total, 5, "the input of epoch 2 is taken, the late input of epoch 1 is dropped");
    assert_eq!(cell.dropped(), 1);
}
