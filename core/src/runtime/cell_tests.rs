use super::cell::Cell;
use super::testing::*;
use super::*;

fn drain(cell: &mut Cell<Toy>) {
    let mut runner = ToyRunner::default();
    while cell.run_one(&mut runner, false).is_some() {}
}

#[test]
fn tick_enqueues_due_inputs_in_time_order() {
    let mut cell = Cell::<Toy>::new(ToyState::default());
    cell.start_epoch(1);
    cell.push(Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(30), 3));
    cell.push(Stamp::Epoch(1), ToyInput::ScheduleAt(2, Time(10), 1));
    cell.push(Stamp::Epoch(1), ToyInput::ScheduleAt(3, Time(20), 2));
    drain(&mut cell);
    assert_eq!(cell.next_due(), Some(Time(10)));
    assert!(cell.tick(Time(25)));
    drain(&mut cell);
    assert_eq!(cell.state.fired, vec![1, 2]);
    assert_eq!(cell.next_due(), Some(Time(30)));
}

#[test]
fn schedule_with_same_id_replaces_the_entry() {
    let mut cell = Cell::<Toy>::new(ToyState::default());
    cell.start_epoch(1);
    cell.push(Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(10), 1));
    cell.push(Stamp::Epoch(1), ToyInput::ScheduleAt(1, Time(50), 2));
    drain(&mut cell);
    assert!(!cell.tick(Time(20)));
    assert!(cell.tick(Time(50)));
    drain(&mut cell);
    assert_eq!(cell.state.fired, vec![2]);
    assert_eq!(cell.next_due(), None);
}
