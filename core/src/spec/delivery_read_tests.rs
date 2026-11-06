use super::*;

fn last_mode() -> d::ReadMode { MODES.with(|modes| *modes.borrow().last().expect("the owner started a read")) }

#[test]
fn dlv_read_overflow() {
    let mut twin = live_rows();
    assert_eq!(twin.step(Input::Committed { revision: 2, touches: true }), [Effect::Read(2)]);
    assert_eq!(last_mode(), d::ReadMode::Changes { from_revision: 1, from_position: 0, limit: d::DELTA_LIMIT });
    let overflowed = twin.read_id();

    assert_eq!(twin.step(Input::ReadOverflow { revision: 2 }), [Effect::Read(2)]);
    assert_eq!(last_mode(), d::ReadMode::Snapshot);
    assert_eq!(twin.read_id(), overflowed + 1, "the read of the whole view has a new read_id");
    assert!(twin.read_all().is_empty(), "nothing is delivered before the Snapshot");

    twin.step(Input::ReadDone { revision: 2, rows: rows(&["r1", "r2"]) });
    let values = twin.read_all();
    assert!(matches!(values.as_slice(), [Val::Delta { revision: 2, size: 2, upserted, .. }] if *upserted == ids(&["r2"])), "{values:?}");
}
