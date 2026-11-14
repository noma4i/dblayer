//! Owner-level tests of the two-sided row selector without an action (D-259): the `version` round-trip and the R2 strays.

use super::outbox_harness::*;
use crate::owners::write::outbox::{self as o, OutboxInput};
use crate::owners::write::transfer::TransferInput;

fn select_rows(sends: &[TransferInput]) -> usize { sends.iter().filter(|send| matches!(send, TransferInput::SelectRow { .. })).count() }

/// `SelectedTransfer` of the current version: the Outbox stays, sends Transfer `SelectConfirmed{reply}`, answers the caller nothing.
#[test]
fn out_selector_transfer_confirmed() {
    let (mut twin, _id) = reach(St::Sending, &ready());
    let reply = 555;
    let (_, asked) = twin.selector(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply });
    let version = asked.iter().find_map(|send| match send { TransferInput::SelectRow { version, .. } => Some(*version), _ => None }).expect("the row question");
    let (effects, sends) = twin.selector(OutboxInput::SelectedTransfer { reply, transfer_id: 1, version });
    let confirmed = sends.iter().filter(|send| matches!(send, TransferInput::SelectConfirmed { .. })).count();
    assert_eq!((confirmed, answers(&effects, reply).len()), (1, 0));
}

/// An answer of a stale `version`: the Outbox resends `SelectRow` with the current version, acts on no stale choice, answers the caller nothing.
#[test]
fn out_selector_version() {
    let (mut twin, _id) = reach(St::Sending, &ready());
    let reply = 555;
    let (_, asked) = twin.selector(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply });
    let stale = asked.iter().find_map(|send| match send { TransferInput::SelectRow { version, .. } => Some(*version), _ => None }).expect("the row question");
    twin.submit("r", Policy::Rollback, false);
    let (effects, sends) = twin.selector(OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: "r".into(), action: "update".into(), kind: o::SelectKind::Retry, reply, version: stale });
    assert_eq!((select_rows(&sends), answers(&effects, reply).len()), (1, 0));
}

/// An operation of another row does not make the answer stale, and a settled selection leaves no version entry.
#[test]
fn out_selector_other_row_keeps_the_choice_and_leaves_no_entry() {
    let (mut twin, _id) = reach(St::Sending, &ready());
    let reply = 555;
    let (_, asked) = twin.selector(OutboxInput::Retry { model: "Moment".into(), row_id: "r".into(), action: None, reply });
    let version = asked.iter().find_map(|send| match send { TransferInput::SelectRow { version, .. } => Some(*version), _ => None }).expect("the row question");
    assert_eq!(twin.state().selection_versions.len(), 1, "the row of the selection in flight has an entry");
    twin.submit("other", Policy::Rollback, false);
    let (_, sends) = twin.selector(OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: "r".into(), action: "update".into(), kind: o::SelectKind::Retry, reply, version });
    assert_eq!((select_rows(&sends), twin.state().selection_versions.len()), (0, 0), "the choice stands and no entry stays");
}

/// A selector answer whose reply waits for no choice (R2): nothing.
#[test]
fn out_selector_stray() {
    for input in [
        OutboxInput::SelectedNone { reply: 777, version: 0 },
        OutboxInput::SelectedOutbox { model: "Moment".into(), row_id: "r".into(), action: "update".into(), kind: o::SelectKind::Retry, reply: 777, version: 0 },
        OutboxInput::SelectedTransfer { reply: 777, transfer_id: 1, version: 0 },
    ] {
        let (mut twin, _id) = reach(St::Sending, &ready());
        let (effects, sends) = twin.selector(input);
        assert_eq!((sends.len(), answers(&effects, 777).len()), (0, 0));
    }
}
