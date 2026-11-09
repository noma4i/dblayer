//! The reference of the Transfer owner takes an item and holds one transfer for it.
use super::ref_transfer::{step, Check, Input, Item, Source, State, TransferKey};

#[test]
fn ref_transfer_accepts_an_item() {
    let key = TransferKey { model: "Moment".into(), row_id: "r1".into(), action: "upload".into() };
    let item = Item { key, source: Source::File { path: "/tmp/a".into() }, values: Vec::new(), check: Check { source_valid: true, action_declared: true, has_file_field: true, row_exists: true, has_effects: true } };
    let (state, effects) = step(&State::new(1, 10, 5), &Input::Accept { epoch: 0, now: 0, item, reply: 1 });
    assert!(!effects.is_empty() || !state.transfers.is_empty(), "the item changed nothing");
}
