use std::collections::BTreeSet;

use super::deliveries::{self as d, Deliveries, DeliveriesState, DeliveryEffect, DeliveryInput, DeliveryPeers, Kind, StreamEffect, View};
use super::feed::Feed;
use super::windows::{WindowKey, Windows};
use crate::owners::contract::{CommittedBroadcast, Models};
use crate::owners::write::transfer::Shown;
use crate::runtime::{Address, Effect, StateOwner};
use crate::store::ddl::table_name;

const EPOCH: u64 = 1;
const STREAM: u64 = 7;

fn blob_view() -> View {
    View { kind: Kind::Blob, tables: vec![table_name("Chat")], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: crate::store::transfer_key("Chat", "r1", "photo"), query: None, model: "Chat".into() }
}

fn opened() -> DeliveriesState {
    let mut state = DeliveriesState::new(EPOCH, DeliveryPeers { windows: Address::<Windows>::new(0), feed: Address::<Feed>::new(1) });
    let effects = Deliveries::step(&mut state, DeliveryInput::Open { stream: STREAM, view: blob_view(), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Run(DeliveryEffect::Stream { effect: StreamEffect::StatusInterest { plus: true, key: Some(key) }, .. }) if key == "Chat:r1:photo")));
    state
}

fn reads(effects: Vec<Effect<Deliveries>>) -> Vec<u64> {
    effects.into_iter().filter_map(|effect| match effect {
        Effect::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { read_id, .. }, .. }) => Some(read_id),
        _ => None,
    }).collect()
}

fn commit(rows: &[&str]) -> DeliveryInput {
    let table = table_name("Chat");
    DeliveryInput::Committed(CommittedBroadcast { revision: 5, position: 0, tables: vec![table.clone()], rows: Some(rows.iter().map(|row| (table.clone(), row.to_string())).collect::<BTreeSet<_>>()), models: Models::Set(BTreeSet::from(["Chat".to_string()])) }.for_deliveries())
}

fn statuses(state: &DeliveriesState) -> Vec<(i64, d::BlobStatus)> {
    state.journal(STREAM).into_iter().filter_map(|value| match value { d::Value::Status { revision, blob } => Some((revision, blob)), _ => None }).collect()
}

/// xfer_status_local_url, xfer_status_processing (P-XFER-23, P-XFER-26): the first value waits for the read of the row, the commit of the row of the key reads it again and a changed copy URL is a new value of the next revision.
#[test]
fn a_blob_stream_shows_the_transfer_with_the_copy_url_and_the_processing_of_its_row() {
    let mut state = opened();
    let asked = reads(Deliveries::step(&mut state, DeliveryInput::TransferStatus { stream: STREAM, revision: 3, blob: d::BlobStatus::of(&Shown::Queued) }));
    assert_eq!(asked.len(), 1);
    assert!(statuses(&state).is_empty(), "no value before the row was read");
    Deliveries::step(&mut state, DeliveryInput::ReadDone { stream: STREAM, read_id: asked[0], revision: 4, data: d::ReadData::Upload { wait_reason: None, local_url: None, processing: None } });
    assert_eq!(statuses(&state), [(4, d::BlobStatus::of(&Shown::Queued))], "the value carries the revision of the row read that completed it");

    let asked = reads(Deliveries::step(&mut state, commit(&["r1"])));
    assert_eq!(asked.len(), 1);
    Deliveries::step(&mut state, DeliveryInput::ReadDone { stream: STREAM, read_id: asked[0], revision: 5, data: d::ReadData::Upload { wait_reason: None, local_url: Some("file:copy".into()), processing: None } });
    let shown = statuses(&state);
    assert_eq!(shown.len(), 1, "the journal keeps the latest status only");
    assert_eq!((shown[0].0, shown[0].1.local_url.as_deref(), shown[0].1.state.as_str()), (5, Some("file:copy"), "queued"), "a re-read row gives the value its commit revision");

    let asked = reads(Deliveries::step(&mut state, DeliveryInput::TransferStatus { stream: STREAM, revision: 6, blob: d::BlobStatus::of(&Shown::Uploading { progress: 40 }) }));
    assert!(asked.is_empty(), "a new state of the transfer reads no row");
    let shown = statuses(&state);
    assert_eq!((shown[0].0, shown[0].1.progress, shown[0].1.local_url.as_deref()), (6, 40, Some("file:copy")), "a new transfer status gives the value its own revision");
}

#[test]
fn xfer_status_quiet_on_other_rows() {
    let mut state = opened();
    assert!(reads(Deliveries::step(&mut state, commit(&["r2"]))).is_empty());
    assert_eq!(state.streams_examined(), 0);
    assert_eq!(reads(Deliveries::step(&mut state, commit(&["r1"]))).len(), 1);
    assert_eq!(state.streams_examined(), 1);
}
