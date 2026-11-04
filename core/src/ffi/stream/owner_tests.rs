use std::sync::Arc;

use super::frame::Frame;
use super::{Request, Stream, StreamDelivery, StreamTable, StreamValue, ValueSink};

struct Quiet;

impl ValueSink for Quiet {
    fn available(&self) {}
}

fn drained(stream: &Stream) -> Vec<StreamDelivery> { std::iter::from_fn(|| stream.read()).collect() }

fn settled(delivery: &StreamDelivery) -> bool {
    match &delivery.value {
        StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } | StreamValue::Count { state, .. } => state.settled,
        _ => false,
    }
}

/// Requirement: the state of an entity stream whose load landed its row at revision 2 is not settled before the frame of revision 2. Observed: `settled` of each value the reader takes. Breaks when `landed_at` leaves the gate in `Facts::show`.
#[test]
fn an_entity_stream_is_not_settled_before_the_frame_of_the_row_that_the_load_landed() {
    let request = Request { model: "User".into(), relation: None, params_json: "{}".into(), entity: Some("u1".into()) };
    let stream = Stream::new(1, 0, Arc::default(), &Arc::<StreamTable>::default(), Arc::new(Quiet), request);
    stream.fetches_entity();
    stream.expect_load();
    stream.rows(Frame::whole(1, Vec::new()));
    stream.entity_loaded(None, 2);
    let before = drained(&stream);
    assert!(!before.is_empty() && !before.iter().any(settled), "a value was settled before the frame of revision 2: {before:?}");

    stream.rows(Frame::whole(2, Vec::new()));
    let after = drained(&stream);
    assert!(matches!(after.as_slice(), [delivery @ StreamDelivery { value: StreamValue::Snapshot { revision: 2, .. }, .. }] if settled(delivery)), "the frame of revision 2 carries the settled state: {after:?}");
}
