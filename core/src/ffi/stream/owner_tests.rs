use std::future::Future;
use std::sync::{Arc, Mutex};

use super::frame::Frame;
use super::{Request, Stream, StreamDelivery, StreamTable, StreamValue, ValueSink, WireModelEvent};

#[derive(Default)]
struct Log(Mutex<Vec<StreamDelivery>>);

impl ValueSink for Log {
    fn delivered(&self, delivery: StreamDelivery) { self.0.lock().unwrap().push(delivery); }
}

fn event(id: &str) -> StreamValue {
    StreamValue::Event { event: WireModelEvent { area: "CHAT".into(), kind: "UPDATED".into(), id: id.into(), before: None, after: None } }
}

#[test]
fn a_missed_delivery_advances_the_sequence_by_its_count() {
    let log = Arc::new(Log::default());
    let request = Request { model: "Chat".into(), relation: None, params_json: "{}".into(), entity: None };
    let stream = Stream::new(1, 0, Arc::default(), &Arc::<StreamTable>::default(), log.clone(), request);
    for id in ["a", "b", "c"] { stream.push(event(id)); }
    stream.push(StreamValue::Missed { count: 44 });
    for id in ["d", "e"] { stream.push(event(id)); }
    let seqs: Vec<u64> = log.0.lock().unwrap().iter().map(|delivery| delivery.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 47, 48, 49], "events sent 3 + 44 + 2");
}

/// Readiness of an entity stream whose load landed its row at revision 2: `loaded()` stays pending until the frame of revision 2 is queued and then returns the sequence number of that frame.
#[tokio::test]
async fn an_entity_stream_is_not_loaded_before_the_frame_of_the_row_that_the_load_landed() {
    let log = Arc::new(Log::default());
    let request = Request { model: "User".into(), relation: None, params_json: "{}".into(), entity: Some("u1".into()) };
    let stream = Stream::new(1, 0, Arc::default(), &Arc::<StreamTable>::default(), log.clone(), request);
    stream.fetches_entity();
    stream.expect_load();
    stream.rows(Frame::whole(1, Vec::new()));
    stream.entity_loaded(None, 2);

    let mut loaded = std::pin::pin!(stream.loaded());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(loaded.as_mut().poll(&mut cx).is_pending(), "loaded() returned before the frame of revision 2");

    stream.rows(Frame::whole(2, Vec::new()));
    let seq = loaded.await.unwrap();
    let frames: Vec<(u64, i64)> = log.0.lock().unwrap().iter().filter_map(|delivery| match &delivery.value {
        StreamValue::Snapshot { revision, .. } => Some((delivery.seq, *revision)),
        _ => None,
    }).collect();
    assert_eq!(frames.last().copied(), Some((seq, 2)), "loaded() returns the sequence number of the frame of revision 2: {frames:?}");
}
