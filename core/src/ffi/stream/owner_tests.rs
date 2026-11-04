use std::sync::{Arc, Mutex};

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
