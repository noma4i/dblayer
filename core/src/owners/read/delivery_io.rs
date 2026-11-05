//! Runner of the effects of `Deliveries`: a read of the snapshot runs in the read pool and comes back as `ReadDone` or
//! `ReadFailed`; `Wake` tells the sink of the platform that values wait. The platform registers a stream before it opens it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::runtime::Handle;

use super::deliveries::{DeliveryEffect, DeliveryInput, Deliveries, StreamEffect, StreamId, View};
use super::ports::Rest;
use super::snapshots::Snapshots;
use crate::ffi::ValueSink;
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::{spawn_blocking_on, Carry};

#[derive(Clone, Default)]
pub struct StreamTable {
    streams: Arc<Mutex<HashMap<StreamId, (View, Arc<dyn ValueSink>)>>>,
}

impl StreamTable {
    pub fn register(&self, stream: StreamId, view: View, sink: Arc<dyn ValueSink>) { self.streams.lock().unwrap_or_else(PoisonError::into_inner).insert(stream, (view, sink)); }

    pub fn remove(&self, stream: StreamId) { self.streams.lock().unwrap_or_else(PoisonError::into_inner).remove(&stream); }

    fn get(&self, stream: StreamId) -> Option<(View, Arc<dyn ValueSink>)> { self.streams.lock().unwrap_or_else(PoisonError::into_inner).get(&stream).cloned() }
}

pub struct DeliveryIo {
    snapshots: Arc<dyn Snapshots>,
    streams: StreamTable,
    handle: Handle,
    rest: Rest<DeliveryEffect>,
}

impl DeliveryIo {
    pub fn new(snapshots: Arc<dyn Snapshots>, streams: StreamTable, handle: Handle, rest: Rest<DeliveryEffect>) -> Self { Self { snapshots, streams, handle, rest } }

    fn read(&self, stamp: EpochStamp, stream: StreamId, read_id: u64, revision: i64, back: &Back<Deliveries>) {
        let (snapshots, table, back) = (self.snapshots.clone(), self.streams.clone(), back.clone());
        spawn_blocking_on(Carry::Detached, &self.handle, move || {
            let answer = match table.get(stream) {
                None => DeliveryInput::ReadFailed { stream, read_id, revision, error: "the stream is not registered".to_string() },
                Some((view, _)) => match snapshots.read(&view, revision) {
                    Ok((seen, data)) => DeliveryInput::ReadDone { stream, read_id, revision: seen, data },
                    Err(error) => DeliveryInput::ReadFailed { stream, read_id, revision, error },
                },
            };
            back.reply(stamp, answer);
        });
    }
}

impl EffectRunner<Deliveries> for DeliveryIo {
    fn run(&mut self, stamp: EpochStamp, effect: DeliveryEffect, back: &Back<Deliveries>) {
        match effect {
            DeliveryEffect::Stream { stream, effect: StreamEffect::Read { read_id, revision } } => self.read(stamp, stream, read_id, revision, back),
            DeliveryEffect::Stream { stream, effect: StreamEffect::Wake } => {
                if let Some((_, sink)) = self.streams.get(stream) {
                    crate::db::lock::effect(|| sink.available());
                }
            }
            other => (self.rest)(stamp, other),
        }
    }
}

impl Drop for StreamTable {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("StreamTable drop"); }
}
