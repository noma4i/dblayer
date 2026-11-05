//! Runner of the effects of `Deliveries`: a read of the snapshot runs in the read pool and comes back as `ReadDone` or `ReadFailed`;
//! the answer of a `Pull` settles its outcome. The effect carries the view of the stream, the runner keeps no registry.

use std::sync::Arc;

use tokio::runtime::Handle;

use super::deliveries::{Deliveries, DeliveryEffect, DeliveryInput, PullAnswer, StreamEffect, StreamId, View};
use super::ports::Rest;
use super::snapshots::Snapshots;
use crate::runtime::{Back, EffectRunner, EpochStamp, Outcomes};
use crate::task::{spawn_blocking_on, Carry};

pub struct DeliveryIo {
    snapshots: Arc<dyn Snapshots>,
    answers: Outcomes<PullAnswer>,
    handle: Handle,
    rest: Rest<DeliveryEffect>,
}

impl DeliveryIo {
    pub fn new(snapshots: Arc<dyn Snapshots>, answers: Outcomes<PullAnswer>, handle: Handle, rest: Rest<DeliveryEffect>) -> Self { Self { snapshots, answers, handle, rest } }

    fn read(&self, stamp: EpochStamp, stream: StreamId, read_id: u64, revision: i64, view: View, back: &Back<Deliveries>) {
        let (snapshots, back) = (self.snapshots.clone(), back.clone());
        spawn_blocking_on(Carry::Detached, &self.handle, move || {
            let answer = match snapshots.read(&view, revision) {
                Ok((seen, data)) => DeliveryInput::ReadDone { stream, read_id, revision: seen, data },
                Err(error) => DeliveryInput::ReadFailed { stream, read_id, revision, error },
            };
            back.reply(stamp, answer);
        });
    }
}

impl EffectRunner<Deliveries> for DeliveryIo {
    fn run(&mut self, stamp: EpochStamp, effect: DeliveryEffect, back: &Back<Deliveries>) {
        match effect {
            DeliveryEffect::Stream { stream, effect: StreamEffect::Read { read_id, revision, view } } => self.read(stamp, stream, read_id, revision, view, back),
            DeliveryEffect::Stream { effect: StreamEffect::Answer { outcome, answer }, .. } => {
                self.answers.settle(outcome, answer);
            }
            other => (self.rest)(stamp, other),
        }
    }
}
