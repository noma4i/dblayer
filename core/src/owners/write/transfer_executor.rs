//! Executor of the effects of `Transfers`: the copy of the source in chunks of 1 MiB outside the writer,
//! the steps of the file (PUT, multipart, complete, abort) with `NetworkPurpose::Transfer`, the answers of calls, the view of `uploads[action]`, the platform job.
//! The result of an effect is an input to `Transfers` through `Back` and nothing else. Store batches, inputs to Outbox and the retry timer
//! are `Effect::Send` and `Effect::Schedule` of the owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use super::transfer::{file_id, CachedBlob, ShowSink, Shown, Source, Steps, TransferAnswer, TransferEffect, TransferId, TransferInput, Transfers};
use super::wire::{Flights, Running, Wire};
use crate::files::{CopiedSource, FileDirectories};
use crate::owners::contract::{Drained, ReplyId};
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};
use crate::transport::{CancelToken, GraphQLRequest, NetworkPurpose, PutRequest, PutResponse, Response, RetryClass, Transport, TransportError};

/// The requests of one step. Every request goes out with `NetworkPurpose::Transfer` and stops with the step.
pub struct Link {
    transport: Arc<dyn Transport>,
    cancel: CancelToken,
    gate: Arc<crate::runtime::EpochGate>,
    epoch: Option<Epoch>,
}

impl Drop for Link {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("Link"); }
}

impl Link {
    pub async fn mutate(&self, mut request: GraphQLRequest) -> Result<Response, TransportError> {
        request.purpose = NetworkPurpose::Transfer;
        crate::runtime::send_in_epoch(&self.gate, self.epoch, self.transport.mutate(request)).await
    }

    pub async fn put(&self, mut request: PutRequest) -> Result<PutResponse, TransportError> {
        request.cancel = self.cancel.clone();
        crate::runtime::send_in_epoch(&self.gate, self.epoch, self.transport.put(request)).await
    }
}

/// What a step brings back: `upload_id` of a multipart upload the step created.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepDone {
    pub upload_id: Option<String>,
    pub etag: Option<String>,
}

/// Encoding of the requests of a step and decoding of the answers. It is stateless: everything it needs comes by value from the effect.
#[async_trait]
pub(crate) trait StepCodec: Send + Sync + 'static {
    /// Step `part` (`None` = a single PUT). The first step of a multipart upload creates it and returns `upload_id`.
    async fn step(&self, link: &Link, key: &str, part: Option<u32>, upload_id: Option<&str>, source: &Source, steps: &Steps) -> Result<StepDone, TransportError>;
    /// The answer of `complete` carries `signed_id` of the file on the server.
    async fn complete(&self, link: &Link, key: &str, upload_id: Option<&str>, parts: &[(u32, Option<String>)]) -> Result<Option<String>, TransportError>;
    async fn discard_upload(&self, link: &Link, key: &str, upload_id: Option<&str>) -> Result<(), TransportError>;
}

/// What Life needs to install Transfer and Rounds: the codec of the steps, the platform job of the round and the sink of `uploads[action]`.
pub(crate) struct TransferPort {
    pub codec: Arc<dyn StepCodec>,
    pub jobs: Arc<dyn super::rounds_executor::RoundJobs>,
    pub shows: ShowSink,
    /// `FileSlots`: transfers in Uploading and Completing at the same time.
    pub slots: u32,
}

type Call<T> = Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send>>;

/// What a step asks of the codec.
struct StepAsk {
    key: String,
    part: Option<u32>,
    upload_id: Option<String>,
    source: Source,
    steps: Steps,
}

/// What the owner asks of Lifecycle and Network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LifeSignal {
    Started(Epoch),
    SnapshotRequest(Epoch),
    RestoreClassified(Epoch),
}

pub(crate) struct TransferExecutor {
    wire: Wire,
    codec: Arc<dyn StepCodec>,
    files: Arc<FileDirectories>,
    settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    shows: ShowSink,
    drained: Arc<dyn Fn(Drained) + Send + Sync>,
    signal: Arc<dyn Fn(LifeSignal) + Send + Sync>,
    copies: Flights<TransferId>,
    steps: Flights<TransferId>,
    /// The cache of blobs: what is known of a server blob for the content of a copy.
    cache: Arc<dyn Fn(&str) -> CachedBlob + Send + Sync>,
}

/// Where the executor reports to.
pub struct TransferSinks {
    pub settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    pub shows: ShowSink,
    pub drained: Arc<dyn Fn(Drained) + Send + Sync>,
    pub signal: Arc<dyn Fn(LifeSignal) + Send + Sync>,
}

impl TransferExecutor {
    pub fn new(wire: Wire, codec: Arc<dyn StepCodec>, files: Arc<FileDirectories>, sinks: TransferSinks, cache: Arc<dyn Fn(&str) -> CachedBlob + Send + Sync>) -> Self {
        let TransferSinks { settle, shows, drained, signal } = sinks;
        Self { wire, codec, files, settle, shows, drained, signal, copies: Flights::default(), steps: Flights::default(), cache }
    }

    fn copy(&self, stamp: EpochStamp, back: &Back<Transfers>, transfer_id: TransferId, attempt_id: u64, source: Source) {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (files, copies, back, flag, id, cache) = (self.files.clone(), self.copies.clone(), back.clone(), cancelled.clone(), file_id(transfer_id), self.cache.clone());
        let running = self.wire.tasks.start_blocking(Box::new(move || {
            let result = files.copy_source(&id, std::path::Path::new(&source.path));
            let input = if flag.load(Ordering::SeqCst) {
                let _ = files.discard(&[id]);
                None
            } else {
                Some(match result {
                    Ok(CopiedSource::Ready { size, sha256, location, data }) => match keep_small_copy(&files, &id, location, &data) {
                        Ok(location) => TransferInput::CopyDone { transfer_id, attempt_id, size, cached: cache(&sha256), sha256, place: location },
                        Err(error) => TransferInput::CopyFailed { transfer_id, attempt_id, error: error.to_string() },
                    },
                    Ok(CopiedSource::Unreadable(error)) => TransferInput::CopyFailed { transfer_id, attempt_id, error },
                    Err(error) => TransferInput::CopyFailed { transfer_id, attempt_id, error: error.to_string() },
                })
            };
            copies.done(&transfer_id);
            if let Some(input) = input {
                back.reply(stamp, input);
            }
        }));
        self.copies.put(transfer_id, Running::new(move || {
            cancelled.store(true, Ordering::SeqCst);
            running.cancel();
        }));
    }

    /// The copy of the library when it exists: the original file may be gone after the call of `upload`.
    fn local(&self, transfer_id: TransferId, source: Source) -> Source {
        let id = file_id(transfer_id);
        let copy = self.files.path(&id).filter(|path| path.exists()).or_else(|| Some(self.files.cache_target(&id, "source")).filter(|path| path.exists()));
        match copy {
            Some(path) => Source { path: path.to_string_lossy().into_owned(), ..source },
            None => source,
        }
    }

    fn link(&self, stamp: EpochStamp) -> Option<Link> {
        let epoch = match stamp {
            EpochStamp::Epoch(epoch) => Some(epoch),
            EpochStamp::Control => None,
        };
        if epoch.is_some_and(|epoch| !self.wire.gate.admits(epoch)) { return None; }
        Some(Link { transport: self.wire.transport.clone(), cancel: CancelToken::new(), gate: self.wire.gate.clone(), epoch })
    }

    /// Runs `call` as the task of `transfer_id`: its result becomes `input_of(result)` for the owner, unless the task is cancelled first.
    fn task<T: Send + 'static>(
        &self,
        stamp: EpochStamp,
        back: &Back<Transfers>,
        transfer_id: TransferId,
        no_link: TransferInput,
        call: impl FnOnce(Arc<dyn StepCodec>, Link) -> Call<T> + Send + 'static,
        input_of: impl FnOnce(Result<T, TransportError>) -> TransferInput + Send + 'static,
    ) {
        let Some(link) = self.link(stamp) else {
            back.reply(stamp, no_link);
            return;
        };
        let cancel = link.cancel.clone();
        let (codec, steps, back) = (self.codec.clone(), self.steps.clone(), back.clone());
        let running = self.wire.tasks.start(Box::pin(async move {
            let result = call(codec, link).await;
            steps.done(&transfer_id);
            back.reply(stamp, input_of(result));
        }));
        self.steps.put(transfer_id, Running::new(move || {
            cancel.cancel();
            running.cancel();
        }));
    }

    fn step(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), ask: StepAsk) {
        let StepAsk { key, part, upload_id, source, steps } = ask;
        let source = self.local(transfer_id, source);
        self.task(
            stamp,
            back,
            transfer_id,
            TransferInput::StepError { transfer_id, attempt_id, temporary: true },
            move |codec, link| Box::pin(async move { codec.step(&link, &key, part, upload_id.as_deref(), &source, &steps).await }),
            move |result| match result {
                Ok(done) => TransferInput::StepOk { transfer_id, attempt_id, part, upload_id: done.upload_id, etag: done.etag },
                Err(error) => TransferInput::StepError { transfer_id, attempt_id, temporary: temporary(&error) },
            },
        );
    }

    fn complete(&self, stamp: EpochStamp, back: &Back<Transfers>, (transfer_id, attempt_id): (TransferId, u64), key: String, upload_id: Option<String>, parts: Vec<(u32, Option<String>)>) {
        self.task(
            stamp,
            back,
            transfer_id,
            TransferInput::CompleteError { transfer_id, attempt_id, temporary: true },
            move |codec, link| Box::pin(async move { codec.complete(&link, &key, upload_id.as_deref(), &parts).await }),
            move |result| match result {
                Ok(signed_id) => TransferInput::Completed { transfer_id, attempt_id, signed_id },
                Err(error) => TransferInput::CompleteError { transfer_id, attempt_id, temporary: temporary(&error) },
            },
        );
    }

    /// The owner has no input for the end of an abort: the result is not reported.
    fn drop_upload(&self, key: String, upload_id: String) {
        let Some(link) = self.link(EpochStamp::Control) else { return };
        let codec = self.codec.clone();
        let _ = self.wire.tasks.start(Box::pin(async move {
            let _ = codec.discard_upload(&link, &key, Some(&upload_id)).await;
        }));
    }

    fn abort_orphan(&self, stamp: EpochStamp, back: &Back<Transfers>, key: String, upload_id: String) {
        let Some(link) = self.link(stamp) else {
            back.reply(stamp, TransferInput::AbortDone { upload_id, answered: false });
            return;
        };
        let (codec, back) = (self.codec.clone(), back.clone());
        let _ = self.wire.tasks.start(Box::pin(async move {
            let answered = match codec.discard_upload(&link, &key, Some(&upload_id)).await {
                Ok(()) => true,
                Err(error) => !temporary(&error),
            };
            back.reply(stamp, TransferInput::AbortDone { upload_id, answered });
        }));
    }

    fn delete_file(&self, path: String) {
        let files = self.files.clone();
        let _ = self.wire.tasks.start_blocking(Box::new(move || {
            let _ = files.discard(&[path]);
        }));
    }
}

/// A copy below the limit of the table also lies in the directory (or in the cache without one): the bytes of the steps come from one place.
fn keep_small_copy(files: &FileDirectories, id: &str, location: crate::files::Location, data: &[u8]) -> std::io::Result<crate::files::Location> {
    if location == crate::files::Location::Directory { return Ok(location); }
    if files.has_directory() {
        files.write(id, data)?;
        Ok(crate::files::Location::Directory)
    } else {
        files.materialize(id, "source", data)?;
        Ok(crate::files::Location::Table)
    }
}

pub(crate) fn temporary(error: &TransportError) -> bool { RetryClass::of_error(error) != RetryClass::Fatal }

impl EffectRunner<Transfers> for TransferExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: TransferEffect, back: &Back<Transfers>) {
        match effect {
            TransferEffect::Copy { transfer_id, attempt_id, source, .. } => self.copy(stamp, back, transfer_id, attempt_id, source),
            TransferEffect::CopyCancel { transfer_id } => self.copies.cancel(&transfer_id),
            TransferEffect::Step { transfer_id, attempt_id, key, part, purpose, upload_id, source, steps, .. } => {
                debug_assert_eq!(purpose, NetworkPurpose::Transfer);
                self.step(stamp, back, (transfer_id, attempt_id), StepAsk { key, part, upload_id, source, steps });
            }
            TransferEffect::Complete { transfer_id, attempt_id, key, upload_id, parts, .. } => self.complete(stamp, back, (transfer_id, attempt_id), key, upload_id, parts),
            TransferEffect::Abort { key, upload_id, .. } => self.drop_upload(key, upload_id),
            TransferEffect::AbortOrphan { key, upload_id } => self.abort_orphan(stamp, back, key, upload_id),
            TransferEffect::CancelStep { transfer_id } => self.steps.cancel(&transfer_id),
            TransferEffect::DeleteFile { path } => self.delete_file(path),
            TransferEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            TransferEffect::Shows { key, shown } => (self.shows)(&key, shown),
            TransferEffect::Refused { key, reason } => {
                if reason.is_some() {
                    (self.shows)(&key, Shown::Failed { error: "refused".into() });
                }
            }
            TransferEffect::Status { .. } => {}
            TransferEffect::Started { epoch } => (self.signal)(LifeSignal::Started(epoch)),
            TransferEffect::SnapshotRequest { epoch } => (self.signal)(LifeSignal::SnapshotRequest(epoch)),
            TransferEffect::RestoreClassified { epoch } => (self.signal)(LifeSignal::RestoreClassified(epoch)),
            TransferEffect::Drained(drained) => (self.drained)(drained),
        }
    }
}
