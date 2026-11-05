//! Executor of the effects of `Transfers` (`design.md` F3): the copy of the source in chunks of 1 MiB outside the writer,
//! the steps of the file (PUT, multipart, complete, abort) with `NetworkPurpose::Transfer`, the answers of calls, the view of `uploads[action]`, the platform job.
//! The result of an effect is an input to `Transfers` through `Back` and nothing else. Store batches, inputs to Outbox and the retry timer
//! are `Effect::Send` and `Effect::Schedule` of the owner.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use super::transfer::{file_id, Key, RoundId, Shown, Source, TransferAnswer, TransferEffect, TransferInput, Transfers};
use super::wire::{Flights, Running, Wire};
use crate::files::{CopiedSource, FileDirectories};
use crate::lifecycle::Admitted;
use crate::owners::contract::ReplyId;
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};
use crate::transport::{CancelToken, GraphQLRequest, NetworkPurpose, PutRequest, PutResponse, Response, RetryClass, Transport, TransportError};

/// The requests of one step. Every request goes out with `NetworkPurpose::Transfer` and stops with the step.
pub struct Link {
    transport: Arc<dyn Transport>,
    admitted: Admitted,
    cancel: CancelToken,
}

impl Drop for Link {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Link"); }
}

impl Link {
    pub async fn mutate(&self, mut request: GraphQLRequest) -> Result<Response, TransportError> {
        request.purpose = NetworkPurpose::Transfer;
        self.transport.mutate(&self.admitted, request).await
    }

    pub async fn query(&self, mut request: GraphQLRequest) -> Result<Response, TransportError> {
        request.purpose = NetworkPurpose::Transfer;
        self.transport.query(&self.admitted, request).await
    }

    pub async fn put(&self, mut request: PutRequest) -> Result<PutResponse, TransportError> {
        request.cancel = self.cancel.clone();
        self.transport.put(&self.admitted, request).await
    }
}

/// What a step brings back: `upload_id` of a multipart upload the step created.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepDone {
    pub upload_id: Option<String>,
}

/// Encoding of the requests of a step and decoding of the answers. It is stateless: everything it needs comes by value from the effect.
#[async_trait]
pub(crate) trait StepCodec: Send + Sync + 'static {
    /// Step `part` (`None` = a single PUT). The first step of a multipart upload creates it and returns `upload_id`.
    async fn step(&self, link: &Link, key: &str, part: Option<u32>, upload_id: Option<&str>, source: &Source) -> Result<StepDone, TransportError>;
    async fn complete(&self, link: &Link, key: &str, upload_id: Option<&str>) -> Result<(), TransportError>;
    async fn discard_upload(&self, link: &Link, key: &str, upload_id: Option<&str>) -> Result<(), TransportError>;
}

/// The platform side of the round (the background job of Android).
pub trait RoundJobs: Send + Sync + 'static {
    fn schedule_job(&self, epoch: Epoch);
    fn end_job(&self, round_id: RoundId);
}

type Call<T> = Pin<Box<dyn Future<Output = Result<T, TransportError>> + Send>>;

pub(crate) struct TransferExecutor {
    wire: Wire,
    codec: Arc<dyn StepCodec>,
    files: Arc<FileDirectories>,
    jobs: Arc<dyn RoundJobs>,
    settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
    shows: Arc<dyn Fn(&str, Shown) + Send + Sync>,
    copies: Flights<Key>,
    steps: Flights<Key>,
}

impl TransferExecutor {
    pub fn new(
        wire: Wire,
        codec: Arc<dyn StepCodec>,
        files: Arc<FileDirectories>,
        jobs: Arc<dyn RoundJobs>,
        settle: Arc<dyn Fn(ReplyId, TransferAnswer) + Send + Sync>,
        shows: Arc<dyn Fn(&str, Shown) + Send + Sync>,
    ) -> Self {
        Self { wire, codec, files, jobs, settle, shows, copies: Flights::default(), steps: Flights::default() }
    }

    fn copy(&self, stamp: EpochStamp, back: &Back<Transfers>, key: Key, source: Source) {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (files, copies, back, flag, id, job_key) = (self.files.clone(), self.copies.clone(), back.clone(), cancelled.clone(), file_id(&key), key.clone());
        let running = self.wire.tasks.start_blocking(Box::new(move || {
            let result = files.copy_source(&id, std::path::Path::new(&source.path));
            let input = if flag.load(Ordering::SeqCst) {
                let _ = files.discard(&[id]);
                None
            } else {
                Some(match result {
                    Ok(CopiedSource::Ready { size, sha256, location, .. }) => TransferInput::CopyDone { key: job_key.clone(), size, sha256, place: location },
                    Ok(CopiedSource::Unreadable(error)) => TransferInput::CopyFailed { key: job_key.clone(), error },
                    Err(error) => TransferInput::CopyFailed { key: job_key.clone(), error: error.to_string() },
                })
            };
            copies.done(&job_key);
            if let Some(input) = input {
                back.reply(stamp, input);
            }
        }));
        self.copies.put(key, Running::new(move || {
            cancelled.store(true, Ordering::SeqCst);
            running.cancel();
        }));
    }

    fn link(&self) -> Option<Link> {
        let admitted = (self.wire.admit)()?;
        Some(Link { transport: self.wire.transport.clone(), admitted, cancel: CancelToken::new() })
    }

    /// Runs `call` as the task of `key`: its result becomes `input_of(result)` for the owner, unless the task is cancelled first.
    fn task<T: Send + 'static>(
        &self,
        stamp: EpochStamp,
        back: &Back<Transfers>,
        key: Key,
        no_link: TransferInput,
        call: impl FnOnce(Arc<dyn StepCodec>, Link) -> Call<T> + Send + 'static,
        input_of: impl FnOnce(Result<T, TransportError>) -> TransferInput + Send + 'static,
    ) {
        let Some(link) = self.link() else {
            back.reply(stamp, no_link);
            return;
        };
        let cancel = link.cancel.clone();
        let (codec, steps, back, task_key) = (self.codec.clone(), self.steps.clone(), back.clone(), key.clone());
        let running = self.wire.tasks.start(Box::pin(async move {
            let result = call(codec, link).await;
            steps.done(&task_key);
            back.reply(stamp, input_of(result));
        }));
        self.steps.put(key, Running::new(move || {
            cancel.cancel();
            running.cancel();
        }));
    }

    fn step(&self, stamp: EpochStamp, back: &Back<Transfers>, key: Key, part: Option<u32>, upload_id: Option<String>, source: Source) {
        let (call_key, ok_key) = (key.clone(), key.clone());
        self.task(
            stamp,
            back,
            key.clone(),
            TransferInput::StepError { key, part, temporary: true },
            move |codec, link| Box::pin(async move { codec.step(&link, &call_key, part, upload_id.as_deref(), &source).await }),
            move |result| match result {
                Ok(done) => TransferInput::StepOk { key: ok_key, part, upload_id: done.upload_id },
                Err(error) => TransferInput::StepError { key: ok_key, part, temporary: temporary(&error) },
            },
        );
    }

    fn complete(&self, stamp: EpochStamp, back: &Back<Transfers>, key: Key, upload_id: Option<String>) {
        let (call_key, ok_key) = (key.clone(), key.clone());
        self.task(
            stamp,
            back,
            key.clone(),
            TransferInput::CompleteError { key, temporary: true },
            move |codec, link| Box::pin(async move { codec.complete(&link, &call_key, upload_id.as_deref()).await }),
            move |result| match result {
                Ok(()) => TransferInput::Completed { key: ok_key },
                Err(error) => TransferInput::CompleteError { key: ok_key, temporary: temporary(&error) },
            },
        );
    }

    /// The owner has no input for the end of an abort: the result is not reported.
    fn drop_upload(&self, key: Key, upload_id: Option<String>) {
        let Some(link) = self.link() else { return };
        let codec = self.codec.clone();
        let _ = self.wire.tasks.start(Box::pin(async move {
            let _ = codec.discard_upload(&link, &key, upload_id.as_deref()).await;
        }));
    }

    fn delete_file(&self, path: String) {
        let files = self.files.clone();
        let id = path;
        let _ = self.wire.tasks.start_blocking(Box::new(move || {
            let _ = files.discard(&[id]);
        }));
    }
}

fn temporary(error: &TransportError) -> bool { RetryClass::of_error(error) != RetryClass::Fatal }

impl EffectRunner<Transfers> for TransferExecutor {
    fn run(&mut self, stamp: EpochStamp, effect: TransferEffect, back: &Back<Transfers>) {
        match effect {
            TransferEffect::Copy { key, source } => self.copy(stamp, back, key, source),
            TransferEffect::CopyCancel { key } => self.copies.cancel(&key),
            TransferEffect::Step { key, part, purpose, upload_id, source } => {
                debug_assert_eq!(purpose, NetworkPurpose::Transfer);
                self.step(stamp, back, key, part, upload_id, source);
            }
            TransferEffect::Complete { key, upload_id } => self.complete(stamp, back, key, upload_id),
            TransferEffect::Abort { key, upload_id } => self.drop_upload(key, upload_id),
            TransferEffect::CancelStep { key } => self.steps.cancel(&key),
            TransferEffect::DeleteFile { path } => self.delete_file(path),
            TransferEffect::Reply { reply, answer } => (self.settle)(reply, answer),
            TransferEffect::Shows { key, shown } => (self.shows)(&key, shown),
            TransferEffect::Refused { key } => (self.shows)(&key, Shown::Failed { error: "refused".into() }),
            TransferEffect::ScheduleJob { epoch } => self.jobs.schedule_job(epoch),
            TransferEffect::EndJob { round_id } => self.jobs.end_job(round_id),
            TransferEffect::Drained(_) => {}
        }
    }
}
