use statig::prelude::*;

use crate::operation::OperationStatus;
use crate::syncstate::OperationFailure;

use super::state::{UploadPart, UploadRequest, UploadRetry, UploadScenario, UploadStage, UploadState, UploadStatus};

#[derive(Debug, Clone)]
pub(crate) enum UploadEvent {
    Restore,
    Start,
    Located { total: u64, scenario: UploadScenario },
    CreateStarted,
    Granted { signed_id: String, blob_id: String, url: String, headers: Vec<(String, String)>, checksum: String, issued_at: u64 },
    PutStarted,
    PutDone { total: u64 },
    GrantDropped,
    InitStarted,
    Initiated { key: String, upload_id: String },
    PresignStarted,
    Presigned,
    Baseline { sent: u64 },
    PartConfirmed { part: UploadPart, sent: u64 },
    CompleteStarted,
    Completed { signed_id: String, blob_id: String, total: u64 },
    Progress { sent: u64 },
    MutationBegan { request: UploadRequest },
    RequestRemembered { request: UploadRequest },
    RetryScheduled { stage: UploadStage, part_number: u32, attempts: u32, retry_at: u64, failure: OperationFailure },
    RetryElapsed { stage: UploadStage, part_number: u32 },
    RetryCleared { stage: UploadStage, part_number: u32 },
    AbortStarted,
    Aborted,
    ErrorNoted { failure: OperationFailure },
    Failed { failure: OperationFailure, at: u64, batch: bool, resume_at: Option<UploadStage> },
    Cancel,
    Retried,
    WentOffline,
    CameOnline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refused {
    pub phase: UploadStatus,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "the upload does not accept this event while it is {:?}", self.phase)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunMode {
    Transfer,
    Mutation,
    Orphan,
    Cancel,
}

struct Flow {
    state: UploadState,
    refused: bool,
}

impl Flow {
    fn begin(&mut self) {
        self.state.stage = UploadStage::Prepare;
        self.state.error = None;
        self.state.offline = false;
    }

    fn staged(&mut self, stage: UploadStage) -> Outcome<State> {
        self.state.stage = stage;
        Handled
    }

    fn mutate(&mut self, request: &UploadRequest) {
        self.state.stage = UploadStage::Mutation;
        self.state.offline = false;
        self.state.request = Some(request.clone());
    }

    fn retry_at(&mut self, stage: UploadStage, part_number: u32) -> Option<&mut UploadRetry> {
        self.state.retries.iter_mut().find(|retry| retry.stage == stage && retry.part_number == part_number)
    }
}

struct UploadFlow;

#[state_machine(initial = "State::queued()")]
impl UploadFlow {
    #[superstate]
    fn any(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::Restore => match state_of(context.state.phase) {
                Some(stored) => Transition(stored),
                None => {
                    context.refused = true;
                    Handled
                }
            },
            UploadEvent::RequestRemembered { request } => {
                context.state.request = Some(request.clone());
                Handled
            }
            UploadEvent::RetryScheduled { stage, part_number, attempts, retry_at, failure } => {
                context.state.retries.retain(|retry| retry.stage != *stage || retry.part_number != *part_number);
                context.state.retries.push(UploadRetry { stage: *stage, part_number: *part_number, attempts: *attempts, retry_at: Some(*retry_at) });
                context.state.error = Some(failure.clone());
                Handled
            }
            UploadEvent::RetryElapsed { stage, part_number } => {
                if let Some(retry) = context.retry_at(*stage, *part_number) {
                    retry.retry_at = None;
                }
                context.state.error = None;
                Handled
            }
            UploadEvent::RetryCleared { stage, part_number } => {
                context.state.retries.retain(|retry| retry.stage != *stage || retry.part_number != *part_number);
                Handled
            }
            UploadEvent::AbortStarted => context.staged(UploadStage::Abort),
            UploadEvent::Aborted => {
                context.state.key = None;
                context.state.upload_id = None;
                context.state.parts.clear();
                context.state.sent_bytes = 0;
                context.state.request = None;
                Handled
            }
            UploadEvent::WentOffline => {
                context.state.offline = true;
                Handled
            }
            UploadEvent::CameOnline => {
                context.state.offline = false;
                Handled
            }
            UploadEvent::ErrorNoted { failure } => {
                context.state.error = Some(failure.clone());
                Handled
            }
            UploadEvent::Failed { failure, at, batch, resume_at } => {
                if let Some(stage) = resume_at {
                    context.state.stage = *stage;
                }
                context.state.error = Some(failure.clone());
                context.state.offline = false;
                context.state.failed_at = Some(*at);
                if *batch { Transition(State::failed()) } else { Handled }
            }
            UploadEvent::Cancel => {
                context.state.offline = false;
                Transition(State::cancelled())
            }
            _ => {
                context.refused = true;
                Handled
            }
        }
    }

    #[state(superstate = "any")]
    fn queued(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::Start => {
                context.begin();
                Transition(State::preparing())
            }
            UploadEvent::MutationBegan { request } => {
                context.mutate(request);
                Transition(State::sending())
            }
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn preparing(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::Start => {
                context.begin();
                Handled
            }
            UploadEvent::Located { total, scenario } => {
                context.state.total_bytes = *total;
                context.state.scenario = Some(*scenario);
                Transition(State::uploading())
            }
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn uploading(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::Start => {
                context.begin();
                Transition(State::preparing())
            }
            UploadEvent::MutationBegan { request } => {
                context.mutate(request);
                Transition(State::sending())
            }
            UploadEvent::CreateStarted => context.staged(UploadStage::Create),
            UploadEvent::Granted { signed_id, blob_id, url, headers, checksum, issued_at } => {
                context.state.signed_id = Some(signed_id.clone());
                context.state.blob_id = Some(blob_id.clone());
                context.state.put_url = Some(url.clone());
                context.state.headers = headers.clone();
                context.state.checksum = Some(checksum.clone());
                context.state.url_issued_at = Some(*issued_at);
                Handled
            }
            UploadEvent::PutStarted => context.staged(UploadStage::Put),
            UploadEvent::PutDone { total } => {
                context.state.sent_bytes = *total;
                context.state.confirmed_bytes = *total;
                context.state.transferred = true;
                Handled
            }
            UploadEvent::GrantDropped => {
                context.state.put_url = None;
                context.state.headers.clear();
                Handled
            }
            UploadEvent::InitStarted => context.staged(UploadStage::Init),
            UploadEvent::Initiated { key, upload_id } => {
                context.state.key = Some(key.clone());
                context.state.upload_id = Some(upload_id.clone());
                context.state.parts.clear();
                context.state.sent_bytes = 0;
                Handled
            }
            UploadEvent::PresignStarted => context.staged(UploadStage::Presign),
            UploadEvent::Presigned => context.staged(UploadStage::Part),
            UploadEvent::Baseline { sent } => {
                context.state.sent_bytes = *sent;
                Handled
            }
            UploadEvent::PartConfirmed { part, sent } => {
                context.state.parts.push(part.clone());
                context.state.parts.sort_by_key(|part| part.number);
                context.state.sent_bytes = context.state.sent_bytes.max(*sent);
                context.state.confirmed_bytes = *sent;
                Handled
            }
            UploadEvent::CompleteStarted => context.staged(UploadStage::Complete),
            UploadEvent::Completed { signed_id, blob_id, total } => {
                context.state.signed_id = Some(signed_id.clone());
                context.state.blob_id = Some(blob_id.clone());
                context.state.sent_bytes = *total;
                context.state.confirmed_bytes = *total;
                context.state.transferred = true;
                context.state.key = None;
                context.state.upload_id = None;
                context.state.parts.clear();
                Handled
            }
            UploadEvent::Progress { sent } => {
                context.state.sent_bytes = context.state.sent_bytes.max(*sent);
                Handled
            }
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn sending(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::MutationBegan { request } => {
                context.mutate(request);
                Handled
            }
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn failed(context: &mut Flow, event: &UploadEvent) -> Outcome<State> {
        match event {
            UploadEvent::Start => {
                context.begin();
                Transition(State::preparing())
            }
            UploadEvent::Retried => {
                context.state.error = None;
                context.state.failed_at = None;
                context.state.retries.clear();
                Transition(State::queued())
            }
            _ => Super,
        }
    }

    #[state(superstate = "any")]
    fn cancelled(event: &UploadEvent) -> Outcome<State> {
        let _ = event;
        Super
    }
}

fn phase_of(state: &State) -> UploadStatus {
    match state {
        State::Queued {} => UploadStatus::Queued,
        State::Preparing {} => UploadStatus::Preparing,
        State::Uploading {} => UploadStatus::Uploading,
        State::Sending {} => UploadStatus::Sending,
        State::Failed {} => UploadStatus::Failed,
        State::Cancelled {} => UploadStatus::Cancelled,
    }
}

fn state_of(phase: UploadStatus) -> Option<State> {
    match phase {
        UploadStatus::Queued => Some(State::queued()),
        UploadStatus::Preparing => Some(State::preparing()),
        UploadStatus::Uploading => Some(State::uploading()),
        UploadStatus::Sending => Some(State::sending()),
        UploadStatus::Failed => Some(State::failed()),
        UploadStatus::Cancelled => Some(State::cancelled()),
        UploadStatus::Waiting | UploadStatus::Done | UploadStatus::DeliveryUnknown => None,
    }
}

impl UploadState {
    /// The only writer of `phase` and `stage`: the machine is rebuilt at the stored phase, handles one event and writes the result back.
    pub(crate) fn apply(&mut self, event: UploadEvent) -> Result<(), Refused> {
        let before = self.phase;
        let mut flow = Flow { state: std::mem::replace(self, UploadState::queued()), refused: false };
        let mut machine = UploadFlow.uninitialized_state_machine().init_with_context(&mut flow);
        machine.handle_with_context(&UploadEvent::Restore, &mut flow);
        if !flow.refused {
            machine.handle_with_context(&event, &mut flow);
        }
        let phase = phase_of(machine.state());
        let refused = flow.refused;
        *self = flow.state;
        if refused {
            return Err(Refused { phase: before });
        }
        self.phase = phase;
        Ok(())
    }

    pub(crate) fn awaits_run(&self) -> bool {
        !matches!(self.phase, UploadStatus::Failed | UploadStatus::Cancelled)
    }

    pub(crate) fn awaits_transfer(&self) -> bool {
        !self.is_transferred() && self.awaits_run()
    }

    pub(crate) fn is_sendable(&self) -> bool {
        self.is_transferred() && self.awaits_run()
    }

    pub(crate) fn run_mode(&self, status: OperationStatus, transport_started: bool, waiting: bool, request_pending: impl Fn() -> bool) -> Option<RunMode> {
        let cancelled = self.phase == UploadStatus::Cancelled;
        match status {
            OperationStatus::Pending if cancelled => Some(RunMode::Cancel),
            OperationStatus::Pending if self.phase == UploadStatus::Failed && self.upload_id.is_some() && !self.all_parts_confirmed() => Some(RunMode::Orphan),
            OperationStatus::Pending if !transport_started && !waiting && self.awaits_run() => Some(if self.is_transferred() { RunMode::Mutation } else { RunMode::Transfer }),
            OperationStatus::Failed | OperationStatus::DeliveryUnknown if self.upload_id.is_some() && (cancelled || !self.all_parts_confirmed()) && !request_pending() => {
                Some(if cancelled { RunMode::Cancel } else { RunMode::Orphan })
            }
            _ => None,
        }
    }

    pub(crate) fn resume_mode(&self) -> RunMode {
        match (self.stage, self.phase) {
            (UploadStage::Abort, UploadStatus::Cancelled) => RunMode::Cancel,
            (UploadStage::Abort, _) => RunMode::Orphan,
            _ if self.is_transferred() => RunMode::Mutation,
            _ => RunMode::Transfer,
        }
    }
}
