//! Owner `Windows`: a dictionary of `Window` machines by key.
//! A table row applies to the machine of the key an input names. Inputs that name a request (`Response`, `Failure`, `Dropped`, `RetryDue`,
//! `Committed`, `Rejected`) go to the machine that owns the request. `Online`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to
//! every machine, `Stale` to the machines of its model named in `keys`. Request ids are unique in the owner. The retry timer is `Effect::Schedule`
//! of the owner (`after` = the backoff). Inputs for other owners (`Commit`, `WindowState`) are `Effect::Send` to the addresses of the state.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use statig::prelude::*;

use crate::owners::read::deliveries::{DeliveryInput, Deliveries};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter};
use crate::runtime::RetryPolicy;
use crate::owners::contract::{BatchAuthor, Committed, Drained, EpochEnded, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError, RequestId, WindowPhase as Phase, WindowState};
use crate::runtime::{Address, Cause, Effect, EffectId, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

mod windows_types;
pub use windows_types::*;

mod windows_facts;
use windows_facts::*;

#[state_machine(initial = "WinState::idle()", state(name = "WinState"))]
impl Facts {
    #[state]
    fn idle(&mut self, context: &mut Ctx<'_>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPassive { id } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::InterestPlus { id, now } => {
                self.interest.insert(*id);
                if self.is_fresh(*now) {
                    self.set_phase(Phase::Loaded);
                    self.announce();
                    Handled
                } else {
                    self.begin(context, 1, false, true)
                }
            }
            Ev::Load { id, goal, force, now, .. } => self.idle_load(context, (*id, *now), *goal, *force),
            Ev::OfflineRefused { message } => {
                self.error = Some(crate::ffi::DbError::Network { detail: message.clone() });
                self.set_phase(self.failed_phase());
                self.announce();
                Handled
            }
            Ev::InterestMinus { id } => {
                self.drop_interest(id);
                Handled
            }
            Ev::Stale if self.watched() => self.begin(context, 1, false, self.is_online()),
            Ev::Stale => {
                self.fresh = false;
                Handled
            }
            Ev::Foreground if !self.interest.is_empty() && self.is_online() => self.begin(context, 1, false, true),
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::Online { online: true, now } if !self.is_fresh(*now) && self.watched() => self.begin(context, 1, false, true),
            Ev::Online { online: true, .. } if self.reconnected && self.interest.iter().any(|id| self.reload.contains(id)) => self.begin(context, 1, false, true),
            Ev::ModelsReset { models, .. } if self.depends_on(models) => {
                self.fresh = false;
                if self.watched() { self.begin(context, 1, false, self.is_online()) } else { Handled }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    fn loading(&mut self, context: &mut Ctx<'_>, load: &mut Load, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        let mine = |got: &RequestId, got_attempt: &u32| *got == load.request && *got_attempt == load.attempt && !load.retry;
        match event {
            Ev::Pinned { request: got, base_revision } if *got == load.request && load.pinning => {
                self.request_base.set(i64::try_from(*base_revision).unwrap_or(i64::MAX));
                load.pinning = false;
                let effect = self.request_effect(load);
                self.push_fx(effect);
                Handled
            }
            Ev::InterestPlus { id, .. } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::Load { id, goal: Goal::First, force: true, .. } if load.page > 1 => {
                self.join_load(*id, Goal::First, load.page);
                let (request, attempt) = context.request();
                if !load.retry {
                    self.push_fx(WindowEffect::Cancel { request: load.request, attempt: load.attempt });
                }
                self.emit(Emit::CancelRetry { request: load.request });
                self.emit(Emit::Pin { epoch: self.epoch, request });
                *load = self.issue(request, attempt, 1, true);
                self.announce();
                Handled
            }
            Ev::Load { id, goal, force: wider, .. } => {
                self.join_load(*id, *goal, load.page);
                load.force |= *wider && *goal != Goal::Next;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.drop_interest(id);
                if self.watched() {
                    return Handled;
                }
                self.fresh = false;
                self.set_phase(if self.has_rows { Phase::Loaded } else { Phase::Idle });
                self.push_fx(WindowEffect::Cancel { request: load.request, attempt: load.attempt });
                self.emit(Emit::CancelRetry { request: load.request });
                Transition(WinState::idle())
            }
            Ev::Response { request: got, attempt: got_attempt, page: answer, now } if mine(got, got_attempt) && load.chain.is_some() => self.media_response(context, load, answer, *now),
            Ev::Response { request: got, attempt: got_attempt, page: answer, now } if mine(got, got_attempt) => {
                self.answered_at = *now;
                self.page_count = if load.page == 1 { 1 } else { self.page_count + 1 };
                let models: BTreeSet<&String> = self.dependency_models.iter().chain(&answer.dependency_models).chain(answer.payload.related.iter().map(|(model, _)| model)).collect();
                let basis = crate::owners::store::basis_for(models.into_iter().map(|model| (model.clone(), load.basis.of(model))));
                self.emit(Emit::Commit { request: load.request, basis, page: answer.clone(), number: load.page, replace_all: self.replace_all, base_revision: self.request_base.get(), marker: None, keep_rows: false });
                Transition(WinState::landing(Landing { request: load.request, page: load.page, cursor: answer.cursor.clone(), rows: !answer.rows.is_empty(), models: answer.dependency_models.clone() }))
            }
            Ev::Failure { request: got, attempt: got_attempt, error } if mine(got, got_attempt) => {
                if error.class == ErrorClass::InvalidCursor && !self.cursor_reset_used {
                    let (new_request, new_attempt) = context.request();
                    self.cursor_reset_used = true;
                    self.replace_all = true;
                    self.emit(Emit::Pin { epoch: self.epoch, request: new_request });
                    return Transition(WinState::loading(self.issue(new_request, new_attempt, 1, load.force)));
                }
                let class = if error.class == ErrorClass::InvalidCursor { ErrorClass::Permanent } else { error.class };
                if class != ErrorClass::Permanent && load.attempts + 1 < attempt_limit(class) {
                    load.attempts += 1;
                    load.retry = true;
                    self.emit(Emit::Retry { request: load.request, attempt: load.attempts, class });
                    return Handled;
                }
                self.fail(&error.message, error.source.clone())
            }
            Ev::RetryDue { request: got } if *got == load.request && load.retry && self.is_online() => {
                load.attempt = context.attempt();
                load.retry = false;
                let effect = self.request_effect(load);
                self.push_fx(effect);
                Handled
            }
            Ev::Online { online: false, .. } => {
                if !load.retry {
                    self.push_fx(WindowEffect::Cancel { request: load.request, attempt: load.attempt });
                }
                self.emit(Emit::CancelRetry { request: load.request });
                Transition(WinState::waiting(load.force, load.page))
            }
            Ev::Stale => {
                self.reread_due = true;
                Handled
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::ModelsReset { models, .. } if self.depends_on(models) => {
                let (new_request, new_attempt) = context.request();
                self.push_fx(WindowEffect::Cancel { request: load.request, attempt: load.attempt });
                self.emit(Emit::CancelRetry { request: load.request });
                self.emit(Emit::Pin { epoch: self.epoch, request: new_request });
                load.request = new_request;
                load.attempt = new_attempt;
                load.retry = false;
                load.pinning = true;
                load.attempts = 0;
                load.basis = self.basis();
                Handled
            }
            Ev::EpochEnded | Ev::Closing => {
                self.emit(Emit::CancelRetry { request: load.request });
                self.finish((!load.retry).then_some((load.request, load.attempt)))
            }
            _ => Handled,
        }
    }

    #[state]
    fn landing(&mut self, context: &mut Ctx<'_>, landing: &mut Landing, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Committed { request: got, revision } if *got == landing.request => self.landed(context, landing.page, landing.cursor.clone(), landing.rows, &landing.models, *revision),
            Ev::Rejected { request: got, reason } if *got == landing.request => match reason {
                RejectReason::StaleGeneration { models: stale, generation } => {
                    for model in stale {
                        self.generations.insert(model.clone(), *generation);
                    }
                    if self.watched() {
                        self.begin(context, 1, false, false)
                    } else {
                        self.fresh = false;
                        Transition(WinState::idle())
                    }
                }
                RejectReason::NoSession | RejectReason::Closed => {
                    self.answer_all(&WindowOutcome::Interrupted);
                    Transition(WinState::ended())
                }
                RejectReason::Storage(message) => self.fail(message, None),
                RejectReason::Invalid => self.fail("the batch has no basis for a model of its rows", None),
            },
            Ev::InterestPlus { id, .. } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::Load { id, goal, .. } => {
                self.join_load(*id, *goal, landing.page);
                Handled
            }
            Ev::InterestMinus { id } => {
                self.drop_interest(id);
                Handled
            }
            Ev::Stale => {
                self.reread_due = true;
                Handled
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    fn waiting(&mut self, context: &mut Ctx<'_>, force: &mut bool, page: &mut u32, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Online { online: true, .. } if self.deferred() => Handled,
            Ev::Online { online: true, .. } => self.leave_waiting(context, *force, *page),
            Ev::PowerSaving { .. } if self.is_online() && !self.deferred() => self.leave_waiting(context, *force, *page),
            Ev::OfflineRefused { message } => {
                self.error = Some(crate::ffi::DbError::Network { detail: message.clone() });
                self.set_phase(self.failed_phase());
                self.announce();
                Handled
            }
            Ev::InterestPlus { id, .. } => {
                self.interest.insert(*id);
                if self.is_online() && !self.deferred() {
                    return self.leave_waiting(context, *force, *page);
                }
                self.announce();
                Handled
            }
            Ev::Load { id, goal, force: wider, .. } => {
                self.join_load(*id, *goal, *page);
                *force |= *wider && *goal != Goal::Next;
                if self.is_online() && !self.deferred() {
                    return self.leave_waiting(context, *force, *page);
                }
                Handled
            }
            Ev::InterestMinus { id } => {
                self.drop_interest(id);
                if self.watched() {
                    Handled
                } else {
                    self.fresh = false;
                    Transition(WinState::idle())
                }
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    fn ended(&mut self, event: &Ev) -> Outcome<WinState> {
        if let Ev::InterestPlus { id, .. } | Ev::Load { id, .. } = event {
            self.answer(*id, WindowOutcome::Interrupted);
        }
        Handled
    }
}

mod windows_owner;
pub use windows_owner::*;
