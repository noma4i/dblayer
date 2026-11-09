use super::*;

impl Lifecycle {
    fn blank() -> Self {
        Self {
            state: State::NoSession { epoch: 0 },
            published: Published { epoch: 0, identity: None, generation: 0, state: PubState::NoSession, closed: false },
            publishing: Vec::new(),
            cell: Published { epoch: 0, identity: None, generation: 0, state: PubState::NoSession, closed: false },
            restoring: None,
            cell_publication: 0,
            ended_up_to: None,
            next_publication: 1,
            next_erase: 1,
            allocated: 0,
        }
    }

    /// `Open` rows: the initial state, the first publication, `EpochStarted` and the wait of `open` held for `Saved`.
    pub fn open(saved_epoch: u64, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, reply: Option<Id>) -> (Self, Out) {
        let mut life = Self::blank();
        life.allocated = saved_generation;
        let epoch = saved_epoch + 1;
        let (state, pub_state) = match saved_identity {
            Some(identity) => (State::Session { epoch, identity }, PubState::Session),
            None => (State::NoSession { epoch }, PubState::NoSession),
        };
        life.state = state;
        life.published = Published { epoch, identity: saved_identity, generation: saved_generation, state: pub_state, closed: false };
        let mut out = Out::default();
        if saved_identity.is_some() {
            life.restoring = Some(Restoring { epoch, start_waiting: [Owner::Outbox, Owner::Transfer].into(), restore_waiting: [Owner::Outbox, Owner::Transfer, Owner::Session].into(), open_reply: reply });
            life.publish(&mut out, Some((epoch, saved_identity, saved_generation)), Vec::new());
        } else if saved_unowned_pending {
            life.restoring = Some(Restoring { epoch, start_waiting: Default::default(), restore_waiting: [Owner::Session].into(), open_reply: reply });
            life.publish(&mut out, Some((epoch, saved_identity, saved_generation)), Vec::new());
        } else {
            life.publish(&mut out, Some((epoch, saved_identity, saved_generation)), reply.into_iter().collect());
        }
        (life, out)
    }

    /// `Publish` of the current `published`. `started` and `replies` wait for `Saved` of this publication.
    fn publish(&mut self, out: &mut Out, started: Option<(u64, Option<Identity>, u64)>, replies: Vec<Id>) {
        let id = self.next_publication;
        self.next_publication += 1;
        out.effects.push(Effect::Publish(id, self.published.clone()));
        if matches!(self.published.state, PubState::Ending | PubState::EndingFailed | PubState::Closing | PubState::StorageFailed) {
            self.cell = self.published.clone();
            self.cell_publication = id;
        }
        self.publishing.push(Publication { id, published: self.published.clone(), epoch_started: started, replies });
    }

    fn set_published(&mut self, epoch: u64, identity: Option<Identity>, generation: u64, state: PubState) {
        self.published = Published { epoch, identity, generation, state, closed: false };
    }

    fn erase(&mut self, kind: EraseKind, replies: Vec<Id>) -> Erase {
        let id = self.next_erase;
        self.next_erase += 1;
        self.allocated = self.allocated.max(self.published.generation) + 1;
        Erase { id, kind, target: self.allocated, replies }
    }

    /// The restore of an epoch that ended, or of a close, ends with the wait of `open`.
    fn drop_restoring(&mut self, out: &mut Out, answer: Answer) {
        if let Some(restoring) = self.restoring.take() { answer_all(out, restoring.open_reply.into_iter().collect(), answer); }
    }

    fn end_restoring(&mut self, out: &mut Out) {
        let saved = self.restoring.as_ref().is_some_and(|restoring| !self.publishing.iter().any(|publication| publication.epoch_started.is_some_and(|(epoch, ..)| epoch == restoring.epoch)));
        self.drop_restoring(out, if saved { Answer::Ok } else { Answer::Interrupted });
    }

    fn ended(&mut self, epoch: u64) { self.ended_up_to = Some(self.ended_up_to.map_or(epoch, |ended| ended.max(epoch))); }

    fn closing(&mut self, out: &mut Out, erases: Vec<Erase>, close_replies: Vec<Id>, end: Option<u64>) -> State {
        self.published.state = PubState::Closing;
        self.published.closed = true;
        self.publish(out, None, Vec::new());
        self.drop_restoring(out, Answer::Closed);
        if let Some(epoch) = end {
            self.ended(epoch);
            out.effects.push(Effect::EpochEnded(epoch, Cause::Close));
        }
        out.effects.push(Effect::Closing);
        out.effects.push(Effect::StopObserving);
        State::Closing { waiting: all_owners(), erases, close_replies, store_closing: false, store_closed: false, observers_stopped: false }
    }

    /// `Ending` entered from a state with a live epoch: `EpochEnded` · `Publish` · `ClearAccount`.
    fn ending(&mut self, epoch: u64, cause: Cause, next: Option<Identity>, mut erases: Vec<Erase>, replies: Vec<Id>, out: &mut Out) {
        self.ended(epoch);
        self.end_restoring(out);
        out.effects.push(Effect::EpochEnded(epoch, cause));
        self.published.state = PubState::Ending;
        self.publish(out, None, Vec::new());
        let erase = self.erase(EraseKind::Account, replies);
        out.effects.push(Effect::ClearAccount { epoch_old: epoch, cause, generation_new: erase.target, erase_id: erase.id });
        erases.push(erase);
        self.state = State::Ending { epoch_old: epoch, cause, next, failed: false, erase_credential: false, erases };
    }

    fn start(&mut self, epoch: u64, identity: Option<Identity>, generation: u64, replies: Vec<Id>, out: &mut Out) {
        self.set_published(epoch, identity, generation, if identity.is_some() { PubState::Session } else { PubState::NoSession });
        if identity.is_some() {
            self.restoring = Some(Restoring { epoch, start_waiting: [Owner::Outbox, Owner::Transfer].into(), restore_waiting: [Owner::Outbox, Owner::Transfer, Owner::Session].into(), open_reply: None });
        }
        self.publish(out, Some((epoch, identity, generation)), replies);
    }

    /// The state a reset returns to.
    fn settled(epoch: u64, identity: Option<Identity>) -> State {
        match identity {
            Some(identity) => State::Session { epoch, identity },
            None => State::NoSession { epoch },
        }
    }

    fn take_erase(erases: &mut Vec<Erase>, id: EraseId) -> Option<Erase> {
        let at = erases.iter().position(|erase| erase.id == id)?;
        Some(erases.remove(at))
    }

    /// One row of the `lifecycle` machine per match arm, in table order.
    pub fn step(&mut self, event: Event) -> Out {
        let mut out = Out::default();
        if let Event::Saved(id) | Event::SaveFailed(id) = event {
            self.saved(id, matches!(event, Event::Saved(_)), &mut out);
            return out;
        }
        if matches!(event, Event::ExecutorFailed) {
            self.executor_failed(&mut out);
            return out;
        }
        if let Event::Started(owner, epoch) | Event::RestoreClassified(owner, epoch) = event {
            self.restore(owner, epoch, matches!(event, Event::Started(..)), &mut out);
            return out;
        }
        let state = std::mem::replace(&mut self.state, State::Closed);
        self.state = match (state, event) {
            (State::NoSession { epoch }, Event::Credential(identity)) => {
                self.ended(epoch);
                out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                self.start(epoch + 1, Some(identity), self.published.generation, Vec::new(), &mut out);
                State::Session { epoch: epoch + 1, identity }
            }
            (State::NoSession { epoch }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), &mut out);
                return out;
            }
            (State::NoSession { epoch }, Event::ResetModels(models, reply)) => self.resetting(epoch, None, models, Vec::new(), reply, &mut out),
            (State::NoSession { epoch }, Event::Close(reply)) => self.closing(&mut out, Vec::new(), vec![reply], Some(epoch)),
            (state @ State::NoSession { .. }, _) => state,

            (State::Session { epoch, identity }, Event::Credential(other)) if other == identity => State::Session { epoch, identity },
            (State::Session { epoch, .. }, Event::Credential(other)) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), Vec::new(), Vec::new(), &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::ServerEnded(request_epoch)) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, Vec::new(), Vec::new(), &mut out);
                return out;
            }
            (State::Session { epoch, identity }, Event::ResetModels(models, reply)) => self.resetting(epoch, Some(identity), models, Vec::new(), reply, &mut out),
            (State::Session { epoch, .. }, Event::Close(reply)) => self.closing(&mut out, Vec::new(), vec![reply], Some(epoch)),
            (state @ State::Session { .. }, _) => state,

            (State::Ending { epoch_old, cause, failed, mut erases, erase_credential, .. }, Event::Credential(identity)) => {
                if failed {
                    self.published.state = PubState::Ending;
                    self.publish(&mut out, None, Vec::new());
                    let erase = self.erase(EraseKind::Account, Vec::new());
                    out.effects.push(Effect::ClearAccount { epoch_old, cause, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                }
                State::Ending { epoch_old, cause, next: Some(identity), failed: false, erase_credential, erases }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::SessionReset(reply)) => {
                if failed {
                    self.published.state = PubState::Ending;
                    self.publish(&mut out, None, Vec::new());
                    let erase = self.erase(EraseKind::Account, reply.into_iter().collect());
                    out.effects.push(Effect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                    let _ = next;
                    State::Ending { epoch_old, cause: Cause::SessionReset, next: None, failed: false, erase_credential: false, erases }
                } else {
                    let credential = erase_credential || cause == Cause::IdentityChanged;
                    if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) { account.replies.extend(reply); }
                    State::Ending { epoch_old, cause, next: None, failed: false, erase_credential: credential, erases }
                }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ResetModels(_, reply)) => {
                if failed {
                    answer_all(&mut out, reply.into_iter().collect(), Answer::Storage);
                } else if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                    account.replies.extend(reply);
                }
                State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::Cleared(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) if erase_credential => {
                    self.published.generation = self.published.generation.max(erase.target);
                    let second = self.erase(EraseKind::Account, erase.replies);
                    out.effects.push(Effect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: second.target, erase_id: second.id });
                    erases.push(second);
                    State::Ending { epoch_old, cause: Cause::SessionReset, next, failed, erase_credential: false, erases }
                }
                Some(erase) => {
                    let generation = self.published.generation.max(erase.target);
                    match next {
                        None => {
                            self.start(epoch_old + 1, None, generation, erase.replies, &mut out);
                            State::NoSession { epoch: epoch_old + 1 }
                        }
                        Some(identity) => {
                            self.start(epoch_old + 2, Some(identity), generation, erase.replies, &mut out);
                            State::Session { epoch: epoch_old + 2, identity }
                        }
                    }
                }
            },
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ClearFailed(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) => {
                    answer_all(&mut out, erase.replies, Answer::Storage);
                    if erase.kind == EraseKind::Account {
                        self.published.state = PubState::EndingFailed;
                        self.publish(&mut out, None, Vec::new());
                        State::Ending { epoch_old, cause, next, failed: true, erase_credential, erases }
                    } else {
                        State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
                    }
                }
            },
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ModelsCleared(_, id)) => {
                if let Some(erase) = Self::take_erase(&mut erases, id) { answer_all(&mut out, erase.replies, Answer::Ok); }
                State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (State::Ending { erases, .. }, Event::Close(reply)) => self.closing(&mut out, erases, vec![reply], None),
            (state @ State::Ending { .. }, _) => state,

            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, Event::Credential(other)) => match identity {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential: Some(other), erases },
                Some(current) if current == other => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), erases, queued_replies, &mut out);
                    return out;
                }
            },
            (State::Resetting { epoch, mut queued_replies, erases, .. }, Event::SessionReset(reply)) => {
                queued_replies.extend(reply);
                self.ending(epoch, Cause::SessionReset, None, erases, queued_replies, &mut out);
                return out;
            }
            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, Event::ServerEnded(request_epoch)) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, erases, queued_replies, &mut out);
                    return out;
                }
                State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (State::Resetting { epoch, identity, models, mut queued_models, mut queued_replies, pending_credential, erases }, Event::ResetModels(more, reply)) => {
                queued_models.extend(more);
                queued_replies.extend(reply);
                State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, Event::ModelsCleared(generation_new, id)) => match Self::take_erase(&mut erases, id) {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    let _ = generation_new;
                    let generation_new = self.published.generation.max(erase.target);
                    self.published.generation = generation_new;
                    if !queued_models.is_empty() {
                        self.published.state = PubState::Resetting;
                        self.publish(&mut out, None, erase.replies);
                        out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                        let next = self.erase(EraseKind::Models, queued_replies);
                        out.effects.push(Effect::ClearModels { epoch, generation_new: next.target, models: queued_models.clone(), erase_id: next.id });
                        erases.push(next);
                        self.state = State::Resetting { epoch, identity, models: queued_models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential, erases };
                        return out;
                    }
                    match pending_credential {
                        Some(next) => {
                            out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                            self.ended(epoch);
                            out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                            self.start(epoch + 1, Some(next), generation_new, erase.replies, &mut out);
                            State::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation_new, if identity.is_some() { PubState::Session } else { PubState::NoSession });
                            self.publish(&mut out, None, erase.replies);
                            out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                            Self::settled(epoch, identity)
                        }
                    }
                }
            },
            (State::Resetting { epoch, identity, queued_replies, pending_credential, mut erases, models, queued_models }, Event::ClearFailed(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    answer_all(&mut out, erase.replies, Answer::Storage);
                    answer_all(&mut out, queued_replies, Answer::Storage);
                    match pending_credential {
                        Some(next) => {
                            self.ended(epoch);
                            out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                            let generation = self.published.generation;
                            self.start(epoch + 1, Some(next), generation, Vec::new(), &mut out);
                            State::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, self.published.generation, if identity.is_some() { PubState::Session } else { PubState::NoSession });
                            self.publish(&mut out, None, Vec::new());
                            Self::settled(epoch, identity)
                        }
                    }
                }
            },
            (State::Resetting { epoch, queued_replies, erases, .. }, Event::Close(reply)) => {
                answer_all(&mut out, queued_replies, Answer::Interrupted);
                self.closing(&mut out, erases, vec![reply], Some(epoch))
            }
            (state @ State::Resetting { .. }, _) => state,

            (State::Closing { mut waiting, mut erases, mut close_replies, mut store_closing, mut store_closed, mut observers_stopped }, ev @ (Event::Drained(_) | Event::Cleared(_) | Event::ModelsCleared(..) | Event::ClearFailed(_) | Event::StoreClosed | Event::ObserversStopped | Event::Close(_))) => {
                match ev.clone() {
                    Event::Drained(owner) => { waiting.remove(&owner); }
                    Event::Cleared(id) | Event::ModelsCleared(_, id) | Event::ClearFailed(id) => {
                        if let Some(erase) = Self::take_erase(&mut erases, id) {
                            answer_all(&mut out, erase.replies, if matches!(ev, Event::ClearFailed(_)) { Answer::Storage } else { Answer::Ok });
                        }
                    }
                    Event::StoreClosed => store_closed = store_closed || store_closing,
                    Event::ObserversStopped => observers_stopped = true,
                    Event::Close(reply) => close_replies.push(reply),
                    _ => {}
                }
                if waiting.is_empty() && erases.is_empty() && !store_closing {
                    store_closing = true;
                    out.effects.push(Effect::StoreClose);
                }
                if store_closed && observers_stopped {
                    answer_all(&mut out, close_replies, Answer::Ok);
                    State::Closed
                } else {
                    State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
                }
            }
            (state @ State::Closing { .. }, Event::SessionReset(reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                state
            }
            (state @ State::Closing { .. }, Event::ResetModels(_, reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                state
            }
            (state @ State::Closing { .. }, _) => state,

            (State::Closed, Event::SessionReset(reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                State::Closed
            }
            (State::Closed, Event::ResetModels(_, reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                State::Closed
            }
            (State::Closed, Event::Close(reply)) => {
                answer_all(&mut out, vec![reply], Answer::Ok);
                State::Closed
            }
            (State::Closed, _) => State::Closed,
        };
        out
    }

    fn resetting(&mut self, epoch: u64, identity: Option<Identity>, models: BTreeSet<String>, mut erases: Vec<Erase>, reply: Option<Id>, out: &mut Out) -> State {
        self.published.state = PubState::Resetting;
        self.publish(out, None, Vec::new());
        let erase = self.erase(EraseKind::Models, reply.into_iter().collect());
        out.effects.push(Effect::ClearModels { epoch, generation_new: erase.target, models: models.clone(), erase_id: erase.id });
        erases.push(erase);
        State::Resetting { epoch, identity, models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential: None, erases }
    }

    /// `life_owner_started`, `life_restore_classified` and their stray row.
    fn restore(&mut self, owner: Owner, epoch: u64, started: bool, out: &mut Out) {
        if matches!(self.state, State::Closing { .. } | State::Closed) { return; }
        let Some(restoring) = &mut self.restoring else { return };
        if restoring.epoch != epoch { return; }
        if started {
            if restoring.start_waiting.remove(&owner) && restoring.start_waiting.is_empty() { out.effects.push(Effect::ReadIntents(epoch)); }
            return;
        }
        if !restoring.restore_waiting.remove(&owner) || !restoring.restore_waiting.is_empty() { return; }
        let reply = restoring.open_reply;
        self.restoring = None;
        answer_all(out, reply.into_iter().collect(), Answer::Ok);
        out.effects.push(Effect::RestoreReady(epoch));
    }

    fn restricting(state: PubState) -> bool { matches!(state, PubState::Ending | PubState::EndingFailed | PubState::Closing | PubState::StorageFailed) }

    /// `Saved` and `SaveFailed` of the rows `life_saved`, `life_saved_stale`, `life_save_failed`, `life_save_failed_stale`, `life_closing_saved` and the stray rows.
    fn saved(&mut self, id: PublicationId, ok: bool, out: &mut Out) {
        let Some(at) = self.publishing.iter().position(|publication| publication.id == id) else { return };
        if matches!(self.state, State::Closed) { return; }
        let publication = self.publishing.remove(at);
        let closing = matches!(self.state, State::Closing { .. });
        if closing {
            answer_all(out, publication.replies, if ok { Answer::Ok } else { Answer::Storage });
            return;
        }
        let newer = self.cell_publication > id;
        let restricting = Self::restricting(publication.published.state);
        if ok {
            let ended = self.ended_up_to.is_some_and(|ended| publication.published.epoch <= ended);
            if !restricting && (ended || newer) {
                answer_all(out, publication.replies, Answer::Interrupted);
                return;
            }
            if !restricting {
                self.cell = publication.published.clone();
                self.cell_publication = id;
                out.effects.push(Effect::PublishCell(publication.published.clone()));
            }
            answer_all(out, publication.replies, Answer::Ok);
            if let Some((epoch, identity, generation)) = publication.epoch_started {
                out.effects.push(Effect::EpochStarted(epoch, identity, generation));
                if self.restoring.as_ref().is_some_and(|restoring| restoring.epoch == epoch && restoring.start_waiting.is_empty()) {
                    out.effects.push(Effect::ReadIntents(epoch));
                }
            }
            return;
        }
        answer_all(out, publication.replies, Answer::Storage);
        let opening = publication.epoch_started.map(|(epoch, ..)| epoch);
        if let Some(restoring) = self.restoring.take_if(|restoring| opening == Some(restoring.epoch)) {
            answer_all(out, restoring.open_reply.into_iter().collect(), Answer::Storage);
        }
        if !newer {
            self.cell.state = PubState::StorageFailed;
            out.effects.push(Effect::PublishCell(self.cell.clone()));
        }
    }

    /// `life_executor_failed`.
    fn executor_failed(&mut self, out: &mut Out) {
        let state = std::mem::replace(&mut self.state, State::Closed);
        self.state = match state {
            State::Closed => State::Closed,
            State::Closing { waiting, mut erases, close_replies, store_closing, store_closed, observers_stopped } => {
                let mut replies = Vec::new();
                for erase in &mut erases { replies.append(&mut erase.replies); }
                for publication in &mut self.publishing { replies.append(&mut publication.replies); }
                answer_all(out, replies, Answer::Runtime);
                State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            state => {
                let (end, erases, open) = match state {
                    State::NoSession { epoch } | State::Session { epoch, .. } => (Some(epoch), Vec::new(), Vec::new()),
                    State::Resetting { epoch, erases, queued_replies, .. } => (Some(epoch), erases, queued_replies),
                    State::Ending { erases, .. } => (None, erases, Vec::new()),
                    _ => (None, Vec::new(), Vec::new()),
                };
                self.drop_restoring(out, Answer::Runtime);
                let mut waiting_replies: Vec<Id> = open;
                for erase in &erases { waiting_replies.extend(erase.replies.iter().copied()); }
                for publication in &mut self.publishing { waiting_replies.append(&mut publication.replies); }
                answer_all(out, waiting_replies, Answer::Runtime);
                let erases: Vec<Erase> = erases.into_iter().map(|erase| Erase { replies: Vec::new(), ..erase }).collect();
                self.closing(out, erases, Vec::new(), end)
            }
        };
    }
}

