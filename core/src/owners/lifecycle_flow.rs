use super::*;

impl LifecycleState {
    /// One row of the table per match arm, in table order.
    pub(super) fn run(&mut self, input: LifecycleInput, fx: &mut Vec<LifecycleEffect>) {
        match input {
            LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, reply } => {
                self.allocated = saved_generation;
                let epoch = saved_epoch + 1;
                self.phase = settled(epoch, saved_identity);
                self.set_published(epoch, saved_identity, saved_generation, settled_state(saved_identity));
                if saved_identity.is_some() {
                    self.restoring = Some(self.new_restoring(epoch, reply));
                    self.publish(Some(Started { epoch, identity: saved_identity, generation: saved_generation }), Vec::new(), fx);
                } else if saved_unowned_pending {
                    self.restoring = Some(Restoring { epoch, start_waiting: Default::default(), restore_waiting: [Peer::Session].into(), open_reply: reply });
                    self.publish(Some(Started { epoch, identity: saved_identity, generation: saved_generation }), Vec::new(), fx);
                } else {
                    self.publish(Some(Started { epoch, identity: saved_identity, generation: saved_generation }), reply.into_iter().collect(), fx);
                }
                return;
            }
            LifecycleInput::Saved { publication_id } => return self.saved(publication_id, true, fx),
            LifecycleInput::SaveFailed { publication_id, .. } => return self.saved(publication_id, false, fx),
            LifecycleInput::ExecutorFailed { .. } => return self.executor_failed(fx),
            LifecycleInput::Started { owner, epoch } => return self.restore(owner, epoch, true, fx),
            LifecycleInput::RestoreClassified { owner, epoch } => return self.restore(owner, epoch, false, fx),
            _ => {}
        }
        let generation = self.published.generation;
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Closed);
        self.phase = match (phase, input) {
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                self.ended(epoch);
                fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                self.start(epoch + 1, Some(identity), generation, Vec::new(), fx);
                LifecyclePhase::Session { epoch: epoch + 1, identity }
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), fx);
                return;
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::ResetModels { models, reply }) => self.resetting(epoch, None, models, reply, fx),
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::Close { reply }) => self.closing(Vec::new(), vec![reply], Some(epoch), fx),
            (phase @ LifecyclePhase::NoSession { .. }, _) => phase,

            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) if other == identity => LifecyclePhase::Session { epoch, identity },
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), Vec::new(), Vec::new(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, Vec::new(), Vec::new(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::ResetModels { models, reply }) => self.resetting(epoch, Some(identity), models, reply, fx),
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::Close { reply }) => self.closing(Vec::new(), vec![reply], Some(epoch), fx),
            (phase @ LifecyclePhase::Session { .. }, _) => phase,

            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                if failed {
                    self.published.state = PublishedState::Ending;
                    self.publish(None, Vec::new(), fx);
                    let erase = self.new_erase(EraseKind::Account, Vec::new());
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                }
                LifecyclePhase::Ending { epoch_old, cause, next: Some(identity), failed: false, erase_credential, erases }
            }
            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::SessionReset { reply }) => {
                if failed {
                    self.published.state = PublishedState::Ending;
                    self.publish(None, Vec::new(), fx);
                    let erase = self.new_erase(EraseKind::Account, reply.into_iter().collect());
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                    LifecyclePhase::Ending { epoch_old, cause: Cause::SessionReset, next: None, failed: false, erase_credential: false, erases }
                } else {
                    let credential = erase_credential || cause == Cause::IdentityChanged;
                    if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                        account.replies.extend(reply);
                    }
                    LifecyclePhase::Ending { epoch_old, cause, next: None, failed: false, erase_credential: credential, erases }
                }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ResetModels { reply, .. }) => {
                if failed {
                    reply_all(reply.into_iter().collect(), Err(LifecycleError::Storage), fx);
                } else if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                    account.replies.extend(reply);
                }
                LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::Cleared { erase_id }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) if erase_credential => {
                    self.published.generation = self.published.generation.max(erase.target);
                    let second = self.new_erase(EraseKind::Account, erase.replies);
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: second.target, erase_id: second.id });
                    erases.push(second);
                    LifecyclePhase::Ending { epoch_old, cause: Cause::SessionReset, next, failed, erase_credential: false, erases }
                }
                Some(erase) => {
                    let generation = self.published.generation.max(erase.target);
                    match next {
                        None => {
                            self.start(epoch_old + 1, None, generation, erase.replies, fx);
                            LifecyclePhase::NoSession { epoch: epoch_old + 1 }
                        }
                        Some(identity) => {
                            self.start(epoch_old + 2, Some(identity), generation, erase.replies, fx);
                            LifecyclePhase::Session { epoch: epoch_old + 2, identity }
                        }
                    }
                }
            },
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ClearFailed { erase_id, .. }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) => {
                    reply_all(erase.replies, Err(LifecycleError::Storage), fx);
                    if erase.kind == EraseKind::Account {
                        self.published.state = PublishedState::EndingFailed;
                        self.publish(None, Vec::new(), fx);
                        LifecyclePhase::Ending { epoch_old, cause, next, failed: true, erase_credential, erases }
                    } else {
                        LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
                    }
                }
            },
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ModelsCleared { erase_id, .. }) => {
                if let Some(erase) = take_erase(&mut erases, erase_id) {
                    reply_all(erase.replies, Ok(()), fx);
                }
                LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (LifecyclePhase::Ending { erases, .. }, LifecycleInput::Close { reply }) => self.closing(erases, vec![reply], None, fx),
            (phase @ LifecyclePhase::Ending { .. }, _) => phase,

            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => match identity {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential: Some(other), erases },
                Some(current) if current == other => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), erases, queued_replies, fx);
                    return;
                }
            },
            (LifecyclePhase::Resetting { epoch, mut queued_replies, erases, .. }, LifecycleInput::SessionReset { reply }) => {
                queued_replies.extend(reply);
                self.ending(epoch, Cause::SessionReset, None, erases, queued_replies, fx);
                return;
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, erases, queued_replies, fx);
                    return;
                }
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, mut queued_models, mut queued_replies, pending_credential, erases }, LifecycleInput::ResetModels { models: more, reply }) => {
                queued_models.extend(more);
                queued_replies.extend(reply);
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, LifecycleInput::ModelsCleared { erase_id, .. }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    let generation_new = self.published.generation.max(erase.target);
                    self.published.generation = generation_new;
                    if !queued_models.is_empty() {
                        self.published.state = PublishedState::Resetting;
                        self.publish(None, erase.replies, fx);
                        fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                        let next = self.new_erase(EraseKind::Models, queued_replies);
                        fx.push(LifecycleEffect::ClearModels { epoch, generation_new: next.target, models: queued_models.clone(), erase_id: next.id });
                        erases.push(next);
                        self.phase = LifecyclePhase::Resetting { epoch, identity, models: queued_models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential, erases };
                        return;
                    }
                    match pending_credential {
                        Some(next) => {
                            fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                            self.ended(epoch);
                            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                            self.start(epoch + 1, Some(next), generation_new, erase.replies, fx);
                            LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation_new, settled_state(identity));
                            self.publish(None, erase.replies, fx);
                            fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                            settled(epoch, identity)
                        }
                    }
                }
            },
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, LifecycleInput::ClearFailed { erase_id, .. }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    reply_all(erase.replies, Err(LifecycleError::Storage), fx);
                    reply_all(queued_replies, Err(LifecycleError::Storage), fx);
                    match pending_credential {
                        Some(next) => {
                            self.ended(epoch);
                            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                            self.start(epoch + 1, Some(next), generation, Vec::new(), fx);
                            LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation, settled_state(identity));
                            self.publish(None, Vec::new(), fx);
                            settled(epoch, identity)
                        }
                    }
                }
            },
            (LifecyclePhase::Resetting { epoch, queued_replies, erases, .. }, LifecycleInput::Close { reply }) => {
                reply_all(queued_replies, Err(LifecycleError::Interrupted), fx);
                self.closing(erases, vec![reply], Some(epoch), fx)
            }
            (phase @ LifecyclePhase::Resetting { .. }, _) => phase,

            (LifecyclePhase::Closing { mut waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::Drained(Drained { owner })) => {
                if let Some(peer) = Peer::named(owner) {
                    waiting.remove(&peer);
                }
                let last = waiting.is_empty() && erases.is_empty() && !store_closing;
                if last {
                    fx.push(LifecycleEffect::StoreClose);
                }
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing: store_closing || last, store_closed, observers_stopped }
            }
            (LifecyclePhase::Closing { waiting, mut erases, close_replies, mut store_closing, store_closed, observers_stopped }, input @ (LifecycleInput::Cleared { .. } | LifecycleInput::ModelsCleared { .. } | LifecycleInput::ClearFailed { .. })) => {
                let (erase_id, result) = match input {
                    LifecycleInput::Cleared { erase_id } | LifecycleInput::ModelsCleared { erase_id, .. } => (erase_id, Ok(())),
                    LifecycleInput::ClearFailed { erase_id, .. } => (erase_id, Err(LifecycleError::Storage)),
                    _ => (u64::MAX, Ok(())),
                };
                if let Some(erase) = take_erase(&mut erases, erase_id) {
                    reply_all(erase.replies, result, fx);
                    if waiting.is_empty() && erases.is_empty() {
                        store_closing = true;
                        fx.push(LifecycleEffect::StoreClose);
                    }
                }
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::StoreClosed) => {
                if store_closing && observers_stopped {
                    reply_all(close_replies, Ok(()), fx);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed: store_closed || store_closing, observers_stopped }
                }
            }
            (LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, .. }, LifecycleInput::ObserversStopped) => {
                if store_closed {
                    reply_all(close_replies, Ok(()), fx);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped: true }
                }
            }
            (LifecyclePhase::Closing { waiting, erases, mut close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::Close { reply }) => {
                close_replies.push(reply);
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (phase @ LifecyclePhase::Closing { .. }, LifecycleInput::SessionReset { reply } | LifecycleInput::ResetModels { reply, .. }) => {
                reply_all(reply.into_iter().collect(), Err(LifecycleError::Closed), fx);
                phase
            }
            (phase @ LifecyclePhase::Closing { .. }, _) => phase,

            (LifecyclePhase::Closed, LifecycleInput::SessionReset { reply } | LifecycleInput::ResetModels { reply, .. }) => {
                reply_all(reply.into_iter().collect(), Err(LifecycleError::Closed), fx);
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, LifecycleInput::Close { reply }) => {
                reply_all(vec![reply], Ok(()), fx);
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, _) => LifecyclePhase::Closed,
        };
    }

    /// `life_owner_started`, `life_restore_classified` and their stray row.
    fn restore(&mut self, owner: Peer, epoch: Epoch, started: bool, fx: &mut Vec<LifecycleEffect>) {
        if matches!(self.phase, LifecyclePhase::Closing { .. } | LifecyclePhase::Closed) {
            return;
        }
        let Some(restoring) = &mut self.restoring else { return };
        if restoring.epoch != epoch {
            return;
        }
        if started {
            if restoring.start_waiting.remove(&owner) && restoring.start_waiting.is_empty() {
                fx.push(LifecycleEffect::ReadIntents { epoch });
            }
            return;
        }
        if !restoring.restore_waiting.remove(&owner) || !restoring.restore_waiting.is_empty() {
            return;
        }
        self.finish_restore(epoch, fx);
    }

    /// `life_saved`, `life_saved_stale`, `life_save_failed`, `life_save_failed_stale`, `life_closing_saved` and the stray rows.
    fn saved(&mut self, id: PublicationId, ok: bool, fx: &mut Vec<LifecycleEffect>) {
        let Some(at) = self.publishing.iter().position(|publication| publication.id == id) else { return };
        if matches!(self.phase, LifecyclePhase::Closed) {
            return;
        }
        let publication = self.publishing.remove(at);
        if matches!(self.phase, LifecyclePhase::Closing { .. }) {
            reply_all(publication.replies, if ok { Ok(()) } else { Err(LifecycleError::Storage) }, fx);
            return;
        }
        let newer = self.cell_publication > id;
        let restricting = restricting(publication.published.state);
        if ok {
            let ended = self.ended_up_to.is_some_and(|ended| publication.published.epoch <= ended);
            if !restricting && (ended || newer) {
                reply_all(publication.replies, Err(LifecycleError::Interrupted), fx);
                return;
            }
            if !restricting {
                self.cell = publication.published.clone();
                self.cell_publication = id;
                fx.push(LifecycleEffect::PublishCell(publication.published.clone()));
            }
            reply_all(publication.replies, Ok(()), fx);
            if let Some(Started { epoch, identity, generation }) = publication.started {
                fx.push(LifecycleEffect::EpochStarted { epoch, identity, generation });
                self.restore_progress(epoch, fx);
            }
            return;
        }
        reply_all(publication.replies, Err(LifecycleError::Storage), fx);
        let opening = publication.started.as_ref().map(|started| started.epoch);
        if let Some(restoring) = self.restoring.take_if(|restoring| opening == Some(restoring.epoch)) {
            reply_all(restoring.open_reply.into_iter().collect(), Err(LifecycleError::Storage), fx);
        }
        if !newer {
            self.cell.state = PublishedState::StorageFailed;
            fx.push(LifecycleEffect::PublishCell(self.cell.clone()));
        }
    }

    /// `life_executor_failed`.
    fn executor_failed(&mut self, fx: &mut Vec<LifecycleEffect>) {
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Closed);
        self.phase = match phase {
            LifecyclePhase::Closed => LifecyclePhase::Closed,
            LifecyclePhase::Closing { waiting, mut erases, close_replies, store_closing, store_closed, observers_stopped } => {
                let mut open = Vec::new();
                for erase in &mut erases { open.append(&mut erase.replies); }
                for publication in &mut self.publishing { open.append(&mut publication.replies); }
                reply_all(open, Err(LifecycleError::Runtime), fx);
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            phase => {
                let (end, erases, mut open) = match phase {
                    LifecyclePhase::NoSession { epoch } | LifecyclePhase::Session { epoch, .. } => (Some(epoch), Vec::new(), Vec::new()),
                    LifecyclePhase::Resetting { epoch, erases, queued_replies, .. } => (Some(epoch), erases, queued_replies),
                    LifecyclePhase::Ending { erases, .. } => (None, erases, Vec::new()),
                    _ => (None, Vec::new(), Vec::new()),
                };
                for erase in &erases {
                    open.extend(erase.replies.iter().copied());
                }
                for publication in &mut self.publishing {
                    open.append(&mut publication.replies);
                }
                self.drop_restoring(Err(LifecycleError::Runtime), fx);
                reply_all(open, Err(LifecycleError::Runtime), fx);
                let erases: Vec<Erase> = erases.into_iter().map(|erase| Erase { replies: Vec::new(), ..erase }).collect();
                self.closing(erases, Vec::new(), end, fx)
            }
        };
    }
}
