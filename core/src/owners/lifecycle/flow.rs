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
                self.starting = Some(Started { epoch, identity: saved_identity, generation: saved_generation });
                if saved_identity.is_some() {
                    self.restoring = Some(self.new_restoring(epoch, reply));
                    self.publish(Vec::new(), fx);
                } else if saved_unowned_pending {
                    self.restoring = Some(Restoring { epoch, start_waiting: Default::default(), restore_waiting: [Peer::Session].into(), open_reply: reply });
                    self.publish(Vec::new(), fx);
                } else {
                    self.open_reply = reply;
                    self.publish(reply.into_iter().collect(), fx);
                }
                return;
            }
            LifecycleInput::Saved { publication_id } => return self.saved(publication_id, true, fx),
            LifecycleInput::SaveFailed { publication_id, .. } => return self.saved(publication_id, false, fx),
            LifecycleInput::ExecutorFailed { .. } => return self.executor_failed(fx),
            LifecycleInput::Online(true) | LifecycleInput::Foreground if matches!(self.phase, LifecyclePhase::NoSession { .. } | LifecyclePhase::Session { .. }) => return self.storage_failed_retried(fx),
            LifecycleInput::Online(false) => return,
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
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::Close { reply }) => self.closing(Vec::new(), vec![reply], Some(epoch), fx),
            (phase @ LifecyclePhase::Session { .. }, _) => phase,

            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                if failed {
                    self.erase_again(epoch_old, cause, Vec::new(), &mut erases, fx);
                }
                LifecyclePhase::Ending { epoch_old, cause, next: Some(identity), failed: false, erase_credential, erases }
            }
            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::SessionReset { reply }) => {
                if failed {
                    self.erase_again(epoch_old, Cause::SessionReset, reply.into_iter().collect(), &mut erases, fx);
                    LifecyclePhase::Ending { epoch_old, cause: Cause::SessionReset, next: None, failed: false, erase_credential: false, erases }
                } else {
                    let credential = erase_credential || cause == Cause::IdentityChanged;
                    if let Some(account) = erases.last_mut() {
                        account.replies.extend(reply);
                    }
                    LifecyclePhase::Ending { epoch_old, cause, next: None, failed: false, erase_credential: credential, erases }
                }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::Cleared { erase_id }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) if erase_credential => {
                    self.published.generation = self.published.generation.max(erase.target);
                    let second = self.new_erase(erase.replies);
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
                    self.published.state = PublishedState::EndingFailed;
                    self.publish(Vec::new(), fx);
                    LifecyclePhase::Ending { epoch_old, cause, next, failed: true, erase_credential, erases }
                }
            },
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::Online(true) | LifecycleInput::Foreground) => {
                let retry = failed && erases.is_empty();
                if retry {
                    self.erase_again(epoch_old, cause, Vec::new(), &mut erases, fx);
                }
                LifecyclePhase::Ending { epoch_old, cause, next, failed: failed && !retry, erase_credential, erases }
            }
            (LifecyclePhase::Ending { erases, .. }, LifecycleInput::Close { reply }) => self.closing(erases, vec![reply], None, fx),
            (phase @ LifecyclePhase::Ending { .. }, _) => phase,

            (LifecyclePhase::Closing { mut waiting, mut erases, mut close_replies, mut store_closing, mut store_closed, mut observers_stopped }, input @ (LifecycleInput::Drained(_) | LifecycleInput::Cleared { .. } | LifecycleInput::ClearFailed { .. } | LifecycleInput::StoreClosed | LifecycleInput::ObserversStopped | LifecycleInput::Close { .. })) => {
                match input {
                    LifecycleInput::Drained(Drained { owner }) => {
                        if let Some(peer) = Peer::named(owner) {
                            waiting.remove(&peer);
                        }
                    }
                    LifecycleInput::Cleared { erase_id } => {
                        if let Some(erase) = take_erase(&mut erases, erase_id) {
                            reply_all(erase.replies, Ok(()), fx);
                        }
                    }
                    LifecycleInput::ClearFailed { erase_id, .. } => {
                        if let Some(erase) = take_erase(&mut erases, erase_id) {
                            reply_all(erase.replies, Err(LifecycleError::Storage), fx);
                        }
                    }
                    LifecycleInput::StoreClosed => store_closed = store_closed || store_closing,
                    LifecycleInput::ObserversStopped => observers_stopped = true,
                    LifecycleInput::Close { reply } => close_replies.push(reply),
                    _ => {}
                }
                if waiting.is_empty() && erases.is_empty() && !store_closing {
                    store_closing = true;
                    fx.push(LifecycleEffect::StoreClose);
                }
                if store_closed && observers_stopped {
                    reply_all(close_replies, Ok(()), fx);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
                }
            }
            (phase @ LifecyclePhase::Closing { .. }, LifecycleInput::SessionReset { reply }) => {
                reply_all(reply.into_iter().collect(), Err(LifecycleError::Closed), fx);
                phase
            }
            (phase @ LifecyclePhase::Closing { .. }, _) => phase,

            (LifecyclePhase::Closed, LifecycleInput::SessionReset { reply }) => {
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
            if let Some(Started { epoch, identity, generation }) = self.starting.clone().filter(|starting| !restricting && starting.epoch == publication.published.epoch) {
                self.starting = None;
                fx.push(LifecycleEffect::EpochStarted { epoch, identity, generation });
                self.restore_progress(epoch, fx);
            }
            return;
        }
        reply_all(publication.replies, Err(LifecycleError::Storage), fx);
        if let Some(open_reply) = self.restoring.as_mut().and_then(|restoring| restoring.open_reply.take()) {
            reply_all(vec![open_reply], Err(LifecycleError::Storage), fx);
        }
        if !newer && (restricting || publication.published != self.cell) {
            self.cell.state = PublishedState::StorageFailed;
            fx.push(LifecycleEffect::PublishCell(self.cell.clone()));
        }
    }

    /// `life_ending_failed_retried_on_event`, `life_ending_credential_after_failure`, `life_ending_reset_after_failure`: the erase of the account is written again.
    fn erase_again(&mut self, epoch_old: Epoch, cause: Cause, replies: Vec<ReplyId>, erases: &mut Vec<Erase>, fx: &mut Vec<LifecycleEffect>) {
        self.published.state = PublishedState::Ending;
        self.publish(Vec::new(), fx);
        let erase = self.new_erase(replies);
        fx.push(LifecycleEffect::ClearAccount { epoch_old, cause, generation_new: erase.target, erase_id: erase.id });
        erases.push(erase);
    }

    /// `life_storage_failed_retried_on_event`, `life_network_event_ignored`: a refused save of the active publication is written again once per event, `Saved` leaves `StorageFailed`.
    fn storage_failed_retried(&mut self, fx: &mut Vec<LifecycleEffect>) {
        if self.cell.state == PublishedState::StorageFailed {
            self.publish(Vec::new(), fx);
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
