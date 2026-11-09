use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundRefusal { JobRefused, StartNotAllowed, SchedulerFailed }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundsIn {
    Needed { epoch: u64, ids: Vec<TransferId> },
    Idle { epoch: u64, transfer_id: TransferId },
    Started { request_id: u64, round_id: u64, epoch: u64 },
    Refused { request_id: u64, epoch: u64, kind: RoundRefusal, detail: String },
    Ended { round_id: u64, epoch: u64 },
    JobsSinkSet,
    Foreground,
    EpochStarted(u64),
    EpochEnded(u64),
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RState {
    None,
    Requested { e: u64, request_id: u64, round_id: u64 },
    Running { request_id: u64, round_id: u64, e: u64, next: Option<u64>, ended: bool },
    Refused { e: u64 },
    Stopped { e: u64 },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundsState { pub sink: bool, pub current_epoch: u64, pub active: BTreeMap<u64, BTreeSet<TransferId>>, pub st: RState, pub next_request: u64, pub next_round: u64 }

impl RoundsState {
    pub fn new() -> RoundsState { RoundsState { sink: false, current_epoch: 0, active: BTreeMap::new(), st: RState::None, next_request: 1, next_round: 1 } }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundsOut {
    Row(&'static str),
    Undefined { row: String, reason: &'static str },
    ScheduleJob { request_id: u64, round_id: u64, epoch: u64 },
    EndJob(u64),
    TransferRoundStatus { epoch: u64, ids: Vec<TransferId>, refused: Option<String> },
    Drained,
}

pub fn active_of(s: &RoundsState, e: u64) -> BTreeSet<TransferId> { s.active.get(&e).cloned().unwrap_or_default() }

pub fn request(s: &mut RoundsState, e: u64, o: &mut Vec<RoundsOut>) {
    let q = s.next_request;
    s.next_request += 1;
    let r = s.next_round;
    s.next_round += 1;
    s.st = RState::Requested { e, request_id: q, round_id: r };
    o.push(RoundsOut::ScheduleJob { request_id: q, round_id: r, epoch: e });
}

pub fn rounds_step(s: &RoundsState, i: &RoundsIn) -> (RoundsState, Vec<RoundsOut>) {
    let mut s = s.clone();
    let mut o = Vec::new();
    rounds_apply(&mut s, i, &mut o);
    (s, o)
}

pub fn rounds_apply(s: &mut RoundsState, i: &RoundsIn, o: &mut Vec<RoundsOut>) {
    use RoundsOut::Row;
    if s.st == RState::Closed {
        if let RoundsIn::Started { round_id, .. } = i { o.push(Row("xfer_round_closed_started")); o.push(RoundsOut::EndJob(*round_id)); }
        else { o.push(Row("xfer_round_closed")); }
        return;
    }
    match i {
        RoundsIn::EpochStarted(e) => { s.current_epoch = *e; o.push(Row("xfer_round_epoch_started")); }
        RoundsIn::Closing => {
            o.push(Row("xfer_round_closing"));
            if let RState::Running { round_id, .. } = &s.st { o.push(RoundsOut::EndJob(*round_id)); }
            s.st = RState::Closed;
            o.push(RoundsOut::Drained);
        }
        RoundsIn::JobsSinkSet => {
            let was = s.sink;
            s.sink = true;
            if !was && s.st == RState::None && !active_of(s, s.current_epoch).is_empty() {
                o.push(Row("xfer_round_sink_set"));
                let e = s.current_epoch;
                request(s, e, o);
            } else { o.push(Row("xfer_round_sink_set_idle")); }
        }
        RoundsIn::Needed { epoch, ids } => {
            if *epoch != s.current_epoch { o.push(Row("xfer_round_request_stale")); return; }
            let e = *epoch;
            s.active.entry(e).or_default().extend(ids.iter().cloned());
            match s.st.clone() {
                RState::None => {
                    if !s.sink { o.push(Row("xfer_round_no_sink")); } else { o.push(Row("xfer_round_request")); request(s, e, o); }
                }
                RState::Requested { e: e1, .. } => {
                    if e1 == e { o.push(Row("xfer_round_needed_requested")); } else { o.push(Row("xfer_round_requested_new_epoch")); request(s, e, o); }
                }
                RState::Running { request_id, round_id, e: e1, ended, .. } => {
                    if e1 == e { o.push(Row("xfer_round_needed_requested")); }
                    else { o.push(Row("xfer_round_needed_new_epoch")); s.st = RState::Running { request_id, round_id, e: e1, next: Some(e), ended }; }
                }
                RState::Refused { e: e1 } => {
                    if e1 == e {
                        o.push(Row("xfer_round_refused_resume"));
                        request(s, e, o);
                        o.push(RoundsOut::TransferRoundStatus { epoch: e, ids: active_of(s, e).into_iter().collect(), refused: None });
                    } else { o.push(Row("xfer_round_refused_new_epoch")); request(s, e, o); }
                }
                RState::Stopped { e: e1 } => {
                    if e1 == e { o.push(Row("xfer_round_stopped_resume")); request(s, e, o); }
                    else { o.push(Row("xfer_round_refused_new_epoch")); request(s, e, o); }
                }
                RState::Closed => {}
            }
        }
        RoundsIn::Foreground => {
            match s.st.clone() {
                RState::None => {
                    if !s.sink { o.push(Row("xfer_round_foreground_no_sink")); }
                    else if !active_of(s, s.current_epoch).is_empty() { o.push(Row("xfer_round_foreground")); let e = s.current_epoch; request(s, e, o); }
                    else { o.push(Row("xfer_round_foreground_idle")); }
                }
                RState::Requested { .. } | RState::Running { .. } => o.push(Row("xfer_round_foreground_busy")),
                RState::Refused { e } => {
                    if e == s.current_epoch {
                        o.push(Row("xfer_round_refused_resume"));
                        request(s, e, o);
                        o.push(RoundsOut::TransferRoundStatus { epoch: e, ids: active_of(s, e).into_iter().collect(), refused: None });
                    } else { o.push(Row("xfer_round_resume_old_epoch")); }
                }
                RState::Stopped { e } => {
                    if e == s.current_epoch { o.push(Row("xfer_round_stopped_resume")); request(s, e, o); }
                    else { o.push(Row("xfer_round_resume_old_epoch")); }
                }
                RState::Closed => {}
            }
        }
        RoundsIn::Started { request_id: q, round_id: r, epoch: e2 } => {
            match s.st.clone() {
                RState::None | RState::Refused { .. } | RState::Stopped { .. } => { o.push(Row("xfer_round_unexpected_started")); o.push(RoundsOut::EndJob(*r)); }
                RState::Requested { e, request_id, .. } => {
                    if *q == request_id && *e2 == e {
                        if active_of(s, e).is_empty() {
                            o.push(Row("a_job_that_starts_with_nothing_unfinished_ends_at_once"));
                            s.st = RState::None;
                            o.push(RoundsOut::EndJob(*r));
                        } else {
                            o.push(Row("xfer_round_started"));
                            s.st = RState::Running { request_id, round_id: *r, e, next: None, ended: false };
                        }
                    } else { o.push(Row("xfer_round_started_stale")); o.push(RoundsOut::EndJob(*r)); }
                }
                RState::Running { request_id, round_id, e, .. } => {
                    if *q == request_id && *r == round_id && *e2 == e { o.push(Row("xfer_round_started_duplicate")); }
                    else { o.push(Row("xfer_round_started_during_running")); o.push(RoundsOut::EndJob(*r)); }
                }
                RState::Closed => {}
            }
        }
        RoundsIn::Refused { request_id: q, kind, detail, .. } => {
            match s.st.clone() {
                RState::Requested { e, request_id, .. } if *q == request_id => {
                    o.push(Row("xfer_round_refused"));
                    s.st = RState::Refused { e };
                    let reason = format!("{:?}: {}", kind, detail);
                    o.push(RoundsOut::TransferRoundStatus { epoch: e, ids: active_of(s, e).into_iter().collect(), refused: Some(reason) });
                }
                _ => o.push(Row("xfer_round_refused_stale")),
            }
        }
        RoundsIn::Idle { epoch, transfer_id } => {
            let e = *epoch;
            if let Some(x) = s.active.get_mut(&e) { x.remove(transfer_id); }
            let empty = active_of(s, e).is_empty();
            match s.st.clone() {
                RState::Running { round_id, e: e0, .. } if e0 == e && empty => {
                    o.push(Row("a_running_round_ends_its_job_when_the_last_transfer_of_its_epoch_goes_idle"));
                    s.st = RState::None;
                    o.push(RoundsOut::EndJob(round_id));
                }
                _ => o.push(Row("xfer_round_idle")),
            }
        }
        RoundsIn::Ended { round_id: r, epoch: e2 } => {
            match s.st.clone() {
                RState::Running { round_id, e, next, .. } => {
                    if *r == round_id && *e2 == e {
                        if let Some(n) = next.filter(|n| !active_of(s, *n).is_empty()) {
                            o.push(Row("xfer_round_end_next"));
                            request(s, n, o);
                        } else if !active_of(s, e).is_empty() {
                            o.push(Row("xfer_round_end_stopped"));
                            s.st = RState::Stopped { e };
                        } else {
                            o.push(Row("xfer_round_end"));
                            s.st = RState::None;
                        }
                    } else { o.push(Row("xfer_round_end_stale")); }
                }
                _ => o.push(Row("xfer_round_end_unknown")),
            }
        }
        RoundsIn::EpochEnded(e) => {
            s.active.insert(*e, BTreeSet::new());
            match s.st.clone() {
                RState::Requested { e: e0, .. } | RState::Refused { e: e0 } | RState::Stopped { e: e0 } if e0 == *e => {
                    o.push(Row("xfer_round_epoch_ended"));
                    s.st = RState::None;
                }
                RState::Running { request_id, round_id, e: e0, next, .. } if e0 == *e => {
                    o.push(Row("xfer_round_running_epoch_ended"));
                    let _ = next;
                    s.st = RState::Running { request_id, round_id, e: e0, next: None, ended: true };
                }
                RState::Running { request_id, round_id, e: e0, next, ended } => {
                    o.push(Row("xfer_round_epoch_ended_other"));
                    let next = if next == Some(*e) { None } else { next };
                    s.st = RState::Running { request_id, round_id, e: e0, next, ended };
                }
                _ => o.push(Row("xfer_round_epoch_ended_other")),
            }
        }
    }
}
