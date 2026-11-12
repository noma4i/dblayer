use super::*;
use std::collections::VecDeque;

fn reaction(effect: &RefEffect, floor: u64) -> Option<Input> {
    Some(match effect {
        RefEffect::GenerateCredential { op } => Input::CredentialGenerated { op: *op, credential: 42 },
        RefEffect::StoreSecret { op, .. } => Input::SecretStored { op: *op, ok: true },
        RefEffect::SealProbe { seal_id, .. } => Input::Sealed { seal_id: *seal_id, size: 1 },
        RefEffect::CommitPending { epoch, .. } if *epoch <= floor => rejected(Reason::NoSession),
        RefEffect::CommitPending { .. } | RefEffect::CommitLanding { .. } => committed(),
        RefEffect::CredentialCommitted(identity) => Input::Published { epoch: EPOCH + 2, identity: Some(*identity), state: PubState::Session },
        RefEffect::HttpRequest { pending, attempt_id } => Input::Response { request_id: pending.request_id, attempt_id: *attempt_id, body: "{}".into(), sequence: Some(pending.sequence), refusal: None },
        _ => return None,
    })
}

fn answer_effects(twin: &mut Twin, first: Vec<RefEffect>, floor: u64) {
    let mut queue: VecDeque<RefEffect> = first.into();
    for _ in 0..40 {
        let Some(effect) = queue.pop_front() else { return };
        if let Some(input) = reaction(&effect, floor) { queue.extend(twin.feed(input)); }
    }
}

fn pairs_after(twin: &Twin, from: usize) -> Vec<u64> {
    twin.history[from..].iter().filter_map(|effect| match effect { RefEffect::CommitPending { epoch, .. } => Some(*epoch), _ => None }).collect()
}

fn next_epoch(state: PubState) -> Input { Input::Published { epoch: EPOCH + 1, identity: None, state } }

#[test]
fn sess_idle_call_after_epoch_end() {
    for operation in ["login", "visit"] {
        let mut twin = Twin::new();
        twin.feed(ended(EndCause::SessionReset));
        let from = twin.history.len();
        assert!(twin.feed(call_of(operation, 1)).is_empty(), "{operation}: the call of an ended epoch starts nothing before a newer Published");
        assert!(twin.feed(published(PubState::Ending)).is_empty(), "{operation}: Published(Ending) starts nothing");
        assert!(twin.replies.is_empty(), "{operation}: no answer before the new epoch");
        let mut effects = twin.feed(next_epoch(PubState::NoSession));
        effects.extend(twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true }));
        answer_effects(&mut twin, effects, EPOCH);
        assert_eq!(pairs_after(&twin, from).iter().filter(|epoch| **epoch <= EPOCH).count(), 0, "{operation}: no pair of the ended epoch is written: {:?}", twin.history);
        assert_eq!(twin.replies.iter().filter(|(id, _)| *id == 1).count(), 1, "{operation}: the call gets exactly one outcome: {:?}", twin.replies);
    }
}

#[test]
fn sess_idle_mutate_after_epoch_end() {
    let mut twin = Twin::new();
    twin.feed(ended(EndCause::Other));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::NoSession))], "a Mutate of an ended epoch answers NoSession and writes nothing");
}

fn orders(items: &mut Vec<usize>, left: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
    if left.is_empty() { out.push(items.clone()); return; }
    for at in 0..left.len() {
        let item = left[at];
        let blocked = item == 2 && left.contains(&1);
        if blocked { continue; }
        left.remove(at);
        items.push(item);
        orders(items, left, out);
        items.pop();
        left.insert(at, item);
    }
}

#[test]
fn sess_prop_no_work_in_an_ended_epoch() {
    let mut all = Vec::new();
    orders(&mut Vec::new(), &mut vec![0, 1, 2, 3], &mut all);
    assert_eq!(all.len(), 12);
    for order in all {
        let mut twin = Twin::new();
        let mut from = None;
        for step in &order {
            let effects = match step {
                0 => { from = Some(twin.history.len()); twin.feed(ended(EndCause::SessionReset)) }
                1 => twin.feed(published(PubState::Ending)),
                2 => { let mut effects = twin.feed(next_epoch(PubState::NoSession)); effects.extend(twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true })); effects }
                _ => twin.feed(call_of("login", 1)),
            };
            answer_effects(&mut twin, effects, EPOCH);
        }
        let from = from.expect("EpochEnded was fed");
        assert!(pairs_after(&twin, from).iter().all(|epoch| *epoch > EPOCH), "{order:?}: a pair of the ended epoch is written: {:?}", twin.history);
        assert_eq!(twin.replies.iter().filter(|(id, _)| *id == 1).count(), 1, "{order:?}: the call gets exactly one outcome: {:?}", twin.replies);
    }
}
