use super::upload_accepted::AcceptedUpload;
use super::upload_round::{RoundStep, UploadRound};

fn item(seq: i64) -> Vec<AcceptedUpload> {
    vec![AcceptedUpload { seq, position: 0 }]
}

#[test]
fn a_round_that_was_abandoned_or_replaced_cannot_end_the_current_round() {
    let mut round = UploadRound::default();
    let old = round.begin().unwrap();
    round.abandon(old);
    let current = round.begin().unwrap();
    assert_ne!(old, current);
    assert_eq!(round.next(old), RoundStep::Stale);
    assert_eq!(round.leave(old), RoundStep::Stale);
    assert_eq!(round.end(old), None);
    round.abandon(old);
    assert_eq!(round.begin(), None, "the current round must still run");
    assert_eq!(round.next(current), RoundStep::Done);
}

#[test]
fn a_running_round_looks_again_instead_of_asking_for_a_new_one() {
    let mut round = UploadRound::default();
    assert!(round.accept(&item(1)));
    let owner = round.begin().unwrap();
    assert!(!round.accept(&item(2)), "a running round still sees the upload");
    assert_eq!(round.next(owner), RoundStep::More);
    assert_eq!(round.next(owner), RoundStep::Done);
}

#[test]
fn uploads_accepted_while_a_round_ends_come_back_from_end() {
    let mut round = UploadRound::default();
    let owner = round.begin().unwrap();
    assert_eq!(round.next(owner), RoundStep::Done);
    assert!(round.accept(&item(3)));
    assert_eq!(round.end(owner), Some(item(3)));
    assert!(round.begin().is_some(), "the ended round is over");
}
