use crate::identity::RowId;

use super::outcomes::{Outcomes, UNREAD_LIMIT};
use super::SendResult;
use crate::syncstate::{FailureKind, OperationFailure};

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

#[test]
fn a_full_store_drops_the_oldest_unread_refusal_and_counts_it() {
    let mut outcomes = Outcomes::default();
    for sequence in 0..=UNREAD_LIMIT as i64 {
        outcomes.keep(sequence, "moments", "moments:update", vec![rid(&format!("m{sequence}"))], &SendResult::Rejected(OperationFailure::new(FailureKind::Rejected, "no")));
    }
    assert_eq!(outcomes.lost(), 1);
    assert!(outcomes.failure_of("moments", None, &rid("m0"), &[]).is_none());
    assert!(outcomes.failure_of("moments", None, &rid("m1"), &[]).is_some());
}

#[test]
fn a_wrong_model_asking_for_an_answer_leaves_it_for_the_right_model() {
    let mut outcomes = Outcomes::default();
    outcomes.keep(5, "moments", "moments:update", vec![rid("m1")], &SendResult::Rejected(OperationFailure::new(FailureKind::Rejected, "no")));
    assert!(outcomes.take("users", 5).is_err());
    assert!(matches!(outcomes.take("moments", 5), Err(crate::sync::SyncError::Graphql { .. })));
}
