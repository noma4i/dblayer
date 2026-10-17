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
        outcomes.keep(sequence, "moments", vec![rid(&format!("m{sequence}"))], &SendResult::Rejected(OperationFailure::new(FailureKind::Rejected, "no")));
    }
    assert_eq!(outcomes.lost(), 1);
    assert!(outcomes.failure_of("moments", &rid("m0"), &[]).is_none());
    assert!(outcomes.failure_of("moments", &rid("m1"), &[]).is_some());
}
