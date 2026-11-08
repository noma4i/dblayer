use super::*;
#[test]
fn empty_result_uses_empty_policy_or_falls_back_to_landed() {
    assert_eq!(selected_policy(true, "push", Some("emptyRetry")), "emptyRetry");
    assert_eq!(selected_policy(true, "push", None), "push");
    assert_eq!(selected_policy(false, "push", Some("emptyRetry")), "push");
}
