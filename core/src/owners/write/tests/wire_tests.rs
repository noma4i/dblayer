use super::*;

/// transfer.md:13 (D-280): the abort of a multipart debt is identified by its upload id alone, the key of the row plays no part.
#[test]
fn exec_xfer_abort_identity_by_upload_id() {
    assert_eq!(wire::abort_request_id("u1"), wire::abort_request_id("u1"), "the same debt from two keys is one pair");
    assert_ne!(wire::abort_request_id("u1"), wire::abort_request_id("u2"), "two debts are two pairs");
}
