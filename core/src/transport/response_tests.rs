use super::*;

#[test]
fn data_responses_commit_and_responses_with_errors_fail() {
    let data_only = ResponseShape::of(true, 0, false);
    let with_errors = ResponseShape::of(true, 1, false);
    assert_eq!(data_only, ResponseShape::DataOnly);
    assert_eq!(data_only.outcome(), ResponseOutcome::Commit);
    assert_eq!(with_errors, ResponseShape::DataWithErrors);
    assert_eq!(with_errors.outcome(), ResponseOutcome::MarkFailed);
}

#[test]
fn errors_only_empty_and_thrown_never_commit() {
    let errors_only = ResponseShape::of(false, 2, false);
    let empty = ResponseShape::of(false, 0, false);
    let thrown_with_data = ResponseShape::of(true, 3, true);
    let thrown_without_data = ResponseShape::of(false, 0, true);
    assert_eq!(errors_only, ResponseShape::ErrorsOnly);
    assert_eq!(errors_only.outcome(), ResponseOutcome::MarkFailed);
    assert_eq!(empty, ResponseShape::Empty);
    assert_eq!(empty.outcome(), ResponseOutcome::MarkFailed);
    assert_eq!(thrown_with_data, ResponseShape::Thrown);
    assert_eq!(thrown_without_data, ResponseShape::Thrown);
    assert_eq!(thrown_with_data.outcome(), ResponseOutcome::MarkFailed);
}

#[test]
fn shape_of_response_counts_data_and_errors() {
    assert_eq!(Response::data(serde_json::json!({"a": 1})).shape(), ResponseShape::DataOnly);
    let errors = Response {
        data: None,
        errors: vec![GraphQLError { message: "x".into(), path: None, extensions: None }],
    };
    assert_eq!(errors.shape(), ResponseShape::ErrorsOnly);
    assert_eq!(Response { data: None, errors: vec![] }.shape(), ResponseShape::Empty);
}
