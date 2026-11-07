use super::upload_stand::{Answer, Script};

fn body_of(answer: Option<Answer>) -> Option<String> {
    match answer {
        Some(Answer::Data(text)) => Some(text),
        _ => None,
    }
}

#[test]
fn a_repeated_pair_gets_the_first_answer_and_a_new_pair_hits_unscripted() {
    let script = Script::default();
    script.data("CreateBlob", "first");
    script.data("CreateBlob", "second");

    assert_eq!(body_of(script.answer("CreateBlob", "r1", Some(4))).as_deref(), Some("first"));
    assert_eq!(body_of(script.answer("CreateBlob", "r1", Some(4))).as_deref(), Some("first"));
    assert_eq!(body_of(script.answer("CreateBlob", "r2", Some(5))).as_deref(), Some("second"));
    assert!(script.answer("CreateBlob", "r3", Some(6)).is_none());
}
