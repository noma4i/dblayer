use super::calls::CallKind;

#[test]
fn a_document_declares_its_kind_by_its_first_token() {
    for (document, kind) in [
        ("query Feed { feed { id } }", Some(CallKind::Query)),
        ("  \n# note\n, mutation Create($a: Int) { create(a: $a) { id } }", Some(CallKind::Mutate)),
        ("{ feed { id } }", Some(CallKind::Query)),
        ("subscription Changes { changed { id } }", None),
        ("mutationLike { x }", None),
        ("", None),
    ] {
        assert_eq!(CallKind::declared_by(document), kind, "{document}");
    }
}
