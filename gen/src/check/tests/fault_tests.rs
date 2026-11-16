use super::*;

#[test]
fn gen_error_not_dropped_for_a_fault_whose_model_has_no_declaration() {
    let fault = dblayer::schema::ModelFault { model: dblayer::identity::ModelKey::new("Ghost"), error: dblayer::schema::DeclarationError::NoFields };
    let parsed = crate::parse_tree(&["\n\nmodel Other do\nend\n"]).unwrap();
    let nodes = crate::decl::lower_models(&parsed.models, &parsed.changes, None).unwrap();
    let found = fault_errors(&nodes, &[fault]);
    assert_eq!(found.len(), 1);
    assert!(found[0].1.message.contains("model Ghost"), "{:?}", found);
    assert_eq!((found[0].0, found[0].1.line), (0, 3));
}
