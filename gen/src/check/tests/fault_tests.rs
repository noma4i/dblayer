use super::*;

#[test]
fn gen_error_not_dropped_for_a_fault_whose_model_has_no_declaration() {
    let fault = dblayer::schema::ModelFault { model: dblayer::identity::ModelKey::new("Ghost"), error: dblayer::schema::DeclarationError::NoFields };
    let found = fault_errors(&[], &[fault]);
    assert_eq!(found.len(), 1);
    assert!(found[0].1.message.contains("model Ghost"), "{:?}", found);
}
