use super::*;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use serde_json::json;

#[test]
fn a_union_field_compares_by_its_typename_column() {
    let variant = FieldSpec::variant("gift", "GiftAttachment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let model = ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::union("attachment", "MessageAttachment", vec![variant]).nullable()]);
    let mut bindings = Vec::new();
    let sql = Where::all_of(vec![Where::equal("attachment", json!("GiftAttachment")), Where::is_null("attachment")]).compile(&model, &mut bindings).unwrap();
    assert_eq!(sql, "(r.\"attachment\" = ? AND r.\"attachment\" IS NULL)");
}
