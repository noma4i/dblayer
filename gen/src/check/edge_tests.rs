use dblayer::schema::FieldKind;

use crate::parse_models as check_source;
use dblayer::value::ScalarKind;

fn model(scope: &str) -> String {
    format!("model Moment do\n  field :text, type: :string, null: true\n  scope :feed do\n    fetch MomentsFeed\n{scope}\n  end\nend\n")
}

fn failure(source: &str) -> (usize, String) {
    let errors = check_source(source).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    (errors[0].line, errors[0].message.clone())
}

#[test]
fn edge_lines_of_a_paginated_scope_become_edge_fields_of_the_relation() {
    let source = model("    paginate 20\n    edge :impressionUuid, type: :string, null: true\n    edge :score, type: :int, null: true, from: \"impressionScore\"");
    let models = check_source(&source).unwrap();
    let feed = models[0].relations.iter().find(|relation| relation.name == "feed").unwrap();
    let names: Vec<(&str, Option<&str>)> = feed.edge_fields.iter().map(|field| (field.name.as_str(), field.from.as_deref())).collect();
    assert_eq!(names, [("impressionUuid", None), ("score", Some("impressionScore"))]);
    assert_eq!(feed.edge_fields[1].kind, FieldKind::Scalar { kind: ScalarKind::Int });
    assert!(feed.edge_fields.iter().all(|field| field.nullable && field.merge.is_none()));
    assert!(models[0].fields.iter().all(|field| !field.name.starts_with("impression")), "no column on the model");
}

#[test]
fn an_edge_needs_a_connection_and_room_for_rows_without_a_value() {
    assert_eq!(failure(&model("    list\n    edge :uuid, type: :string, null: true")), (6, "edge `uuid` needs `paginate` in its scope".to_string()));
    assert_eq!(failure(&model("    paginate 20\n    edge :uuid, type: :string")), (6, "edge `uuid` needs `null: true` or `optional: true`".to_string()));
    assert_eq!(failure(&model("    paginate 20\n    edge :uuid, type: :string, null: true, merge: :present")), (6, "edge `uuid` cannot set `merge`, `local`, `index` or `default`".to_string()));
    assert_eq!(failure(&model("    paginate 20\n    edge :uuid, type: :string, null: true\n    edge :uuid, type: :int, null: true")), (7, "edge `uuid` is declared twice".to_string()));
}
