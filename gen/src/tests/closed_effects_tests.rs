use dblayer::schema::ScopeEffectKind;

use crate::check::check_source;

const MODELS: &str = "model Person do\n  attribute :name, :string\n  scope :club_members do\n    parameter :clubId, :string\n    parameter :order, :string\n    query \"ClubMembers\"\n    response from: \"node.members\"\n    paginate size: 30, direction: :forward\n  end\nend\n\nmodel Club do\n  attribute :name, :string\n";

fn club(operations: &str) -> Result<Vec<dblayer::schema::ModelSpec>, Vec<String>> {
    check_source(&format!("{MODELS}{operations}end\n")).map_err(|errors| errors.into_iter().map(|error| error.message).collect())
}

#[test]
fn the_four_effect_kinds_reach_the_action_declaration() {
    let models = club("  operation :leave do\n    mutation \"LeaveClub\"\n    variables clubId: :id\n    optimistic false\n    removes_receiver\n    response :ignore\n  end\n  operation :add_member do\n    mutation \"AddClubMember\"\n    parameter :userIds, [:id]\n    variables clubId: :id, userIds: :userIds\n    optimistic false\n    joins_scope :club_members, model: Person, rows: :userIds, with: { clubId: :id }, order: :order, orders: [\"JOINED\", \"OWNER_FIRST\"]\n    response from: \"addClubMember\"\n  end\n  operation :copy do\n    mutation \"CopyClub\"\n    parameter :target, :id\n    variables clubId: :id, target: :target\n    optimistic false\n    copies_receiver_to :target, field: :name\n    response from: \"copyClub\"\n  end\n").unwrap();
    let actions = &models.iter().find(|model| model.key.as_str() == "Club").unwrap().actions;
    let action = |name: &str| actions.iter().find(|action| action.name == name).unwrap();
    assert!(action("leave").removes_receiver);
    let join = &action("add_member").scope_effects[0];
    assert_eq!((join.kind, join.scope.as_str(), join.rows.as_str(), join.orders.len()), (ScopeEffectKind::Join, "club_members", "userIds", 2));
    assert_eq!(action("copy").copies_receiver_to.as_ref().map(|copy| (copy.arg.as_str(), copy.field.as_str())), Some(("target", "name")));
}

#[test]
fn an_operation_cannot_combine_effects_or_name_an_unknown_scope() {
    let cases = [
        ("two kinds", "  operation :both do\n    mutation \"Both\"\n    parameter :userIds, [:id]\n    variables clubId: :id\n    optimistic false\n    removes_receiver\n    leaves_scope :club_members, model: Person, rows: :userIds, with: { clubId: :id }, order: :order, orders: [\"JOINED\"]\n    response :ignore\n  end\n", "do not combine"),
        ("unknown scope", "  operation :join do\n    mutation \"Join\"\n    parameter :userIds, [:id]\n    variables clubId: :id\n    optimistic false\n    joins_scope :nothing, model: Person, rows: :userIds, with: { clubId: :id }, order: :order, orders: [\"JOINED\"]\n    response :ignore\n  end\n", "has no scope"),
        ("optimistic fields", "  operation :leave do\n    mutation \"Leave\"\n    variables clubId: :id\n    optimistic name: \"x\"\n    removes_receiver\n    response :ignore\n  end\n", "write `optimistic false`"),
        ("unknown key", "  operation :leave do\n    mutation \"Leave\"\n    variables clubId: :id\n    optimistic false\n    removes_receiver when: :flag\n    response :ignore\n  end\n", "unknown key"),
    ];
    for (case, operations, expected) in cases {
        let errors = club(operations).unwrap_err();
        assert!(errors.iter().any(|message| message.contains(expected)), "{case}: {errors:?}");
    }
}

#[test]
fn a_copy_sets_literals_and_sources_and_names_only_known_attributes() {
    use dblayer::schema::{CopyValue, EffectSource};
    let copy = |set: &str| format!("  operation :copy do\n    mutation \"CopyClub\"\n    parameter :target, :id\n    variables clubId: :id, target: :target\n    optimistic false\n    copies_receiver_to :target, field: :name, set: {set}\n    response from: \"copyClub\"\n  end\n");
    let models = club(&copy("{ name: \"x\" }")).unwrap();
    let declared = models.iter().find(|model| model.key.as_str() == "Club").unwrap().actions.iter().find(|action| action.name == "copy").unwrap();
    assert_eq!(declared.copies_receiver_to.as_ref().unwrap().set.get("name"), Some(&CopyValue::Literal(serde_json::json!("x"))));
    let models = club(&copy("{ name: { owner: :name } }")).unwrap();
    let declared = models.iter().find(|model| model.key.as_str() == "Club").unwrap().actions.iter().find(|action| action.name == "copy").unwrap();
    assert_eq!(declared.copies_receiver_to.as_ref().unwrap().set.get("name"), Some(&CopyValue::Source(EffectSource::Owner("name".into()))));
    let errors = club(&copy("{ nothing: true }")).unwrap_err();
    assert!(errors.iter().any(|message| message.contains("sets unknown attribute")), "{errors:?}");
}
