use super::check_source;

const CHILD: &str = "model Sample do\n  attribute :ownerId, :id\n  attribute :createdAt, :date\n  attribute :seq, :int\n  attribute :other, :string, null: true\n  index :ownerId\n";

fn source(owner: &str, association: &str) -> String {
    format!("model Owner do\n{owner}end\n\n{CHILD}  {association}\nend\n")
}

fn refused(name: &str, owner: &str, association: &str, on: &str, field: &str) {
    let text = source(owner, association);
    let line = text.lines().position(|row| row.contains(on)).unwrap_or_else(|| panic!("{name}: no line with `{on}`")) + 1;
    match check_source(&text) {
        Ok(_) => panic!("{name}: accepted"),
        Err(errors) => assert!(errors.iter().any(|error| error.line == line && error.message.contains(field)), "{name}: expected `{field}` on line {line}, got {errors:?}"),
    }
}

const SETS: &str = "belongs_to :owner, class_name: Owner, foreign_key: :ownerId, sets: { lastId: :id }";

#[test]
fn gen_derived_field_has_one_writer() {
    let plain = "  attribute :lastId, :id, derived: true\n";
    let ok = check_source(&source(plain, SETS)).unwrap();
    let field = serde_json::to_value(ok[0].fields.iter().find(|field| field.name == "lastId").unwrap()).unwrap();
    assert_eq!((field["nullable"].clone(), field["derived"].clone()), (true.into(), true.into()));
    for (name, owner, association, on, field) in [
        ("with null", "  attribute :lastId, :id, derived: true, null: true\n", SETS, "attribute :lastId", "lastId"),
        ("with optional", "  attribute :lastId, :id, derived: true, optional: true\n", SETS, "attribute :lastId", "lastId"),
        ("with default", "  attribute :lastId, :id, derived: true, default: \"x\"\n", SETS, "attribute :lastId", "lastId"),
        ("with from", "  attribute :lastId, :id, derived: true, from: \"last\"\n", SETS, "attribute :lastId", "lastId"),
        ("with local", "  attribute :lastId, :id, derived: true, local: true\n", SETS, "attribute :lastId", "lastId"),
        ("with merge", "  attribute :lastId, :id, derived: true, merge: :present\n", SETS, "attribute :lastId", "lastId"),
        ("with id_from", "  attribute :lastId, :id, derived: true, id_from: :lastId\n", SETS, "attribute :lastId", "lastId"),
        ("named by no sets", "  attribute :lastId, :id, derived: true\n  attribute :lonely, :int, derived: true\n", SETS, "attribute :lonely", "lonely"),
        ("named by two sets", "  attribute :lastId, :id, derived: true\n  attribute :lastId2, :id, derived: true\n", "belongs_to :owner, class_name: Owner, foreign_key: :ownerId, sets: { lastId: :id, lastId2: :id }\n  belongs_to :second, class_name: Owner, foreign_key: :ownerId, sets: { lastId: :id }", "attribute :lastId,", "lastId"),
        ("target without derived", "  attribute :lastId, :id, null: true\n", SETS, "belongs_to :owner", "lastId"),
        ("type differs from the source", "  attribute :lastId, :int, derived: true\n", SETS, "belongs_to :owner", "lastId"),
        ("unknown source field", "  attribute :lastId, :id, derived: true\n  attribute :lastAt, :date, derived: true\n", "belongs_to :owner, class_name: Owner, foreign_key: :ownerId, sets: { lastId: :id, lastAt: :ghost }", "belongs_to :owner", "ghost"),
    ] {
        refused(name, owner, association, on, field);
    }
    let before_answer = "  attribute :lastId, :id, derived: true\n  operation :act do\n    mutation \"M\"\n    optimistic lastId: \"x\"\n  end\n";
    refused("effect before the answer", before_answer, SETS, "optimistic lastId", "lastId");
}

#[test]
fn gen_newer_is_a_list_of_child_fields() {
    let owner = "  attribute :lastId, :id, derived: true\n  attribute :lastAt, :date, derived: true\n  attribute :lastSeq, :int, derived: true\n";
    let sets = "sets: { lastId: :id, lastAt: :createdAt, lastSeq: :seq }";
    let make = |newer: &str| format!("belongs_to :owner, class_name: Owner, foreign_key: :ownerId, {sets}{newer}");
    let kept = check_source(&source(owner, &make(", newer: [:seq, :createdAt]"))).unwrap().remove(1);
    assert_eq!(serde_json::to_value(&kept.associations[0]).unwrap()["newer"], serde_json::json!(["seq", "createdAt"]));
    for (name, newer, field) in [
        ("pairs", ", newer: { seq: :lastSeq }", "newer"),
        ("unknown child field", ", newer: [:ghost]", "ghost"),
    ] {
        refused(name, owner, &make(newer), "belongs_to :owner", field);
    }
    refused("newer without sets", owner, "belongs_to :owner, class_name: Owner, foreign_key: :ownerId, newer: [:seq]", "belongs_to :owner", "newer");
}
