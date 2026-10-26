use dblayer::schema::{ActionEffect, EffectSource, ModelSpec};
use serde_json::json;

use super::check_source;

const MODELS: &str = "model Person do
  attribute :name, :string
  scope :blocked do
    query \"BlockedPeople\"
    response from: \"viewer.blockedPeople\"
    paginate size: 20, direction: :forward
  end
  scope :roster do
    parameter :roomId, :string
    parameter :order, :string
    query \"RoomRoster\"
    response from: \"node.members\"
    paginate size: 30, direction: :forward
  end
  scope :plain do
    parameter :roomId, :string
    parameter :folder, :string
    query \"Plain\"
    response from: \"node.plain\"
    paginate size: 30, direction: :forward
  end
  scope :local do
    order name: :asc
  end
end

model Wallet do
  attribute :balance, :int
  attribute :label, :string
  scope :current do
    query \"WalletQuery\"
  end
end

model Note do
  attribute :roomId, :id
  attribute :body, :string, null: true
  attribute :kind, :string, null: true
  index :roomId
  scope :in_room do
    parameter :roomId, :id
    where roomId: :roomId
    query \"NoteInRoom\"
    response from: \"node.notes\"
  end
end
";

fn source(operations: &str) -> String {
    format!("{MODELS}\nmodel Room do\n  attribute :title, :string\n  attribute :count, :int\n  scope :open do\n    parameter :folder, :string\n    query \"OpenRooms\"\n    response from: \"viewer.rooms\"\n    paginate size: 25, direction: :forward\n  end\n{operations}end\n")
}

fn op(lines: &str) -> String {
    format!("  operation :act do\n    mutation \"M\"\n{lines}  end\n")
}

fn run(rows: &[(&str, &str, Option<&str>)]) {
    for (name, lines, expected) in rows {
        match (check_source(&source(&op(lines))), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(message)) => panic!("{name}: accepted, expected `{message}`"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|error| error.message.contains(message)), "{name}: {errors:?}"),
        }
    }
}

fn actions(lines: &str) -> Vec<ActionEffect> {
    let models: Vec<ModelSpec> = check_source(&source(&op(lines))).unwrap();
    models.into_iter().find(|model| model.key.as_str() == "Room").unwrap().actions.remove(0).effects
}

#[test]
fn the_four_effect_lines_become_effects_of_the_action() {
    let effects = actions("    parameter :personIds, [:id]\n    parameter :target, :id\n    parameter :price, :int, null: true\n    parameter :wallet, :id\n    add_to :roster, Person, rows: :personIds, with: { roomId: :id, order: \"FIRST\" }\n    remove_from :open\n    create_row Note, roomId: :target, body: { owner: :title }, kind: nil\n    upsert Note, from: \"a.note\"\n    change_row Wallet, balance: { minus: :price }, label: \"x\"\n    change_row Wallet, row: :wallet, balance: 5\n");
    let json = serde_json::to_value(&effects).unwrap();
    assert_eq!(json[0], json!({"kind": "add", "model": "Person", "scope": "roster", "rows": "personIds", "with": {"roomId": {"owner": "id"}, "order": {"literal": "FIRST"}}}));
    assert_eq!(json[1], json!({"kind": "remove", "model": "Room", "scope": "open"}));
    assert_eq!(json[2], json!({"kind": "create", "model": "Note", "values": {"roomId": {"arg": "target"}, "body": {"owner": "title"}, "kind": {"literal": null}}}));
    assert_eq!(json[3], json!({"kind": "change", "model": "Wallet", "values": {"balance": {"minus": "price"}, "label": {"literal": "x"}}}));
    assert_eq!(json[4], json!({"kind": "change", "model": "Wallet", "row": "wallet", "values": {"balance": {"literal": 5}}}));
    assert!(matches!(&effects[3], ActionEffect::Change { values, .. } if values["balance"] == EffectSource::Minus("price".into())));
}

#[test]
fn effect_lines_are_checked_against_the_models_and_the_parameters() {
    run(&[
        ("add_to of the owner", "    add_to :open, with: { folder: \"a\" }\n", None),
        ("remove_from with every parameter", "    remove_from :plain, Person, rows: :ids, with: { roomId: :id, folder: \"a\" }\n    parameter :ids, [:id]\n", None),
        ("remove_from with none", "    remove_from :plain, Person, rows: :ids\n    parameter :ids, [:id]\n", None),
        ("a scope linked by where", "    add_to :in_room, Note, rows: :ids, with: { roomId: :id }\n    parameter :ids, [:id]\n", None),
        ("a repeated line", "    remove_from :open\n    remove_from :open\n", None),
        ("create_row answered by the root of its model", "    create_row Room, title: \"x\"\n    response from: \"a.b\"\n", None),
        ("create_row answered by an upsert", "    create_row Note, roomId: :id\n    upsert Note, from: \"a.note\"\n", None),
        ("optimistic false with an effect", "    optimistic false\n    add_to :open\n", Some("`optimistic false` contradicts the effect lines")),
        ("unknown model", "    add_to :roster, Ghost, rows: :ids\n    parameter :ids, [:id]\n", Some("`add_to` names unknown model `Ghost`")),
        ("unknown scope", "    add_to :ghost\n", Some("`add_to` names `ghost`, which is not a scope with `query` of `Room`")),
        ("local scope", "    add_to :local, Person, rows: :ids\n    parameter :ids, [:id]\n", Some("`add_to` names `local`, which is not a scope with `query` of `Person`")),
        ("rows not declared", "    add_to :blocked, Person, rows: :ids\n", Some("`rows:` names `ids`, declare it with `parameter :ids, [:id]`")),
        ("rows not an id", "    parameter :ids, :string\n    add_to :blocked, Person, rows: :ids\n", Some("`rows:` names `ids`")),
        ("another model without rows", "    add_to :blocked, Person\n", Some("`add_to :blocked` selects `Person` rows: add `rows:`")),
        ("with names no parameter", "    add_to :open, with: { ghost: \"a\" }\n", Some("`with:` names `ghost`, which is not a parameter of scope `open`")),
        ("add_to leaves a parameter unbound", "    add_to :open\n", Some("`add_to :open` needs `with: { folder: ... }` for parameter `folder`")),
        ("remove_from binds some parameters", "    remove_from :plain, Person, rows: :ids, with: { roomId: :id }\n    parameter :ids, [:id]\n", Some("`remove_from :plain` binds all parameters or none")),
        ("value is an undeclared parameter", "    add_to :open, with: { folder: :ghost }\n", Some("value `:ghost` is not declared with `parameter`")),
        ("owner names no attribute", "    create_row Room, title: { owner: :ghost }\n    response from: \"a.b\"\n", Some("`{ owner: :ghost }` names unknown attribute `ghost`")),
        ("create_row unknown attribute", "    create_row Room, ghost: 1\n    response from: \"a.b\"\n", Some("`create_row` names unknown attribute `ghost` of `Room`")),
        ("create_row unknown model", "    create_row Ghost, a: 1\n", Some("`create_row` names unknown model `Ghost`")),
        ("create_row twice", "    create_row Room, title: \"a\"\n    create_row Room, title: \"b\"\n    response from: \"a.b\"\n", Some("`create_row Room` is declared twice")),
        ("create_row with no answer row", "    create_row Note, roomId: :id\n", Some("`create_row Note` is replaced by the answer: add `upsert Note, from:`")),
        ("create_row with plus", "    parameter :price, :int\n    create_row Room, count: { plus: :price }\n    response from: \"a.b\"\n", Some("`plus:` and `minus:` belong to `change_row`")),
        ("change_row of a model that is no singleton", "    change_row Note, body: \"a\"\n", Some("`change_row Note` needs `row: :param`: Note is not a singleton")),
        ("change_row row not an id", "    parameter :wallet, :string\n    change_row Wallet, row: :wallet, balance: 1\n", Some("`row:` names `wallet`, declare it with `parameter :wallet, :id`")),
        ("change_row unknown attribute", "    change_row Wallet, ghost: 1\n", Some("`change_row` names unknown attribute `ghost` of `Wallet`")),
        ("minus on a text attribute", "    parameter :price, :int\n    change_row Wallet, label: { minus: :price }\n", Some("`plus:` and `minus:` need an :int or :num attribute, `label` is not one")),
        ("minus with a text parameter", "    parameter :price, :string\n    change_row Wallet, balance: { minus: :price }\n", Some("`price` must be a number parameter")),
        ("row source of a model that is no singleton", "    create_row Room, title: { of: Note, field: :body }\n    response from: \"a.b\"\n", Some("`{ of: Note }` needs `row: :param`: Note is not a singleton")),
        ("row source", "    create_row Room, title: { of: Wallet, field: :label }\n    response from: \"a.b\"\n", None),
        ("a parameter used by an effect only", "    parameter :price, :int, null: true\n    change_row Wallet, balance: { minus: :price }\n", None),
        ("an effect hash that is none of the forms", "    create_row Room, title: { by: :x }\n    response from: \"a.b\"\n", Some("an effect hash is")),
        ("an effect array", "    create_row Room, title: [1]\n    response from: \"a.b\"\n", Some("an effect value must be a literal, a parameter symbol or a hash")),
    ]);
}

#[test]
fn effect_lines_belong_to_named_operations() {
    for word in ["add_to :open", "remove_from :open", "create_row Room, title: \"x\"", "change_row Wallet, balance: 1"] {
        let errors = check_source(&source(&format!("  operation :create do\n    mutation \"M\"\n    {word}\n  end\n"))).unwrap_err();
        let name = word.split_whitespace().next().unwrap();
        assert!(errors.iter().any(|error| error.message.contains(&format!("`{name}` does not apply to operation `create`"))), "{name}: {errors:?}");
    }
}

#[test]
fn a_local_attribute_reads_the_singleton_row_and_the_generated_create_makes_it_optional() {
    let create = |local: &str| source(&format!("  operation :create do\n    mutation \"M\"\n    variables title: :title\n    {local}\n  end\n"));
    let models = check_source(&create("local :count, from: { of: Wallet, field: :balance }")).unwrap();
    let room = models.into_iter().find(|model| model.key.as_str() == "Room").unwrap();
    let declared = &room.actions[0];
    assert_eq!(declared.local, ["count"]);
    assert_eq!(serde_json::to_value(&declared.local_from).unwrap(), json!({"count": {"row": {"model": "Wallet", "field": "balance"}}}));
    for (local, message) in [
        ("local :count, from: { of: Note, field: :body }", "`{ of: Note }` needs `row: :param`: Note is not a singleton"),
        ("local :count, from: { of: Wallet, field: :ghost }", "`{ of: Wallet }` names unknown attribute `ghost`"),
        ("local :count, from: :title", "`local` takes `from: { of: Model, field: :name }`"),
        ("local :count, from: { owner: :title }", "`local` takes `from: { of: Model, field: :name }`"),
    ] {
        let errors = check_source(&create(local)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(message)), "{local}: {errors:?}");
    }
}
