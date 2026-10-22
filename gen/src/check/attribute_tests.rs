use super::check_source;

fn run(rows: &[(&str, &str, Option<&str>)]) {
    for (name, body, expected) in rows {
        let source = format!("model Sample do\n  attribute :name, :string\n  attribute :ownerId, :id\n{body}\nend\n");
        match (check_source(&source), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(message)) => panic!("{name}: accepted, expected `{message}`"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|error| error.message.contains(message)), "{name}: {errors:?}"),
        }
    }
}

#[test]
fn every_type_word_is_accepted_and_every_old_or_unknown_word_is_refused() {
    run(&[
        ("string", "  attribute :a, :string", None),
        ("num", "  attribute :a, :num", None),
        ("int", "  attribute :a, :int", None),
        ("bool", "  attribute :a, :bool", None),
        ("date", "  attribute :a, :date", None),
        ("id", "  attribute :a, :id", None),
        ("json", "  attribute :a, :json", None),
        ("file", "  attribute :a, :file, null: true", None),
        ("enum", "  attribute :a, :enum, values: [\"X\", \"Y\"]", None),
        ("scalar list", "  attribute :a, [:string], default: []", None),
        ("enum list", "  attribute :a, [:enum], values: [\"X\"]", None),
        ("named object", "  attribute :a, Inner do\n    attribute :x, :int\n  end", None),
        ("named list", "  attribute :a, [Cell] do\n    attribute :x, :int\n  end", None),
        ("old field word", "  field :a, type: :string", Some("unknown model declaration `field`")),
        ("old raw type", "  attribute :a, :raw", Some("an attribute type is")),
        ("untyped object", "  attribute :a, :object", Some("an attribute type is")),
        ("untyped array", "  attribute :a, :array", Some("an attribute type is")),
        ("old list type", "  attribute :a, :list", Some("an attribute type is")),
        ("list of unknown scalar", "  attribute :a, [:blob]", Some("an attribute type is")),
        ("no type and no schema", "  attribute :a", Some("needs a type")),
        ("type given twice", "  attribute :a, :string, :int", Some("takes a name and a type")),
        ("no name", "  attribute", Some("attribute needs a name")),
        ("string name", "  attribute \"a\", :string", Some("attribute name must be a symbol")),
    ]);
}

#[test]
fn attribute_options_are_closed_and_each_applies_to_its_own_types() {
    run(&[
        ("null", "  attribute :a, :string, null: true", None),
        ("null wrong value", "  attribute :a, :string, null: 1", Some("`null` must be true or false")),
        ("optional wrong value", "  attribute :a, :string, optional: \"yes\"", Some("`optional` must be true or false")),
        ("unknown key", "  attribute :a, :string, nullable: true", Some("unknown key `nullable` for `attribute`")),
        ("index option", "  attribute :a, :string, index: true", Some("unknown key `index`")),
        ("searchable option", "  attribute :a, :string, searchable: true", Some("unknown key `searchable`")),
        ("key given twice", "  attribute :a, :string, null: true, null: false", Some("`null` is given twice")),
        ("from path", "  attribute :a, :string, from: \"x.y\"", None),
        ("from symbol", "  attribute :a, :string, from: :x", Some("`from` must be a string")),
        ("from empty segment", "  attribute :a, :string, from: \"x..y\"", Some("has an empty segment")),
        ("from on a file", "  attribute :a, :file, null: true, from: \"x\"", Some("`from` does not apply")),
        ("id_from", "  attribute :a, :id, id_from: :owner", None),
        ("id_from string", "  attribute :a, :id, id_from: \"owner\"", Some("`id_from` must be a symbol")),
        ("merge present", "  attribute :a, :string, merge: :present", None),
        ("merge unknown rule", "  attribute :a, :string, merge: :newest", Some("`merge` must be")),
        ("merge on a file", "  attribute :a, :file, null: true, merge: :present", Some("`merge` applies only")),
        ("values on a string", "  attribute :a, :string, values: [\"X\"]", Some("`values:` belongs to `:enum`")),
        ("values symbols", "  attribute :a, :enum, values: [:x]", Some("`values` must be a list of strings")),
        ("enum without values", "  attribute :a, :enum", Some("needs `values:`")),
        ("as on an enum", "  attribute :a, :enum, values: [\"X\"], as: Kind", None),
        ("as on a string", "  attribute :a, :string, as: Kind", Some("`as:` names the generated enum")),
        ("as is a symbol", "  attribute :a, :enum, values: [\"X\"], as: :kind", Some("`as` must be a type name")),
        ("parse json", "  attribute :a, :json, parse: :json, null: true, default: nil", None),
        ("parse on a string", "  attribute :a, :string, parse: :json", Some("`parse:` accepts only `:json` on a `:json` attribute")),
        ("parse other kind", "  attribute :a, :json, parse: :xml", Some("`parse:` accepts only")),
        ("reset_after outside ephemeral", "  attribute :a, :bool, default: false, reset_after: 5.seconds", Some("`reset_after` applies only inside `ephemeral`")),
        ("default of any value", "  attribute :a, :int, default: 3", None),
        ("block on a scalar", "  attribute :a, :string do\n    attribute :x, :int\n  end", Some("takes no block")),
    ]);
}

#[test]
fn local_attributes_follow_their_own_rules() {
    run(&[
        ("local optional", "  attribute :a, :string, optional: true, local: true", None),
        ("local null", "  attribute :a, :string, null: true, local: true", None),
        ("local needs optional or null", "  attribute :a, :string, local: true", Some("needs `optional: true` or `null: true`")),
        ("local with default", "  attribute :a, :string, optional: true, local: true, default: \"x\"", Some("cannot set `merge`, `default`, `from` or `id_from`")),
        ("local with from", "  attribute :a, :string, optional: true, local: true, from: \"x\"", Some("cannot set")),
    ]);
}

#[test]
fn names_are_unique_and_blocks_are_not_empty() {
    run(&[
        ("duplicate attribute", "  attribute :a, :string\n  attribute :a, :int", Some("attribute `a` is declared twice")),
        ("duplicate nested", "  attribute :a, Inner do\n    attribute :x, :int\n    attribute :x, :int\n  end", Some("attribute `x` is declared twice")),
        ("empty object block", "  attribute :a, Inner do\n  end", Some("block needs at least 1 attribute")),
        ("type not declared", "  attribute :a, Missing", Some("is not declared by any `do ... end` block")),
        ("type declared twice", "  attribute :a, Inner do\n    attribute :x, :int\n  end\n  attribute :b, Inner do\n    attribute :x, :int\n  end", Some("type `Inner` is declared twice")),
        ("type reused", "  attribute :a, Inner do\n    attribute :x, :int\n  end\n  attribute :b, Inner, null: true", None),
        ("type has a model name", "  attribute :a, Sample do\n    attribute :x, :int\n  end", Some("has the name of a model")),
        ("recursive object", "  attribute :a, Loop, null: true do\n    attribute :next, Loop, null: true\n  end", Some("recursive objects need an array")),
        ("nested only attributes", "  attribute :a, Inner do\n    index :x\n  end", Some("a block holds only `attribute` declarations")),
        ("nested merge", "  attribute :a, Inner do\n    attribute :x, :int, merge: :positive\n  end", Some("`merge` applies only to a top-level")),
        ("nested id_from", "  attribute :a, Inner do\n    attribute :x, :id, id_from: :owner\n  end", Some("`id_from` applies only to a top-level")),
        ("nested local", "  attribute :a, Inner do\n    attribute :x, :int, optional: true, local: true\n  end", Some("`local` applies only to a top-level")),
        ("nested default fits", "  attribute :a, Inner do\n    attribute :x, :int, default: 1\n  end", None),
        ("nested default does not fit", "  attribute :a, Inner do\n    attribute :x, :int, default: \"s\"\n  end", Some("default must match its type")),
    ]);
}

#[test]
fn unions_declare_variants_with_one_shared_block() {
    run(&[
        ("union", "  attribute :u, Pick, null: true do\n    on Left do\n      attribute :id, :id\n    end\n    on Right do\n      attribute :id, :id\n    end\n  end", None),
        ("shared fields", "  attribute :u, Pick, null: true do\n    shared do\n      attribute :id, :id\n    end\n    on Left do\n    end\n    on Right do\n    end\n  end", None),
        ("case override", "  attribute :u, Pick, null: true do\n    on LeftSide, case: :left do\n      attribute :id, :id\n    end\n    on RightSide, case: :right do\n      attribute :id, :id\n    end\n  end", None),
        ("on needs a type name", "  attribute :u, Pick, null: true do\n    on :left do\n      attribute :id, :id\n    end\n  end", Some("`on` needs a type name")),
        ("variant twice", "  attribute :u, Pick, null: true do\n    on Left do\n      attribute :id, :id\n    end\n    on Left do\n      attribute :id, :id\n    end\n  end", Some("variant `Left` is declared twice")),
        ("shared twice", "  attribute :u, Pick, null: true do\n    shared do\n      attribute :id, :id\n    end\n    shared do\n      attribute :id, :id\n    end\n    on Left do\n    end\n  end", Some("`shared` is declared twice")),
        ("kind is reserved", "  attribute :u, Pick, null: true do\n    shared do\n      attribute :kind, :string\n    end\n    on Left do\n    end\n    on Right do\n    end\n  end", Some("`kind` is reserved")),
        ("field repeats shared", "  attribute :u, Pick, null: true do\n    shared do\n      attribute :id, :id\n    end\n    on Left do\n      attribute :id, :id\n    end\n    on Right do\n    end\n  end", Some("is also a `shared` attribute")),
        ("other word in a union", "  attribute :u, Pick, null: true do\n    attribute :id, :id\n    on Left do\n      attribute :id, :id\n    end\n  end", Some("holds only `on` variants and one `shared` block")),
        ("case is a symbol", "  attribute :u, Pick, null: true do\n    on Left, case: \"left\" do\n      attribute :id, :id\n    end\n  end", Some("`case` must be a symbol")),
        ("union only on a model attribute", "  ephemeral do\n    attribute :u, Pick, null: true do\n      on Left do\n        attribute :id, :id\n      end\n    end\n  end", Some("a union is declared only on a model attribute")),
    ]);
}

#[test]
fn ephemeral_attributes_reset_and_need_a_default_or_null() {
    run(&[
        ("default and reset", "  ephemeral do\n    attribute :typing, :bool, default: false, reset_after: 5.seconds\n  end", None),
        ("null", "  ephemeral do\n    attribute :mood, :string, null: true\n  end", None),
        ("reset without default", "  ephemeral do\n    attribute :typing, :bool, null: true, reset_after: 5.seconds\n  end", Some("`reset_after` needs `default:`")),
        ("no default and not null", "  ephemeral do\n    attribute :typing, :bool\n  end", Some("needs `default:` or `null: true`")),
        ("reset is a duration", "  ephemeral do\n    attribute :typing, :bool, default: false, reset_after: 5\n  end", Some("`reset_after` must be a duration")),
        ("file", "  ephemeral do\n    attribute :f, :file, null: true\n  end", Some("cannot be a file")),
        ("merge", "  ephemeral do\n    attribute :typing, :bool, default: false, merge: :present\n  end", Some("`merge` applies only")),
        ("name used by a stored attribute", "  ephemeral do\n    attribute :name, :string, null: true\n  end", Some("attribute `name` is declared twice")),
        ("only attributes", "  ephemeral do\n    index :x\n  end", Some("a block holds only `attribute` declarations")),
    ]);
}

#[test]
fn index_and_search_name_declared_attributes() {
    run(&[
        ("index", "  index :name, :ownerId", None),
        ("index without names", "  index", Some("`index` needs 1 or more attribute names")),
        ("index name is a string", "  index \"name\"", Some("index column must be a symbol")),
        ("index twice", "  index :name\n  index :name", Some("index on name is declared twice")),
        ("index option", "  index :name, unique: true", Some("unknown key `unique` for `index`")),
        ("search", "  search :name", None),
        ("search without names", "  search", Some("`search` needs 1 or more attribute names")),
        ("search an unknown attribute", "  search :ghost", Some("search needs a declared attribute")),
        ("search a number", "  attribute :n, :int\n  search :n", Some("needs a :string or list attribute")),
        ("search twice", "  search :name\n  search :name", Some("search `name` is declared twice")),
        ("search option", "  search :name, ignored: true", Some("unknown key `ignored` for `search`")),
        ("old schema block", "  schema do\n    string :x\n  end", Some("unknown model declaration `schema`")),
    ]);
}

#[test]
fn associations_have_one_form_per_kind() {
    run(&[
        ("belongs_to", "  belongs_to :owner, class_name: Sample, foreign_key: :ownerId, touch: true", None),
        ("old model key", "  belongs_to :owner, model: Sample", Some("unknown key `model` for `belongs_to`")),
        ("old key key", "  belongs_to :owner, class_name: Sample, key: :ownerId", Some("unknown key `key` for `belongs_to`")),
        ("class_name is a symbol", "  belongs_to :owner, class_name: :sample", Some("`class_name` must be a type name")),
        ("target unknown", "  belongs_to :owner, class_name: Ghost, foreign_key: :ownerId", Some("targets unknown model `Ghost`")),
        ("dependent", "  has_many :samples, foreign_key: :ownerId, dependent: :destroy", None),
        ("dependent other policy", "  has_many :samples, foreign_key: :ownerId, dependent: :nullify", Some("`dependent` must be `:destroy`")),
        ("dependent on belongs_to", "  belongs_to :owner, class_name: Sample, foreign_key: :ownerId, dependent: :destroy", Some("unknown key `dependent` for `belongs_to`")),
        ("touch on has_many", "  has_many :samples, foreign_key: :ownerId, touch: true", Some("unknown key `touch` for `has_many`")),
        ("sets on has_one", "  has_one :first, class_name: Sample, foreign_key: :ownerId, sets: { a: :b }", Some("unknown key `sets` for `has_one`")),
        ("sets values are symbols", "  belongs_to :owner, class_name: Sample, foreign_key: :ownerId, sets: { a: \"b\" }", Some("`sets` must be a symbol")),
        ("name twice", "  belongs_to :owner, class_name: Sample, foreign_key: :ownerId\n  has_many :owner, class_name: Sample, foreign_key: :ownerId", Some("name `owner` is declared twice")),
        ("block", "  belongs_to :owner, class_name: Sample do\n  end", Some("takes no block")),
    ]);
}
