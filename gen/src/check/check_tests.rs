use crate::check::check_source;

fn failure(source: &str) -> (usize, String) {
    let errors = check_source(source).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    (errors[0].line, errors[0].message.clone())
}

fn failure_in(body: &str) -> (usize, String) {
    failure(&format!("model Sample do\n  schema do\n    string :name, index: true\n    id :ownerId, index: true\n  end\n\n{body}\nend\n"))
}

fn field_failure(line: &str) -> (usize, String) {
    failure(&format!("model Sample do\n  schema do\n    {line}\n  end\nend\n"))
}

#[test]
fn unknown_word_in_model_and_schema_and_remote() {
    assert_eq!(failure_in("  frobnicate :x"), (7, "unknown word `frobnicate`".to_string()));
    assert_eq!(field_failure("text :x"), (3, "unknown word `text`".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    bogus 1\n  end"), (9, "unknown word `bogus`".to_string()));
}

#[test]
fn field_outside_schema_block() {
    assert_eq!(failure_in("  string :extra"), (7, "field `string` must be inside a `schema` block".to_string()));
}

#[test]
fn unknown_key() {
    assert_eq!(field_failure("string :a, nullable: true"), (3, "unknown key `nullable` for `string`".to_string()));
    assert_eq!(
        failure_in("  belongs_to :o, model: Sample, key: :ownerId, bogus: 1"),
        (7, "unknown key `bogus` for `belongs_to`".to_string())
    );
    assert_eq!(field_failure("file :avatar, index: true"), (3, "unknown key `index` for `file`".to_string()), "file field cannot be indexed");
    for on_failure in [":keep", ":rollback"] {
        assert_eq!(
            failure_in(&format!("  destroy with: A, on_failure: {on_failure}")),
            (7, "unknown key `on_failure` for `destroy`".to_string()),
            "destroy takes no on_failure {on_failure}"
        );
    }
}

#[test]
fn missing_required_key() {
    assert_eq!(failure_in("  belongs_to :o, key: :ownerId"), (7, "association `o` targets unknown model `O`".to_string()));
    assert_eq!(failure_in("  has_many :o, model: Sample"), (1, "model Sample: foreign key of association o is not indexed".to_string()));
    assert_eq!(field_failure("enum :kind"), (3, "`enum` needs `values:`".to_string()));
    assert_eq!(field_failure("list :tags"), (3, "`list` needs `of:`".to_string()));
}

#[test]
fn wrong_value_types() {
    assert_eq!(field_failure("string :a, null: 1"), (3, "`null` must be true or false".to_string()));
    assert_eq!(field_failure("list :a, of: :blob"), (3, "`of` must be one of string, num, int, bool, date, id".to_string()));
    assert_eq!(
        failure_in("  has_many :samples, key: :ownerId, dependent: :nullify"),
        (7, "`dependent` must be `:destroy`".to_string())
    );
    assert_eq!(failure_in("  scope :s, order: { name: :up }"), (7, "order direction must be `:asc`, `:desc`, `:asc_nulls_last` or `:desc_nulls_first`".to_string()));
    assert_eq!(
        failure_in("  scope :s, params: [:a], where: { name: :b, other: :a }"),
        (7, "where value `:b` is not listed in `params`".to_string())
    );
    assert_eq!(failure_in("  scope :s, where: [1]"), (7, "`where` must be a hash like `{ field: :param }`".to_string()));
    assert_eq!(field_failure("enum :k, values: :a"), (3, "`values` must be a list of symbols".to_string()));
}

#[test]
fn wrong_positional_arguments() {
    assert_eq!(field_failure("string"), (3, "`string` needs a name like `:name`".to_string()));
    assert_eq!(field_failure("string :a, :b"), (3, "`string` needs a name like `:name`".to_string()));
    assert_eq!(failure("model Sample do\n  schema do\n    index\n  end\nend"), (3, "`index` needs field names like `:a` or `:a, :b`".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    paginate :x\n  end"), (9, "`paginate` needs a number".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    fresh_for 5\n  end"), (9, "`fresh_for` needs a duration like `5.minutes`".to_string()));
}

#[test]
fn block_rules() {
    assert_eq!(failure("model Sample do\n  schema\nend"), (2, "`schema` needs a `do ... end` block".to_string()));
    assert_eq!(failure_in("  scope :s do\n  end"), (7, "`scope` takes no block".to_string()));
    assert_eq!(failure("model Sample do\n  schema do\n    string :a do\n    end\n  end\nend"), (3, "`string` takes no block".to_string()));
}

#[test]
fn remote_rules() {
    assert_eq!(
        failure_in("  remote :feed do\n    query Q\n    paginate 5\n    list\n  end"),
        (7, "`paginate` and `list` cannot be used together".to_string())
    );
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    max_pages 3\n  end"), (9, "`max_pages` needs `paginate`".to_string()));
}

#[test]
fn duplicate_names() {
    assert_eq!(field_failure("string :a\n    int :a"), (4, "field `a` is declared twice".to_string()));
    assert_eq!(
        failure_in("  scope :s\n  scope :s"),
        (8, "name `s` is declared twice".to_string())
    );
    assert_eq!(
        failure_in("  belongs_to :o, model: Sample, key: :ownerId\n  scope :o"),
        (8, "name `o` is declared twice".to_string())
    );
    assert_eq!(failure("model A do\n  schema do\n    string :x\n  end\nend\nmodel A do\n  schema do\n    string :x\n  end\nend"), (6, "model `A` is declared twice".to_string()));
    assert_eq!(failure("model A do\n  schema do\n    string :x\n  end\n  schema do\n    string :y\n  end\nend"), (5, "`schema` is declared twice".to_string()));
}

#[test]
fn association_to_unknown_model() {
    assert_eq!(
        failure_in("  has_many :things, model: Ghost, key: :ownerId"),
        (7, "association `things` targets unknown model `Ghost`".to_string())
    );
}

#[test]
fn core_declaration_errors_point_at_the_model() {
    assert_eq!(
        failure("model Sample do\n  schema do\n    string :a\n  end\n  scope :s, order: { zzz: :asc }\nend"),
        (1, "model Sample: relation s sorts by unknown column zzz".to_string())
    );
    assert_eq!(
        failure("model Sample do\n  schema do\n    string :a, index: true\n    index :a, :b\n  end\nend"),
        (1, "model Sample: index column b must name a scalar or enum field".to_string())
    );
}

#[test]
fn errors_are_collected_across_models() {
    let errors = check_source("model A do\n  frob :x\nend\nmodel B do\n  glob :y\nend").unwrap_err();
    assert_eq!(errors.iter().map(|error| error.line).collect::<Vec<_>>(), vec![2, 5]);
}

#[test]
fn syntax_errors_stop_before_checks() {
    let errors = check_source("model A do\n  string :x\n").unwrap_err();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].message, "`model` block is not closed with `end`");
}

#[test]
fn standard_action_rules() {
    assert_eq!(failure_in("  create input: { name: :name }"), (7, "`create` needs `with:`".to_string()));
    assert_eq!(failure_in("  create with: A\n  create with: B"), (8, "`create` is declared twice".to_string()));
    assert_eq!(failure_in("  update with: :a"), (7, "`with` must be an operation name like `CreateMoment` or \"likeMoment\"".to_string()));
    assert_eq!(failure_in("  destroy :x, with: A"), (7, "`destroy` takes no arguments".to_string()));
    assert_eq!(failure_in("  create with: A, input: [1]"), (7, "`input` must be a hash like `{ field: :param }`".to_string()));
    assert_eq!(failure_in("  create with: A, input: { name: 1 }"), (7, "input value must be a field symbol".to_string()));
    assert_eq!(failure_in("  create with: A, input: { name: :ghost }"), (7, "input names unknown field `ghost`".to_string()));
}

#[test]
fn action_correlate_takes_fields_and_dotted_paths() {
    let source = |line: &str| format!("model Sample do\n  schema do\n    string :name, index: true\n    object :media, null: true\n  end\n\n  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    {line}\n  end\nend\n");
    assert!(check_source(&source("correlate :name, \"media.blobId\"")).is_ok());
    assert_eq!(failure(&source("correlate 1")), (10, "`correlate` needs 1 or more names like `:a, \"media.blobId\"`".to_string()));
}

#[test]
fn action_block_rules() {
    assert_eq!(failure_in("  action :x do\n    input a: :id\n    local_effect :none, reason: \"test\"\n  end"), (7, "`action` needs a `mutation` line".to_string()));
    assert_eq!(failure_in("  action :x\n"), (7, "`action` needs a `do ... end` block".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    mutation N\n  end"), (10, "`mutation` is declared twice".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    bogus 1\n  end"), (10, "unknown word `bogus`".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    optimistic ghost: true\n  end"), (9, "`optimistic` names unknown field `ghost`".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    optimistic name: { by: :ghost, element: { name: :a } }\n  end").1, "optimistic hash must be `{ by: :key, element: { key: :arg, ... } }` with the key among the element fields");
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    refetch :ghost\n  end"), (10, "`refetch` names `ghost`, which is not a `remote`".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    result note: :ids\n  end"), (10, "result type must be :bool, :int, :num, :string or :id".to_string()));
    assert_eq!(failure_in("  scope :x\n  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n  end"), (8, "name `x` is declared twice".to_string()));
}

#[test]
fn updates_line_rules() {
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    updates :x\n  end"), (9, "`updates` needs an operation name like `MomentsFeed` or \"feed\"".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    updates E, deleted_by: 1\n  end"), (9, "`deleted_by` must be a symbol like `:name`".to_string()));
    assert_eq!(failure_in("  live :events do\n    subscription E\n  end"), (7, "unknown word `live`".to_string()));
}

#[test]
fn on_failure_accepts_keep_and_rollback_only() {
    let message = "`on_failure` must be `:keep` or `:rollback`".to_string();
    assert_eq!(failure_in("  create with: A, on_failure: :ghost"), (7, message.clone()));
    assert_eq!(failure_in("  update with: A, on_failure: 1"), (7, message.clone()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    on_failure :ghost\n  end"), (10, message.clone()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    on_failure :keep\n    on_failure :keep\n  end"), (11, "`on_failure` is declared twice".to_string()));
    let valid = "model Sample do\n  schema do\n    string :name\n  end\n  create with: A, on_failure: :keep\n  action :x do\n    mutation M\n    local_effect :none, reason: \"test\"\n    on_failure :rollback\n  end\nend\n";
    assert!(check_source(valid).is_ok());
}

fn ephemeral_failure(lines: &str) -> (usize, String) {
    failure(&format!("model Sample do\n  schema do\n    string :name\n    ephemeral do\n{lines}\n    end\n  end\nend\n"))
}

#[test]
fn ephemeral_fields_check_their_keys_and_values() {
    assert!(check_source("model Sample do\n  schema do\n    string :name\n    ephemeral do\n      bool :typing, default: false, reset_after: 5.seconds\n      string :mood, null: true\n    end\n  end\nend\n").is_ok());
    assert_eq!(ephemeral_failure("      bool :typing, reset_after: 5.seconds"), (5, "`reset_after` needs `default:`".to_string()));
    assert_eq!(ephemeral_failure("      bool :typing"), (5, "ephemeral field `typing` needs `default:` or `null: true`".to_string()));
    assert_eq!(ephemeral_failure("      bool :typing, default: false, index: true"), (5, "ephemeral field `typing` cannot be indexed".to_string()));
    assert_eq!(ephemeral_failure("      file :photo, null: true"), (5, "ephemeral field `photo` cannot be a file".to_string()));
    assert_eq!(ephemeral_failure("      bool :typing, default: false, reset_after: 5"), (5, "`reset_after` must be a duration like `5.minutes`".to_string()));
    assert_eq!(ephemeral_failure("      bool :typing, default: false, optional: true"), (5, "unknown key `optional` for `bool`".to_string()));
}

#[test]
fn ephemeral_names_are_unique_across_stored_and_memory_fields() {
    assert_eq!(ephemeral_failure("      bool :name, default: false"), (5, "field `name` is declared twice".to_string()));
    assert_eq!(ephemeral_failure("      bool :typing, default: false\n      bool :typing, default: true"), (6, "field `typing` is declared twice".to_string()));
    assert_eq!(failure("model Sample do\n  schema do\n    string :name\n    ephemeral do\n      bool :name, default: false\n    end\n  end\nend\n"), (5, "field `name` is declared twice".to_string()));
    assert_eq!(
        failure("model Sample do\n  schema do\n    string :name\n    ephemeral do\n      ephemeral do\n      end\n    end\n  end\nend\n"),
        (5, "unknown word `ephemeral`".to_string())
    );
}

#[test]
fn ephemeral_fields_are_not_allowed_in_indexes_scopes_or_inputs() {
    let indexed = "model Sample do\n  schema do\n    string :name, index: true\n    ephemeral do\n      bool :typing, default: false, reset_after: 5.seconds\n    end\n    index :name, :typing\n  end\nend\n";
    assert_eq!(failure(indexed), (1, "model Sample: ephemeral field typing cannot be indexed".to_string()));
    let scoped = "model Sample do\n  schema do\n    string :name, index: true\n    ephemeral do\n      bool :typing, default: false\n    end\n  end\n  scope :typists, params: [:typing], where: { typing: :typing }\nend\n";
    assert_eq!(failure(scoped), (1, "model Sample: relation typists uses ephemeral field typing".to_string()));
    let ordered = "model Sample do\n  schema do\n    string :name, index: true\n    ephemeral do\n      bool :typing, default: false\n    end\n  end\n  scope :by_typing, order: { typing: :desc }\nend\n";
    assert_eq!(failure(ordered), (1, "model Sample: relation by_typing uses ephemeral field typing".to_string()));
}

#[test]
fn memory_model_line_checks_storage_and_expire_after() {
    assert!(check_source("model Presence, storage: :memory, expire_after: 30.seconds do\n  schema do\n    string :room\n  end\nend\n").is_ok());
    assert_eq!(failure("model P, storage: :cloud do\n  schema do\n    string :a\n  end\nend\n"), (1, "`storage` must be `:memory`, `:disk` or `:local`".to_string()));
    assert_eq!(failure("model P, expire_after: 30 do\n  schema do\n    string :a\n  end\nend\n"), (1, "`expire_after` must be a duration like `5.minutes`".to_string()));
    assert_eq!(failure("model P, bogus: 1 do\n  schema do\n    string :a\n  end\nend\n"), (1, "unknown key `bogus` for `model`".to_string()));
    assert_eq!(
        failure("model P, expire_after: 30.seconds do\n  schema do\n    string :a\n  end\nend\n"),
        (1, "model P: expire_after needs storage: :memory".to_string())
    );
    assert_eq!(
        failure("model P, storage: :memory do\n  schema do\n    string :a\n    file :photo, null: true\n  end\nend\n"),
        (1, "model P: memory model has file field photo".to_string())
    );
    assert_eq!(
        failure("model P, storage: :memory do\n  schema do\n    string :a\n  end\n  create with: CreateP\nend\n"),
        (1, "model P: memory model declares action create".to_string())
    );
}

#[test]
fn updates_set_is_a_hash_of_ephemeral_values() {
    let model = |set: &str| {
        format!("model Sample do\n  schema do\n    string :name\n    ephemeral do\n      bool :typing, default: false, reset_after: 5.seconds\n    end\n  end\n  remote :one do\n    query One\n    updates Typing, set: {set}\n  end\nend\n")
    };
    assert!(check_source(&model("{ typing: true }")).is_ok());
    assert_eq!(failure(&model(":typing")), (10, "`set` must be a hash like `{ field: :param }`".to_string()));
    let unbounded = model("{ typing: true }").replace(", reset_after: 5.seconds", "");
    assert_eq!(failure(&unbounded), (1, "model Sample: relation one sets typing, which has no reset_after".to_string()));
    assert_eq!(failure(&model("{ name: true }")), (1, "model Sample: relation one sets name, which is not an ephemeral field".to_string()));
}

#[test]
fn a_merge_rule_must_be_a_known_word_a_list_of_tiers_or_a_hash_of_those() {
    let message = "`merge` must be `:present`, `:non_empty`, `:positive`, `:monotonic`, a list of tiers like `[[:a, :b], [:c]]` or a hash of those per key (a key may also be `:local`)";
    assert_eq!(field_failure("object :a, merge: :newest"), (3, message.to_string()));
    assert_eq!(field_failure("object :a, merge: { width: { deep: :positive } }"), (3, message.to_string()));
    assert_eq!(field_failure("enum :a, values: [:x], merge: [:x]"), (3, message.to_string()));
}

#[test]
fn a_local_field_is_optional_and_carries_no_other_policy() {
    assert_eq!(field_failure("string :a, local: true"), (3, "local field `a` needs `optional: true` or `null: true`".to_string()));
    let message = "local field `a` cannot set `merge`, `index`, `default`, `from` or `id_from`";
    assert_eq!(field_failure("string :a, optional: true, local: true, index: true"), (3, message.to_string()));
    assert_eq!(field_failure("string :a, null: true, local: true, merge: :non_empty"), (3, message.to_string()));
    assert_eq!(field_failure("string :a, optional: true, merge: :local").0, 3);
    assert!(check_source("model Sample do\n  schema do\n    file :a, optional: true, local: true\n  end\nend\n").unwrap_err().iter().any(|error| error.message == "unknown key `local` for `file`"));
}

#[test]
fn an_action_declares_its_local_effect() {
    let action = |lines: &str| format!("model Sample do\n  schema do\n    string :name\n  end\n\n  action :x do\n    mutation M\n{lines}  end\nend\n");
    assert!(check_source(&action("    optimistic name: :a\n")).is_ok());
    assert!(check_source(&action("    local_effect :none, reason: \"the server owns the result\"\n")).is_ok());
    assert_eq!(failure(&action("")), (6, "`action :x` must declare `optimistic` or `local_effect :none, reason: \"...\"`".to_string()));
    assert_eq!(failure(&action("    local_effect :none\n")).1, "`local_effect :none` needs a non-empty `reason:` string");
    assert_eq!(failure(&action("    local_effect :none, reason: \"\"\n")).1, "`local_effect :none` needs a non-empty `reason:` string");
    assert_eq!(failure(&action("    local_effect :some, reason: \"r\"\n")).1, "`local_effect` must be `:none`");
}
