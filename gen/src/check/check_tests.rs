use crate::parse_models;

fn failure(source: &str) -> (usize, String) {
    let errors = parse_models(source).unwrap_err();
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
}

#[test]
fn file_field_cannot_be_indexed() {
    assert_eq!(field_failure("file :avatar, index: true"), (3, "unknown key `index` for `file`".to_string()));
}

#[test]
fn missing_required_key() {
    assert_eq!(failure_in("  belongs_to :o, key: :ownerId"), (7, "`belongs_to` needs `model:`".to_string()));
    assert_eq!(failure_in("  has_many :o, model: Sample"), (7, "`has_many` needs `key:`".to_string()));
    assert_eq!(field_failure("enum :kind"), (3, "`enum` needs `values:`".to_string()));
    assert_eq!(field_failure("list :tags"), (3, "`list` needs `of:`".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    list\n  end"), (7, "`remote` needs a `query` line".to_string()));
}

#[test]
fn wrong_value_types() {
    assert_eq!(field_failure("string :a, null: 1"), (3, "`null` must be true or false".to_string()));
    assert_eq!(field_failure("list :a, of: :blob"), (3, "`of` must be one of string, num, int, bool, date, id".to_string()));
    assert_eq!(
        failure_in("  has_many :o, model: Sample, key: :ownerId, dependent: :nullify"),
        (7, "`dependent` must be `:destroy`".to_string())
    );
    assert_eq!(failure_in("  scope :s, order: { name: :up }"), (7, "order direction must be `:asc` or `:desc`".to_string()));
    assert_eq!(
        failure_in("  scope :s, params: [:a], where: { name: :b }"),
        (7, "where value `:b` is not listed in `params`".to_string())
    );
    assert_eq!(failure_in("  scope :s, where: [1]"), (7, "`where` must be a hash like `{ field: :param }`".to_string()));
    assert_eq!(field_failure("enum :k, values: :a"), (3, "`values` must be a list of symbols".to_string()));
}

#[test]
fn wrong_positional_arguments() {
    assert_eq!(field_failure("string"), (3, "`string` needs a name like `:name`".to_string()));
    assert_eq!(field_failure("string :a, :b"), (3, "`string` needs a name like `:name`".to_string()));
    assert_eq!(failure("model Sample do\n  schema do\n    index :a\n  end\nend"), (3, "`index` needs 2 or more field names like `:a, :b`".to_string()));
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
        failure("model Sample do\n  schema do\n    string :a\n    id :ownerId\n  end\n  belongs_to :o, model: Sample, key: :ownerId\nend"),
        (1, "model Sample: foreign key of association o is not indexed".to_string())
    );
    assert_eq!(
        failure("model Sample do\n  schema do\n    string :a\n  end\n  scope :s, order: { zzz: :asc }\nend"),
        (1, "model Sample: relation s sorts by unknown column zzz".to_string())
    );
    assert_eq!(
        failure("model Sample do\n  schema do\n    string :a, index: true\n    index :a, :b\n  end\nend"),
        (1, "model Sample: composite index field b is not an indexed field".to_string())
    );
}

#[test]
fn errors_are_collected_across_models() {
    let errors = parse_models("model A do\n  frob :x\nend\nmodel B do\n  glob :y\nend").unwrap_err();
    assert_eq!(errors.iter().map(|error| error.line).collect::<Vec<_>>(), vec![2, 5]);
}

#[test]
fn syntax_errors_stop_before_checks() {
    let errors = parse_models("model A do\n  string :x\n").unwrap_err();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].message, "`model` block is not closed with `end`");
}

#[test]
fn standard_action_rules() {
    assert_eq!(failure_in("  create input: { name: :name }"), (7, "`create` needs `with:`".to_string()));
    assert_eq!(failure_in("  create with: A\n  create with: B"), (8, "`create` is declared twice".to_string()));
    assert_eq!(failure_in("  update with: :a"), (7, "`with` must be an operation name like `CreateMoment`".to_string()));
    assert_eq!(failure_in("  destroy :x, with: A"), (7, "`destroy` takes no arguments".to_string()));
    assert_eq!(failure_in("  create with: A, input: [1]"), (7, "`input` must be a hash like `{ field: :param }`".to_string()));
    assert_eq!(failure_in("  create with: A, input: { name: 1 }"), (7, "input value must be a field symbol".to_string()));
    assert_eq!(failure_in("  create with: A, input: { name: :ghost }"), (7, "input names unknown field `ghost`".to_string()));
}

#[test]
fn action_block_rules() {
    assert_eq!(failure_in("  action :x do\n    input a: :id\n  end"), (7, "`action` needs a `mutation` line".to_string()));
    assert_eq!(failure_in("  action :x\n"), (7, "`action` needs a `do ... end` block".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    mutation N\n  end"), (9, "`mutation` is declared twice".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    bogus 1\n  end"), (9, "unknown word `bogus`".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    optimistic ghost: true\n  end"), (9, "`optimistic` names unknown field `ghost`".to_string()));
    assert_eq!(failure_in("  action :x do\n    mutation M\n    refetch :ghost\n  end"), (9, "`refetch` names `ghost`, which is not a `remote`".to_string()));
    assert_eq!(failure_in("  scope :x\n  action :x do\n    mutation M\n  end"), (8, "name `x` is declared twice".to_string()));
}

#[test]
fn updates_line_rules() {
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    updates :x\n  end"), (9, "`updates` needs an operation name like `MomentsFeed`".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    updates E, deleted_by: 1\n  end"), (9, "`deleted_by` must be a symbol like `:name`".to_string()));
    assert_eq!(failure_in("  remote :feed do\n    query Q\n    updates E\n    updates F\n  end"), (10, "`updates` is declared twice".to_string()));
    assert_eq!(failure_in("  live :events do\n    subscription E\n  end"), (7, "unknown word `live`".to_string()));
}
