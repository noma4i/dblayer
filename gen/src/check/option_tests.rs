use std::collections::BTreeSet;

use super::{word_for, Context, EPHEMERAL_WORDS, FIELD_WORDS};
use crate::parse_models;

const WORLD: &str = "model User do\n  schema do\n    string :name\n  end\nend\nmodel Vibe do\n  schema do\n    id :momentId, index: true\n  end\nend\n";

fn sample(extra: &str, body: &str) -> String {
    format!("{WORLD}model Sample{extra} do\n  schema do\n    id :ownerId, index: true\n    string :a\n  end\n{body}\nend\n")
}

fn built(source: &str) -> Result<String, String> {
    let models = parse_models(source).map_err(|errors| format!("{errors:?}"))?;
    Ok(format!("{:?}", models.last().unwrap()))
}

fn with_schema(lines: &str) -> String {
    format!("{WORLD}model Sample do\n  schema do\n    id :ownerId, index: true\n{lines}\n  end\nend\n")
}

fn remote(lines: &str) -> String {
    sample("", &format!("  remote :feed do\n    query Feed\n{lines}\n  end"))
}

fn action(lines: &str) -> String {
    sample("", &format!("  action :like do\n    mutation Like\n{lines}\n  end"))
}

fn rows() -> Vec<(&'static str, String, String)> {
    let assoc = |line: &str| sample("", line);
    vec![
        ("model.storage", sample("", ""), sample(", storage: :memory", "")),
        ("model.expire_after", sample(", storage: :memory", ""), sample(", storage: :memory, expire_after: 1.minute", "")),
        ("field.null", with_schema("    string :b"), with_schema("    string :b, null: true")),
        ("field.optional", with_schema("    string :b"), with_schema("    string :b, optional: true")),
        ("field.default", with_schema("    string :b"), with_schema("    string :b, default: \"x\"")),
        ("field.id_from", with_schema("    id :b"), with_schema("    id :b, id_from: :ownerId")),
        ("field.from", with_schema("    num :b"), with_schema("    num :b, from: \"loc.b\"")),
        ("field.index", with_schema("    string :b"), with_schema("    string :b, index: true")),
        ("field.values", with_schema("    enum :b, values: [:x]"), with_schema("    enum :b, values: [:x, :y]")),
        ("field.of", with_schema("    list :b, of: :int"), with_schema("    list :b, of: :string")),
        ("ephemeral.default", with_schema("    ephemeral do\n      bool :t, default: false\n    end"), with_schema("    ephemeral do\n      bool :t, default: true\n    end")),
        ("ephemeral.reset_after", with_schema("    ephemeral do\n      bool :t, default: false\n    end"), with_schema("    ephemeral do\n      bool :t, default: false, reset_after: 5.seconds\n    end")),
        ("ephemeral.null", with_schema("    ephemeral do\n      string :t, default: \"a\"\n    end"), with_schema("    ephemeral do\n      string :t, default: \"a\", null: true\n    end")),
        ("ephemeral.index", with_schema("    ephemeral do\n      string :t, default: \"a\"\n    end"), with_schema("    ephemeral do\n      string :t, default: \"a\", index: true\n    end")),
        ("ephemeral.of", with_schema("    ephemeral do\n      list :t, of: :int, default: []\n    end"), with_schema("    ephemeral do\n      list :t, of: :string, default: []\n    end")),
        ("ephemeral.values", with_schema("    ephemeral do\n      enum :t, values: [:x], default: \"x\"\n    end"), with_schema("    ephemeral do\n      enum :t, values: [:x, :y], default: \"x\"\n    end")),
        ("create.with", assoc("  create with: CreateA"), assoc("  create with: CreateB")),
        ("create.input", assoc("  create with: CreateA"), assoc("  create with: CreateA, input: { a: :a }")),
        ("create.on_failure", assoc("  create with: CreateA"), assoc("  create with: CreateA, on_failure: keep")),
        ("update.with", assoc("  update with: UpdateA"), assoc("  update with: UpdateB")),
        ("update.input", assoc("  update with: UpdateA"), assoc("  update with: UpdateA, input: { a: :a }")),
        ("update.on_failure", assoc("  update with: UpdateA"), assoc("  update with: UpdateA, on_failure: keep")),
        ("destroy.with", assoc("  destroy with: DeleteA"), assoc("  destroy with: DeleteB")),
        ("scope.params", assoc("  scope :by"), assoc("  scope :by, params: [:ownerId]")),
        ("scope.where", assoc("  scope :by"), assoc("  scope :by, params: [:ownerId], where: { ownerId: :ownerId }")),
        ("scope.order", assoc("  scope :by"), assoc("  scope :by, order: { ownerId: :desc }")),
        ("belongs_to.model", assoc("  belongs_to :u, model: User, key: :ownerId"), assoc("  belongs_to :u, model: Vibe, key: :ownerId")),
        ("belongs_to.key", assoc("  belongs_to :u, model: User, key: :ownerId"), assoc("  belongs_to :u, model: User, key: :a")),
        ("belongs_to.touch", assoc("  belongs_to :u, model: User, key: :ownerId"), assoc("  belongs_to :u, model: User, key: :ownerId, touch: true")),
        ("has_many.model", assoc("  has_many :v, model: Vibe, key: :momentId"), assoc("  has_many :v, model: User, key: :momentId")),
        ("has_many.key", assoc("  has_many :v, model: Vibe, key: :momentId"), assoc("  has_many :v, model: Vibe, key: :ownerId")),
        ("has_many.dependent", assoc("  has_many :v, model: Vibe, key: :momentId"), assoc("  has_many :v, model: Vibe, key: :momentId, dependent: :destroy")),
        ("references.model", assoc("  references :r, model: User"), assoc("  references :r, model: Vibe")),
        ("references.key", assoc("  references :r, model: User"), assoc("  references :r, model: User, key: :a")),
        ("query", remote(""), remote("").replace("query Feed", "query Other")),
        ("paginate", remote("    paginate 20"), remote("    paginate 30")),
        ("paginate.backward", remote("    paginate 20"), remote("    paginate 20, backward: true")),
        ("list", remote(""), remote("    list")),
        ("fresh_for", remote("    fresh_for 1.minute"), remote("    fresh_for 2.minutes")),
        ("fresh_for.empty", remote("    fresh_for 1.minute"), remote("    fresh_for 1.minute, empty: 10.seconds")),
        ("max_pages", remote("    paginate 20"), remote("    paginate 20\n    max_pages 3")),
        ("updates", remote(""), remote("    updates Changed")),
        ("updates.deleted_by", remote("    updates Changed"), remote("    updates Changed, deleted_by: :gone")),
        ("updates.set", remote("    updates Changed"), remote("    updates Changed, set: { a: \"x\" }")),
        ("action.mutation", action(""), action("").replace("mutation Like", "mutation Dislike")),
        ("action.input", action(""), action("    input momentId: :id")),
        ("action.optimistic", action(""), action("    optimistic a: \"x\"")),
        ("action.refetch", action(""), action("    refetch :feed")),
        ("action.on_failure", action(""), action("    on_failure keep")),
    ]
}

fn declared_keys() -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut collect = |group: &str, context: Context, words: &[&str]| {
        for word in words {
            let Some(entry) = word_for(context, word) else { continue };
            for key in entry.keys {
                let name = match context {
                    Context::Schema => format!("field.{}", key.name),
                    Context::Ephemeral => format!("ephemeral.{}", key.name),
                    _ => format!("{}.{}", if group.is_empty() { word } else { group }, key.name),
                };
                found.insert(name);
            }
        }
    };
    collect("", Context::Model, &["create", "update", "destroy", "scope", "belongs_to", "has_many", "has_one", "references"]);
    collect("", Context::Schema, &FIELD_WORDS);
    collect("", Context::Ephemeral, &EPHEMERAL_WORDS);
    collect("", Context::Remote, &["query", "paginate", "list", "fresh_for", "max_pages", "updates"]);
    found.extend(["model.storage".to_string(), "model.expire_after".to_string()]);
    found.extend(["action.mutation", "action.input", "action.optimistic", "action.refetch", "action.on_failure"].map(str::to_string));
    found
}

#[test]
fn inv_dsl_03_every_option_the_parser_accepts_changes_the_built_spec() {
    let table = rows();
    let invalid_base: Vec<String> = table.iter().filter_map(|(label, base, _)| built(base).err().map(|error| format!("{label}: {error}"))).collect();
    assert_eq!(invalid_base, Vec::<String>::new(), "rows whose base is not a valid model");
    let unchanged: Vec<&str> = table.iter().filter(|(_, base, variant)| built(variant).is_ok_and(|built_variant| Ok(built_variant) == built(base))).map(|(label, _, _)| *label).collect();
    assert_eq!(unchanged, Vec::<&str>::new(), "options accepted and ignored");
}

#[test]
fn inv_dsl_03_the_table_names_every_option_the_checker_declares() {
    let labelled: BTreeSet<String> = rows().into_iter().map(|(label, _, _)| label.replace("has_one", "has_many").to_string()).collect();
    let mut declared = declared_keys();
    declared.remove("has_one.model");
    declared.remove("has_one.key");
    declared.remove("has_one.dependent");
    let missing: Vec<&String> = declared.iter().filter(|label| !labelled.contains(*label)).collect();
    assert_eq!(missing, Vec::<&String>::new(), "options without a row in the table");
}
