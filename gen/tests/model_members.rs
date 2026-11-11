use dblayer::schema::{generated_member_names, reserved_member_names};
use dblayer_gen::parse_files;
use std::path::PathBuf;

fn fixtures() -> Vec<PathBuf> {
    let directory = PathBuf::from(".").join("../fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    paths.sort();
    paths
}

fn refusal(source: &str) -> String {
    dblayer_gen::parse_models(source).unwrap_err().iter().map(|error| error.message.clone()).collect::<Vec<_>>().join("\n")
}

#[test]
fn a_field_named_like_a_generated_member_is_refused() {
    for name in generated_member_names() {
        let message = refusal(&format!("model Page do\n  attribute :{name}, :string\nend\n"));
        let constructor = format!("model `Page` constructor: field `{name}` has the name of the generated parameter `{name}`");
        let core = format!("field name {name} is reserved");
        assert!(message.contains(&constructor) || message.contains(&core), "{name}: wanted a refusal, got {message}");
    }
}

#[test]
fn a_relation_named_like_a_reserved_member_is_refused_naming_the_dbl_name() {
    let model = |scope: &str| format!("model Page do\n  attribute :title, :string\n  scope :{scope} do\n    order title: :asc\n  end\nend\n");
    let singleton = "model Totals do\n  attribute :count, :int\n  scope :singleton do\n    query \"Totals\"\n  end\nend\n";
    let mut rows: Vec<(String, String, String)> = reserved_member_names().into_iter().map(|name| (format!("scope {name}"), model(name), if name == "sync" { "relation name sync is reserved".to_string() } else { format!("model Page: name {name} collides with a model member") })).collect();
    rows.extend([
        ("scope singleton with a query", singleton.to_string(), "model Totals: name singleton collides with a model member"),
        ("scope snake_case modelName", model("model_name"), "generated Swift declares static property `modelName` twice in `Page`. It comes from scope `model_name`"),
        ("scope snake_case wireLayout", model("wire_layout"), "generated Swift declares static property `wireLayout` twice in `Page`. It comes from scope `wire_layout`"),
        ("scope with a trailing underscore", model("layout_"), "name `layout_` ends with `_`"),
        ("scope starting with a capital", model("Recent"), "scope name `Recent` must start with a lowercase letter"),
    ].map(|(row, source, wanted)| (row.to_string(), source, wanted.to_string())));
    for (row, source, wanted) in rows {
        let message = refusal(&source);
        assert!(message.contains(&wanted), "{row}: wanted `{wanted}`, got {message}");
    }
}

#[test]
fn every_schema_the_generator_accepts_opens() {
    let models = parse_files(&fixtures()).unwrap();
    assert!(models.len() > 20, "the fixtures declare models: {}", models.len());
    dblayer::store::Store::open_models(None, models, None, std::sync::Arc::new(dblayer::runtime::SystemClock)).unwrap();
}
