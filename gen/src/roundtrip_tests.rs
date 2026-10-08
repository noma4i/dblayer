use dblayer::db::Db;
use dblayer::identity::RowId;
use dblayer::query::Query;
use serde_json::{json, Map, Value};

use crate::parse_models;

struct Dice(u64);

impl Dice {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

const KINDS: [(&str, fn() -> Value); 10] = [
    ("string", || json!("text")),
    ("num", || json!(1.5)),
    ("int", || json!(3)),
    ("bool", || json!(true)),
    ("date", || json!("2026-01-01T00:00:00.000Z")),
    ("id", || json!("x1")),
    ("object", || json!({"a": 1})),
    ("array", || json!([1, 2])),
    ("raw", || json!("raw")),
    ("list", || json!([1, 2])),
];

fn random_model(dice: &mut Dice) -> (String, Map<String, Value>) {
    let mut source = String::from("model Sample do\n  schema do\n");
    let mut values = Map::new();
    if dice.below(2) == 0 {
        source.push_str("    id :id\n");
    }
    for index in 0..1 + dice.below(5) {
        let (word, value) = KINDS[dice.below(KINDS.len())];
        let name = format!("field{index}");
        let mut line = format!("    {word} :{name}");
        if word == "list" {
            line.push_str(", of: :int");
        }
        for (key, chance) in [("null: true", 3), ("optional: true", 3), ("index: true", 4)] {
            let indexable = !matches!(word, "object" | "array" | "raw" | "list");
            if dice.below(chance) == 0 && (indexable || !key.starts_with("index")) {
                line.push_str(&format!(", {key}"));
            }
        }
        source.push_str(&line);
        source.push('\n');
        values.insert(name, value());
    }
    source.push_str("  end\nend\n");
    (source, values)
}

#[test]
fn inv_row_09_inv_dsl_01_every_accepted_model_survives_create_find_observe_update_destroy_and_rollback() {
    let mut accepted = 0;
    for seed in 1..=200u64 {
        let mut dice = Dice(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let (source, values) = random_model(&mut dice);
        let Ok(models) = parse_models(&source) else { continue };
        if !dblayer::schema::validate_models(&models, &[]).is_empty() {
            continue;
        }
        accepted += 1;
        let model = models[0].key.to_string();
        let model = model.as_str();
        let context = format!("seed {seed}\n{source}");
        let db = Db::open_in_memory(models, None).unwrap_or_else(|error| panic!("{context}: {error}"));
        let heard = std::sync::Arc::new(std::sync::Mutex::new(0usize));
        let sink = heard.clone();
        let _observer = db.observe(Query::new(model), move |change| *sink.lock().unwrap() += change.rows.len()).unwrap_or_else(|error| panic!("{context}: {error}"));
        let row = db.create(model, values.clone()).unwrap_or_else(|error| panic!("{context}: {error}"));
        let found = db.find(model, &row.id).unwrap_or_else(|error| panic!("{context}: {error}"));
        assert!(found.is_some(), "{context}: create returned a row that find cannot read");
        db.settle();
        assert!(*heard.lock().unwrap() >= 1, "{context}: the observer saw no row");
        let key = values.keys().next().cloned().unwrap();
        db.update(model, &row.id, values.iter().filter(|(name, _)| **name == key).map(|(name, value)| (name.clone(), value.clone())).collect()).unwrap_or_else(|error| panic!("{context}: {error}"));
        db.destroy(model, &row.id).unwrap_or_else(|error| panic!("{context}: {error}"));
        assert!(db.find(model, &row.id).unwrap().is_none(), "{context}: destroyed row is still readable");
        db.discard(model, &RowId::new(row.id.as_str()).unwrap()).unwrap_or_else(|error| panic!("{context}: {error}"));
        assert!(db.find(model, &row.id).unwrap().is_none(), "{context}: discarding a created row left it");
    }
    assert!(accepted >= 100, "only {accepted} of 200 generated models were accepted");
}
