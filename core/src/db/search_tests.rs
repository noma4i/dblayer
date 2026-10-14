use super::fixtures::*;
use super::*;
use crate::query::{Order, Query, QueryError, Where};
use crate::schema::{resolve_types, DeclarationError, FieldSpec, ModelSpec, Storage};
use crate::value::{ScalarKind, SortDirection};
use serde_json::json;

fn searchable(mut field: FieldSpec) -> FieldSpec {
    field.searchable = true;
    field
}

fn notes_spec(storage: Storage) -> ModelSpec {
    let mut author = FieldSpec::object("author");
    author.app_type = Some("NoteAuthor".to_string());
    author.fields = vec![searchable(FieldSpec::scalar("fullName", ScalarKind::Str).nullable()), FieldSpec::scalar("age", ScalarKind::Int).nullable()];
    let mut spec = ModelSpec::new(
        "notes",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            searchable(FieldSpec::scalar("body", ScalarKind::Str).optional()),
            searchable(FieldSpec::list("tags", ScalarKind::Str).optional()),
            author.nullable().optional(),
            FieldSpec::scalar("secret", ScalarKind::Str).optional(),
        ],
    );
    spec.storage = storage;
    resolve_types(std::slice::from_mut(&mut spec));
    spec
}

fn plain_spec() -> ModelSpec {
    ModelSpec::new("plain", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str).optional()])
}

fn opened(storage: Storage) -> Db {
    Db::open_in_memory(vec![notes_spec(storage), plain_spec()], None).unwrap()
}

fn found(db: &Db, text: &str, whole_words: bool) -> Vec<String> {
    let rows = db.query(&Query::new("notes").filtered(Where::search(text, whole_words))).unwrap();
    let mut ids: Vec<String> = rows.iter().map(|row| row.id.to_string()).collect();
    ids.sort();
    ids
}

fn seed(db: &Db) {
    put(db, "notes", json!({"id": "n1", "chatId": "c1", "body": "hello world", "tags": ["greeting"], "secret": "zebra"}));
    put(db, "notes", json!({"id": "n2", "chatId": "c1", "body": "help me please", "author": {"fullName": "Alice Cooper", "age": 4}}));
    put(db, "notes", json!({"id": "n3", "chatId": "c2", "body": "yellow submarine", "tags": ["music", "Beatles"]}));
}

#[test]
fn the_last_word_matches_as_a_prefix_and_the_others_as_whole_words() {
    let db = opened(Storage::Disk);
    seed(&db);
    assert_eq!(found(&db, "hel", false), ["n1", "n2"]);
    assert_eq!(found(&db, "hello wor", false), ["n1"]);
    assert_eq!(found(&db, "hel wor", false), Vec::<String>::new());
}

#[test]
fn whole_words_turn_the_prefix_off() {
    let db = opened(Storage::Disk);
    seed(&db);
    assert_eq!(found(&db, "hel", true), Vec::<String>::new());
    assert_eq!(found(&db, "hello", true), ["n1"]);
}

#[test]
fn text_of_lists_and_nested_objects_is_searched_and_other_fields_are_not() {
    let db = opened(Storage::Disk);
    seed(&db);
    assert_eq!(found(&db, "beatles", false), ["n3"]);
    assert_eq!(found(&db, "cooper", false), ["n2"]);
    assert_eq!(found(&db, "zebra", false), Vec::<String>::new());
}

#[test]
fn case_and_accents_do_not_matter() {
    let db = opened(Storage::Disk);
    put(&db, "notes", json!({"id": "n1", "chatId": "c1", "body": "Café Crème"}));
    assert_eq!(found(&db, "CAFE creme", false), ["n1"]);
}

#[test]
fn a_search_follows_updates_and_deletes() {
    let db = opened(Storage::Disk);
    seed(&db);
    put(&db, "notes", json!({"id": "n1", "chatId": "c1", "body": "goodbye moon", "tags": ["greeting"]}));
    assert_eq!(found(&db, "hello", false), Vec::<String>::new());
    assert_eq!(found(&db, "moon", false), ["n1"]);
    db.destroy("notes", &rid("n3")).unwrap();
    db.settle();
    assert_eq!(found(&db, "submarine", false), Vec::<String>::new());
}

#[test]
fn a_search_combines_with_filters_counts_and_negation() {
    let db = opened(Storage::Disk);
    seed(&db);
    let both = Where::all_of(vec![Where::search("hel", false), Where::equal("chatId", json!("c1"))]);
    assert_eq!(db.query(&Query::new("notes").filtered(both.clone())).unwrap().len(), 2);
    assert_eq!(db.count(&Query::new("notes").filtered(both)).unwrap(), 2);
    let rest = Where::negated(Where::search("hel", false));
    assert_eq!(db.query(&Query::new("notes").filtered(rest)).unwrap().len(), 1);
}

#[test]
fn the_default_order_is_the_rank_and_an_explicit_order_wins() {
    let db = opened(Storage::Disk);
    put(&db, "notes", json!({"id": "a", "chatId": "c", "body": "cat dog bird fish horse mouse"}));
    put(&db, "notes", json!({"id": "b", "chatId": "c", "body": "cat"}));
    let ranked: Vec<String> = db.query(&Query::new("notes").filtered(Where::search("cat", true))).unwrap().iter().map(|row| row.id.to_string()).collect();
    assert_eq!(ranked, ["b", "a"]);
    let by_id = Query::new("notes").filtered(Where::search("cat", true)).ordered(vec![Order::new("id", SortDirection::Asc)]);
    assert_eq!(db.query(&by_id).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>(), ["a", "b"]);
}

#[test]
fn search_syntax_in_the_text_is_plain_text() {
    let db = opened(Storage::Disk);
    seed(&db);
    for text in ["\"hello", "hello OR (", "NEAR(", "* ^", "hello\"world", "-", "   ", ""] {
        found(&db, text, false);
    }
    assert_eq!(found(&db, "hello OR yellow", false), Vec::<String>::new());
    assert_eq!(found(&db, "   ", false), Vec::<String>::new());
}

#[test]
fn a_memory_model_is_searchable() {
    let db = opened(Storage::Memory);
    seed(&db);
    assert_eq!(found(&db, "yell", false), ["n3"]);
}

#[test]
fn a_search_on_a_model_without_searchable_fields_or_with_two_searches_is_refused() {
    let db = opened(Storage::Disk);
    let plain = db.query(&Query::new("plain").filtered(Where::search("x", false)));
    assert!(matches!(plain, Err(DbError::Query(QueryError::NoSearchableField(_)))), "{plain:?}");
    let twice = db.query(&Query::new("notes").filtered(Where::all_of(vec![Where::search("a", false), Where::search("b", false)])));
    assert!(matches!(twice, Err(DbError::Query(QueryError::SeveralSearches))), "{twice:?}");
}

#[test]
fn an_observed_search_reports_a_row_when_its_text_starts_to_match() {
    let db = opened(Storage::Disk);
    put(&db, "notes", json!({"id": "n1", "chatId": "c1", "body": "nothing"}));
    let (log, listener) = recorder();
    let _handle = db.observe(Query::new("notes").filtered(Where::search("rocket", false)), listener).unwrap();
    db.settle();
    put(&db, "notes", json!({"id": "n1", "chatId": "c1", "body": "rocket science"}));
    db.settle();
    assert_eq!(changes(&log).last().map(ids), Some(vec!["n1".to_string()]));
}

#[test]
fn searchable_is_only_for_text() {
    let mut spec = ModelSpec::new("bad", vec![FieldSpec::scalar("id", ScalarKind::Id), searchable(FieldSpec::scalar("count", ScalarKind::Int))]);
    assert_eq!(spec.validate(&[]), Err(DeclarationError::SearchableNotText("count".into())));
    spec.fields[1] = searchable(FieldSpec::list("tags", ScalarKind::Int));
    assert_eq!(spec.validate(&[]), Err(DeclarationError::SearchableNotText("tags".into())));
}
