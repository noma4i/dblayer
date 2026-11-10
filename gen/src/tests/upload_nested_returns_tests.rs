use crate::compile_files;

const RETURNS: &str = "    returns do\n      attribute :moment, PhotoMoment do\n        attribute :id, :id\n      end\n      attribute :userPhoto, PhotoResult do\n        attribute :reason, :string\n        attribute :media, PhotoMedia do\n          attribute :thumbUrl, :string\n        end\n      end\n    end\n";

fn declared(body: &str) -> crate::Declarations {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("profile.dbl");
    std::fs::write(&path, format!("{}{body}", crate::TEST_UPLOADS)).unwrap();
    compile_files(&[path], None, &[]).unwrap_or_else(|errors| panic!("{errors:?}"))
}

fn unnamed(mut value: serde_json::Value) -> serde_json::Value {
    match &mut value {
        serde_json::Value::Object(map) => { map.remove("app_type"); map.values_mut().for_each(|inner| *inner = unnamed(inner.take())); }
        serde_json::Value::Array(items) => items.iter_mut().for_each(|inner| *inner = unnamed(inner.take())),
        _ => {}
    }
    value
}

/// models.md M53, D-287: an upload action declares no `file` field and its `returns` takes nested objects with the syntax and the normalizer of a standalone operation: the same fields, the same types in Swift and Kotlin.
#[test]
fn gen_upload_action_nested_returns() {
    let model = format!("model Profile do\n  attribute :id, :id\n  attribute :name, :string\n  operation :upload_photo, upload: true do\n    mutation \"UserPhotoUpdate\"\n    optimistic false\n    response from: \"userPhotoUpdate.viewer\"\n{RETURNS}  end\nend\n");
    let standalone = format!("operation :alt_photo do\n  mutation \"UserPhotoUpdate\"\n{}end\n", RETURNS.replace("PhotoMoment", "AltMoment").replace("PhotoResult", "AltResult").replace("PhotoMedia", "AltMedia").replace("\n    ", "\n  ").trim_start_matches("  "));
    let found = declared(&model);
    let action = serde_json::to_value(&found.models[0].actions[0]).unwrap();
    let call = serde_json::to_value(declared(&format!("{model}{standalone}")).operations.iter().find(|call| call.name == "alt_photo").and_then(|call| call.result.clone()).expect("the standalone result")).unwrap();
    assert_eq!(unnamed(action["returns"]["fields"].clone()), unnamed(call["fields"].clone()), "one normalizer: the fields of the action equal the fields of the standalone operation");
    assert_eq!(action["returns"]["fields"][0]["fields"][0]["name"], "id", "the nested object keeps its fields");
    assert_eq!(action["returns"]["fields"][1]["fields"][1]["fields"][0]["name"], "thumbUrl", "two levels");
    assert!(action["returns"]["fields"].as_array().unwrap().iter().all(|field| field["nullable"] == true), "every returned field is nullable");
    let swift = crate::emit_swift(&found.models, &found.operations, &found.inputs);
    let kotlin = crate::emit_kotlin(&found.models, &found.operations, &found.inputs);
    for (name, text) in [("swift", &swift), ("kotlin", &kotlin)] {
        for type_name in ["PhotoMoment", "PhotoResult", "PhotoMedia"] {
            assert!(text.contains(type_name), "{name} lacks the type {type_name}");
        }
        assert!(text.contains("thumbUrl"), "{name} lacks the nested field");
    }
}
