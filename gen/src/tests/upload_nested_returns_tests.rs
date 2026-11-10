use crate::compile_files;

const RETURNS: &str = "    returns do\n      attribute :moment, PhotoMoment do\n        attribute :id, :id\n      end\n      attribute :userPhoto, PhotoResult do\n        attribute :reason, :string\n        attribute :media, PhotoMedia do\n          attribute :thumbUrl, :string\n        end\n      end\n    end\n";

fn declared(body: &str) -> crate::Declarations {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("profile.dbl");
    std::fs::write(&path, body).unwrap();
    compile_files(&[path], None, &[]).unwrap_or_else(|errors| panic!("{errors:?}"))
}

/// The text of the declaration of `name` in generated code: from its line to the next blank line.
fn declaration(text: &str, name: &str) -> String {
    let start = text.lines().position(|line| (line.contains("struct ") || line.contains("class ")) && line.contains(&format!(" {name}"))).unwrap_or_else(|| panic!("no declaration of {name}"));
    text.lines().skip(start).take_while(|line| !line.is_empty()).collect::<Vec<_>>().join("\n")
}

/// models.md M53, D-287: an upload action declares no `file` field and its `returns` takes nested objects with the syntax and the normalizer of a standalone operation: the same fields and the same generated types in Swift and Kotlin.
#[test]
fn gen_upload_action_nested_returns() {
    let action = format!("{}model Profile do\n  attribute :id, :id\n  attribute :name, :string\n  operation :upload_photo, upload: true do\n    mutation \"UserPhotoUpdate\"\n    optimistic false\n    response from: \"userPhotoUpdate.viewer\"\n{RETURNS}  end\nend\n", crate::TEST_UPLOADS);
    let standalone = format!("operation :upload_photo do\n  mutation \"UserPhotoUpdate\"\n{}end\n", RETURNS.replace("\n    ", "\n  ").trim_start_matches("  "));
    let (found, call) = (declared(&action), declared(&standalone));
    let returns = found.models[0].actions[0].returns.clone().expect("the action carries its returns");
    let result = call.operations[0].result.clone().expect("the standalone result");
    assert_eq!(returns.fields, result.fields, "one normalizer: the fields of the action are the fields of the standalone operation");
    assert!(returns.fields.iter().all(|field| field.nullable), "every returned field is nullable");
    let (swift, kotlin) = (crate::emit_swift(&found.models, &found.operations, &found.inputs), crate::emit_kotlin(&found.models, &found.operations, &found.inputs));
    let (swift_call, kotlin_call) = (crate::emit_swift(&call.models, &call.operations, &call.inputs), crate::emit_kotlin(&call.models, &call.operations, &call.inputs));
    for type_name in ["PhotoMoment", "PhotoResult", "PhotoMedia"] {
        assert_eq!(declaration(&swift, type_name), declaration(&swift_call, type_name), "swift type {type_name}");
        assert_eq!(declaration(&kotlin, type_name), declaration(&kotlin_call, type_name), "kotlin type {type_name}");
    }
    assert!(declaration(&swift, "PhotoMedia").contains("thumbUrl: String?"), "{}", declaration(&swift, "PhotoMedia"));
    assert!(declaration(&kotlin, "PhotoMedia").contains("val thumbUrl: String? = null"), "{}", declaration(&kotlin, "PhotoMedia"));
}
