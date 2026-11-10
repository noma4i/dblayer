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

fn consumer() -> crate::Declarations {
    let app = crate::package_dir().join("../fixtures/consumer/app");
    let mut models: Vec<std::path::PathBuf> = std::fs::read_dir(app.join("schemas/models")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    models.extend(std::fs::read_dir(app.join("schemas/operations")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")));
    models.sort();
    let documents: Vec<std::path::PathBuf> = std::fs::read_dir(app.join("graphql")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.is_dir()).collect();
    compile_files(&models, Some(&app.join("graphql/schema.graphql")), &documents).unwrap_or_else(|errors| panic!("{errors:?}"))
}

/// D-287: an upload action has a call on the type of the model, so an upload needs no instance: a singleton takes no id, a model takes its id; the instance method calls the one on the type, with the same variables on both platforms.
#[test]
fn gen_upload_action_type_call() {
    let found = consumer();
    let (swift, kotlin) = (crate::emit_swift(&found.models, &found.operations, &found.inputs), crate::emit_kotlin(&found.models, &found.operations, &found.inputs));
    let swift_type = |name: &str| { let start = swift.find(&format!("public struct {name}:")).expect("the model"); &swift[start..start + swift[start..].find("\npublic struct ").unwrap_or(swift.len() - start)] };
    let current = swift_type("CurrentUser");
    assert!(current.contains("public static func updatePhoto(source: String, createInitialMoment: Bool? = nil, skipAssign: Bool? = nil, in scope_: AccountScope) async throws -> MutationReceipt<Self>"), "singleton: no id");
    assert!(current.contains("scope_.upload(Self.self, id: \"singleton\", action: \"update_photo\", UploadRequest(path: source, variables: variables_))"));
    assert!(current.contains("public func updatePhoto(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {\n        try await scope_.upload(Self.self, id: id, action: \"update_photo\", request)"), "the instance method calls the one on the type");
    let chat = swift_type("Chat");
    assert!(chat.contains("public static func updatePhoto(id: String, source: String") && chat.contains("scope_.upload(Self.self, id: id, action: \"update_photo\""), "a model takes its id");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, source: String, createInitialMoment: Boolean? = null, skipAssign: Boolean? = null): MutationReceipt<CurrentUser> =\n            account.upload(this, \"singleton\", \"update_photo\", UploadRequest(source, variables = buildJsonObject { if (createInitialMoment != null) this.put(\"createInitialMoment\", jsonValue(createInitialMoment)); if (skipAssign != null) this.put(\"skipAssign\", jsonValue(skipAssign)) }))"), "kotlin singleton");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, id: String, source: String"), "kotlin model takes its id");
    assert!(kotlin.contains("suspend fun updatePhoto(account: AccountScope, request: UploadRequest): MutationReceipt<CurrentUser> = account.upload(CurrentUser, id, \"update_photo\", request)"), "kotlin instance method calls the one on the type");
}

/// A generated Swift binding that is never assigned is declared with `let`: the consumer builds with warnings as errors.
#[test]
fn gen_swift_variables_binding_is_mutated() {
    let found = consumer();
    let swift = crate::emit_swift(&found.models, &found.operations, &found.inputs);
    let lines: Vec<&str> = swift.lines().collect();
    let never: Vec<usize> = lines.iter().enumerate().filter(|(_, line)| line.trim_start().starts_with("var variables_")).filter(|(at, _)| !lines[at + 1..].iter().take_while(|line| !line.contains("return ")).any(|line| line.contains("variables_[\""))).map(|(at, _)| at + 1).collect();
    assert!(never.is_empty(), "`var variables_` is never assigned at generated lines {never:?}");
}
