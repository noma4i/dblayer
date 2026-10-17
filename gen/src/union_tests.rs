use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};
use dblayer::schema::FieldKind;

const MESSAGE: &str = "model Message do
  field :text, type: :string
  field :attachment, type: MessageAttachment, null: true do
    on GiftAttachment do
      field :id, type: :id
      field :tier, type: :string, from: \"giftTier\"
      field :tags, type: [:string], default: []
    end
    on MomentAttachment do
      field :momentId, type: :id
      field :caption, type: :string, null: true
    end
  end
end
";

fn variants(source: &str) -> Vec<(String, String, Vec<String>)> {
    let models = parse_models(source).unwrap();
    let field = models[0].field("attachment").unwrap();
    assert_eq!((&field.kind, field.app_type.as_deref(), field.nullable), (&FieldKind::Union, Some("MessageAttachment"), true));
    field.fields.iter().map(|variant| (variant.name.clone(), variant.app_type.clone().unwrap(), variant.fields.iter().map(|member| member.name.clone()).collect())).collect()
}

#[test]
fn a_union_field_declares_one_object_type_per_variant() {
    let found = variants(MESSAGE);
    assert_eq!(found[0], ("gift".to_string(), "GiftAttachment".to_string(), vec!["id".to_string(), "tier".to_string(), "tags".to_string()]));
    assert_eq!(found[1], ("moment".to_string(), "MomentAttachment".to_string(), vec!["momentId".to_string(), "caption".to_string()]));
}

#[test]
fn case_names_drop_the_words_every_variant_shares_and_follow_case_overrides() {
    let names = |types: &[&str], extra: &str| {
        let blocks: String = types.iter().map(|name| format!("    on {name}{extra} do\n      field :id, type: :id\n    end\n")).collect();
        let models = parse_models(&format!("model Item do\n  field :props, type: ItemProps, null: true do\n{blocks}  end\nend\n")).unwrap();
        models[0].field("props").unwrap().fields.iter().map(|variant| variant.name.clone()).collect::<Vec<_>>()
    };
    assert_eq!(names(&["GiftAttachment", "MomentAttachment", "ProfileAttachment"], ""), ["gift", "moment", "profile"]);
    assert_eq!(names(&["MomentEmojiProps", "MomentGifProps", "MomentHashtagProps"], ""), ["emoji", "gif", "hashtag"]);
    let overridden = parse_models("model Item do\n  field :props, type: ItemProps do\n    on FirstProps, case: :primary do\n      field :id, type: :id\n    end\n    on SecondProps do\n      field :id, type: :id\n    end\n  end\nend\n").unwrap();
    assert_eq!(overridden[0].field("props").unwrap().fields.iter().map(|variant| variant.name.as_str()).collect::<Vec<_>>(), ["primary", "second"]);
}

#[test]
fn a_union_named_without_a_block_is_reused_and_variants_can_be_shared() {
    let source = format!("{MESSAGE}model Draft do\n  field :attachment, type: MessageAttachment, null: true\n  field :other, type: Other, null: true do\n    on GiftAttachment\n    on ExtraAttachment do\n      field :id, type: :id\n    end\n  end\nend\n");
    let models = parse_models(&source).unwrap();
    let reused = models[1].field("attachment").unwrap();
    assert_eq!((&reused.kind, reused.app_type.as_deref(), reused.fields.len()), (&FieldKind::Union, Some("MessageAttachment"), 0));
    assert_eq!(models[1].field("other").unwrap().fields[0].app_type.as_deref(), Some("GiftAttachment"));
}

#[test]
fn union_declarations_reject_invalid_variants_at_their_line() {
    let wrap = |body: &str| format!("model Sample do\n\n  field :v, type: Value, null: true do\n{body}  end\nend\n");
    let variant = |name: &str| format!("    on {name} do\n      field :id, type: :id\n    end\n");
    let cases = [
        (wrap(&format!("{}{}", variant("OneProps"), variant("OneProps"))), "declared twice"),
        (wrap("    on EmptyProps do\n    end\n"), "at least 1 field"),
        (wrap("    on OneProps do\n      field :id, type: :id\n    end\n    field :loose, type: :string\n"), "only `on` variants"),
        (wrap(&variant("Unknown")), "reserved"),
        (wrap("    on OneProps, case: :same do\n      field :id, type: :id\n    end\n    on TwoProps, case: :same do\n      field :id, type: :id\n    end\n"), "both become"),
        ("model Sample do\n  field :v, type: :object do\n    on OneProps do\n      field :id, type: :id\n    end\n  end\nend\n".to_string(), "only inside a union"),
        ("model Sample do\n  field :v, type: Value, null: true, default: 1 do\n    on OneProps do\n      field :id, type: :id\n    end\n  end\nend\n".to_string(), "unknown key `default`"),
        ("model Sample do\n  field :v, type: Value, null: true do\n    on OneProps do\n      field :v, type: Value, null: true\n    end\n  end\nend\n".to_string(), "recursive"),
    ];
    for (source, error) in cases {
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.message.contains(error)), "{source}: {errors:?}");
    }
}

#[test]
fn swift_unions_are_codable_enums_with_an_unknown_case() {
    let swift = emit_swift(&parse_models(MESSAGE).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "public enum MessageAttachment: Codable, Sendable, Equatable {",
        "case gift(GiftAttachment)",
        "case moment(MomentAttachment)",
        "case unknown(typename: String)",
        "case \"GiftAttachment\": self = .gift(try GiftAttachment(from: decoder))",
        "public init(cells: [Cell]) throws",
        "public let attachment: MessageAttachment?",
        "self.attachment = try r.objOpt(1, MessageAttachment.init(cells:))",
        "public enum MessageAttachmentKind: String, Codable, Sendable {",
        "case gift = \"GiftAttachment\"",
        "public static let attachment = Column<Message, MessageAttachmentKind>(\"attachment\")",
        "public struct GiftAttachment: Codable, Sendable, Equatable {",
    ] {
        assert!(swift.contains(token), "missing Swift `{token}`:\n{swift}");
    }
}

#[test]
fn a_kotlin_case_named_like_its_type_refers_to_the_type_by_its_package() {
    let source = "model Item do\n  field :a, type: Attachment, null: true do\n    on Gift do\n      field :id, type: :id\n    end\n    on Moment do\n      field :id, type: :id\n    end\n  end\nend\n";
    let kotlin = emit_kotlin(&parse_models(source).unwrap(), &[], &SchemaTypes::default());
    assert!(kotlin.contains("data class Gift(val value: com.noma4i.dblayer.models.Gift) : Attachment()"), "{kotlin}");
    assert!(kotlin.contains("\"Gift\" -> Gift(r.obj(1) { com.noma4i.dblayer.models.Gift.fromCells(it) })"), "{kotlin}");
    assert!(kotlin.contains("\"Gift\" -> Attachment.Gift(dbJson.decodeFromJsonElement(Gift.serializer(), body))"), "{kotlin}");
}

#[test]
fn kotlin_unions_are_sealed_classes_with_an_unknown_case() {
    let kotlin = emit_kotlin(&parse_models(MESSAGE).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "@Serializable(with = MessageAttachmentSerializer::class)\nsealed class MessageAttachment {",
        "data class Gift(val value: GiftAttachment) : MessageAttachment()",
        "data class Unknown(val typename: String) : MessageAttachment()",
        "fun fromCells(cells: List<Cell>): MessageAttachment",
        "object MessageAttachmentSerializer : UnionSerializer<MessageAttachment>(\"MessageAttachment\")",
        "\"GiftAttachment\" -> MessageAttachment.Gift(dbJson.decodeFromJsonElement(GiftAttachment.serializer(), body))",
        "val attachment: MessageAttachment? = null",
        "enum class MessageAttachmentKind {",
        "@SerialName(\"GiftAttachment\") Gift,",
        "val attachment = Column<Message, MessageAttachmentKind>(\"attachment\")",
        "data class GiftAttachment(",
    ] {
        assert!(kotlin.contains(token), "missing Kotlin `{token}`:\n{kotlin}");
    }
}

#[test]
fn a_union_too_wide_for_one_sqlite_table_is_rejected_at_its_model() {
    let variant = |at: usize| format!("    on Wide{at}Item do\n{}    end\n", (0..60).map(|member| format!("      field :m{member}, type: :string, null: true\n")).collect::<String>());
    let source = format!("model Screen do\n  field :spec, type: ScreenSpec, null: true do\n{}  end\nend\n", (0..40).map(variant).collect::<String>());
    let errors = parse_models(&source).unwrap_err();
    assert!(errors.iter().any(|found| found.message == "model Screen: table row_Screen would have 2405 columns, above the SQLite limit of 2000. A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model"), "{errors:?}");
}
