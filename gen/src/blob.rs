use std::collections::HashSet;
use dblayer::upload::BlobUploadDeclaration;
use crate::parser::{Call, Node, Value};
use crate::ParseError;

pub(crate) fn lower(calls: &[(usize, Call)]) -> Result<Vec<BlobUploadDeclaration>, Vec<(usize, ParseError)>> {
    let mut declarations = Vec::new();
    let mut errors = Vec::new();
    let mut names = HashSet::new();
    for (file, call) in calls {
        let fail = |message| (*file, ParseError::new(call.line, call.column, message));
        let [Node { item: Value::Symbol(name), .. }] = call.args.as_slice() else { errors.push(fail("blob_upload needs a symbol name")); continue };
        if call.block.is_some() || call.options.len() != 1 || call.options[0].key != "create" { errors.push(fail("blob_upload needs only create: operation name")); continue; }
        let Value::Str(create) = &call.options[0].value.item else { errors.push(fail("blob_upload create must be a mutation name string")); continue };
        if !names.insert(name.clone()) { errors.push(fail("blob_upload name is declared twice")); continue; }
        declarations.push(BlobUploadDeclaration { name: name.clone(), create: create.clone() });
    }
    if errors.is_empty() { Ok(declarations) } else { Err(errors) }
}

pub fn emit_blob_swift(declarations: &[BlobUploadDeclaration]) -> String {
    if declarations.is_empty() { return String::new(); }
    let mut result = "\npublic extension BlobUpload {\n".to_string();
    for declaration in declarations {
        let class = crate::build::camel(&declaration.name);
        let mut chars = class.chars();
        let name = chars.next().unwrap().to_lowercase().to_string() + chars.as_str();
        result.push_str(&format!("    static let {name} = BlobUpload(name: {:?}, create: {:?})\n", declaration.name, declaration.create));
    }
    result.push_str("}\n");
    result
}

pub fn emit_blob_kotlin(declarations: &[BlobUploadDeclaration]) -> String {
    declarations.iter().map(|declaration| format!("\nval BlobUpload.Companion.{}: BlobUpload get() = BlobUpload({:?}, {:?})\n", crate::build::camel(&declaration.name), declaration.name, declaration.create)).collect()
}
