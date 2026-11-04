mod build;
mod decl;
mod check;
mod lexer;
mod operation;
mod parser;
mod schema_check;
mod emit;
mod enums;
mod scalars;
mod blob;
#[cfg(test)]
mod blob_tests;
pub use blob::{emit_blob_swift, emit_blob_kotlin};
mod unions;
pub use emit::{emit_swift, emit_swift_with, emit_kotlin, Access, MODEL_MEMBERS};
pub use enums::EnumDecl;
pub use scalars::Scalars;
pub use operation::OperationCall;
pub use schema_check::{InputField, InputObject, SchemaTypes, Ty, TyKind};

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use dblayer::schema::ModelSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub column: usize,
    pub message: String,
}

impl ParseError {
    pub fn new(line: usize, column: usize, message: impl Into<String>) -> Self {
        Self { line, column, message: message.into() }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}: {}", self.line, self.column, self.message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileError {
    pub file: String,
    pub error: ParseError,
}

impl fmt::Display for FileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}: {}", self.file, self.error.line, self.error.message)
    }
}

pub fn parse_models(source: &str) -> Result<Vec<ModelSpec>, Vec<ParseError>> {
    parse_sources(&[source]).map_err(|errors| errors.into_iter().map(|(_, error)| error).collect())
}

#[derive(Debug)]
pub struct Declarations {
    pub blob_uploads: Vec<dblayer::upload::BlobUploadDeclaration>,
    pub models: Vec<ModelSpec>,
    pub operations: Vec<OperationCall>,
    pub inputs: SchemaTypes,
    pub scalars: Scalars,
    pub notes: Vec<String>,
}

pub fn parse_files(paths: &[impl AsRef<Path>]) -> Result<Vec<ModelSpec>, Vec<FileError>> {
    compile_files(paths, None, &[]).map(|declarations| declarations.models)
}

pub fn compile_files(paths: &[impl AsRef<Path>], schema: Option<&Path>, operations: &[PathBuf]) -> Result<Declarations, Vec<FileError>> {
    let (names, sources) = read_sources(paths)?;
    let sources: Vec<&str> = sources.iter().map(String::as_str).collect();
    let locate = |found: Vec<(usize, ParseError)>| {
        found.into_iter().map(|(file, error)| FileError { file: names[file].clone(), error }).collect::<Vec<_>>()
    };
    let parsed = parse_tree(&sources).map_err(locate)?;
    let blob_uploads = blob::lower(&parsed.blob_uploads).map_err(locate)?;
    let scalars = scalars::build(&parsed.scalars).map_err(locate)?;
    let loaded = schema.map(schema_check::Schema::load).transpose().map_err(|message| vec![FileError {
        file: schema.unwrap().display().to_string(), error: ParseError::new(0, 0, message),
    }])?;
    let models = decl::lower_models(&parsed.models, loaded.as_ref()).map_err(locate)?;
    if loaded.is_none() && models.iter().flat_map(|model| &model.scopes).any(|scope| scope.marker.is_some()) {
        return Err(vec![FileError { file: names[0].clone(), error: ParseError::new(1, 1, "marker and tail require --schema and operation documents") }]);
    }
    let mut call_errors = Vec::new();
    let calls = decl::lower_calls(&parsed.operations, &mut call_errors);
    if !call_errors.is_empty() {
        return Err(locate(call_errors));
    }
    let checked = loaded.as_ref().map(|schema| schema_check::check_schema(schema, &scalars, operations, &names, &models, &calls)).transpose()?;
    if !blob_uploads.is_empty() {
        let Some(schema) = &loaded else { return Err(vec![FileError { file: names[0].clone(), error: ParseError::new(1, 1, "blob_upload requires --schema and operation documents") }]); };
        schema_check::check_blob_uploads(schema, operations, &blob_uploads).map_err(|message| vec![FileError { file: names[0].clone(), error: ParseError::new(1, 1, message) }])?;
    }
    let mut declared = build_declarations(&models, &calls).map_err(locate)?;
    declared.blob_uploads = blob_uploads;
    if let Some(checked) = &checked {
        for media in declared.models.iter_mut().flat_map(|model| &mut model.relations).filter_map(|scope| scope.media.as_mut()) {
            media.marker_variables.retain(|name, _| checked.variables.get(&media.marker).is_some_and(|variables| variables.contains(name)));
            media.tail_variables.retain(|name, _| checked.variables.get(&media.tail).is_some_and(|variables| variables.contains(name)));
        }
        for action in declared.models.iter_mut().flat_map(|model| &mut model.actions) {
            action.client_mutation_id = action.mutation.as_ref().is_some_and(|name| checked.idempotent_inputs.contains(&(name.clone(), action.wrap.clone().unwrap_or_else(|| "input".into()))));
        }
    }
    let failed = |message: String| vec![FileError { file: names.first().cloned().unwrap_or_default(), error: ParseError::new(0, 0, message) }];
    declared.inputs = inputs_from_schema(loaded.as_ref(), &scalars, &declared.models, &declared.operations).map_err(failed)?;
    operation::resolve_result_enums(&mut declared.operations, loaded.as_ref()).map_err(failed)?;
    emit::enum_decls(&declared.models, &declared.operations, &declared.inputs).map_err(failed)?;
    let clashes = name_clash_errors(&declared, &models, &calls);
    if !clashes.is_empty() { return Err(locate(clashes)); }
    declared.scalars = scalars;
    declared.notes = checked.map_or_else(Vec::new, |checked| checked.notes);
    Ok(declared)
}

pub fn load_schema_types(schema: Option<&Path>, scalars: &Scalars, models: &[ModelSpec], operations: &[OperationCall]) -> Result<SchemaTypes, String> {
    let loaded = schema.map(schema_check::Schema::load).transpose()?;
    inputs_from_schema(loaded.as_ref(), scalars, models, operations)
}

fn inputs_from_schema(schema: Option<&schema_check::Schema>, scalars: &Scalars, models: &[ModelSpec], operations: &[OperationCall]) -> Result<SchemaTypes, String> {
    let wanted = operation::schema_type_names(models, operations);
    match (schema, wanted.first()) {
        (_, None) => Ok(SchemaTypes::default()),
        (None, Some(name)) => Err(format!("a declaration names the schema type `{name}`: pass --schema <schema.graphql> so the generator can read it")),
        (Some(schema), Some(_)) => schema_check::load_schema_types(schema, scalars, &wanted),
    }
}

pub fn check_models_against_schema(schema: &Path, operations: &[PathBuf], paths: &[impl AsRef<Path>]) -> Result<Vec<String>, Vec<FileError>> {
    compile_files(paths, Some(schema), operations).map(|declarations| declarations.notes)
}

fn read_sources(paths: &[impl AsRef<Path>]) -> Result<(Vec<String>, Vec<String>), Vec<FileError>> {
    let names: Vec<String> = paths.iter().map(|path| path.as_ref().display().to_string()).collect();
    let mut sources = Vec::new();
    let mut errors = Vec::new();
    for (path, name) in paths.iter().zip(&names) {
        match fs::read_to_string(Path::new(path.as_ref())) {
            Ok(text) => sources.push(text),
            Err(error) => errors.push(FileError { file: name.clone(), error: ParseError::new(0, 0, error.to_string()) }),
        }
    }
    if errors.is_empty() { Ok((names, sources)) } else { Err(errors) }
}

fn parse_sources(sources: &[&str]) -> Result<Vec<ModelSpec>, Vec<(usize, ParseError)>> {
    parse_all(sources).map(|declarations| declarations.models)
}

fn parse_all(sources: &[&str]) -> Result<Declarations, Vec<(usize, ParseError)>> {
    let parsed = parse_tree(sources)?;
    let blob_uploads = blob::lower(&parsed.blob_uploads)?;
    let scalars = scalars::build(&parsed.scalars)?;
    let models = decl::lower_models(&parsed.models, None)?;
    let mut call_errors = Vec::new();
    let calls = decl::lower_calls(&parsed.operations, &mut call_errors);
    if !call_errors.is_empty() {
        return Err(call_errors);
    }
    let mut declared = build_declarations(&models, &calls)?;
    let clashes = name_clash_errors(&declared, &models, &calls);
    if !clashes.is_empty() { return Err(clashes); }
    declared.blob_uploads = blob_uploads;
    declared.scalars = scalars;
    Ok(declared)
}

fn parse_tree(sources: &[&str]) -> Result<parser::Parsed, Vec<(usize, ParseError)>> {
    let mut nodes = Vec::new();
    let mut operation_nodes = Vec::new();
    let mut scalar_nodes = Vec::new();
    let mut blob_uploads = Vec::new();
    let mut errors = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        match lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)) {
            Ok(parsed) => {
                nodes.extend(parsed.models);
                operation_nodes.extend(parsed.operations);
                scalar_nodes.extend(parsed.scalars);
                blob_uploads.extend(parsed.blob_uploads);
            }
            Err(error) => errors.push((file, error)),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(parser::Parsed { models: nodes, operations: operation_nodes, scalars: scalar_nodes, blob_uploads })
}

fn build_declarations(declared_models: &[decl::ModelDecl], calls: &[decl::CallDecl]) -> Result<Declarations, Vec<(usize, ParseError)>> {
    let mut errors = Vec::new();
    let operations = operation::build(calls, &mut errors);
    let models = match check::check(declared_models) {
        Ok(models) => models,
        Err(found) => {
            errors.extend(found);
            return Err(errors);
        }
    };
    if errors.is_empty() { Ok(Declarations { models, operations, inputs: SchemaTypes::default(), scalars: Scalars::new(), notes: Vec::new(), blob_uploads: Vec::new() }) } else { Err(errors) }
}

fn name_clash_errors(declared: &Declarations, declared_models: &[decl::ModelDecl], calls: &[decl::CallDecl]) -> Vec<(usize, ParseError)> {
    emit::name_clashes(&declared.models, &declared.operations, &declared.inputs).into_iter().map(|(subject, message)| {
        let at = declaration_site(declared_models, calls, &subject);
        (at.0, ParseError::new(at.1.line, at.1.column, message))
    }).collect()
}

fn declaration_site(models: &[decl::ModelDecl], calls: &[decl::CallDecl], subject: &str) -> (usize, decl::Pos) {
    models.iter().find(|node| node.name == subject).map(|node| (node.file, node.pos))
        .or_else(|| calls.iter().find(|call| call.name == subject).map(|call| (call.file, call.pos)))
        .unwrap_or_default()
}

#[cfg(test)]
mod lib_tests;

#[cfg(test)]
mod roundtrip_tests;

#[cfg(test)]
mod docs_tests;

#[cfg(test)]
mod short_form_tests;

#[cfg(test)]
mod closed_effects_tests;

#[cfg(test)]
mod nested_tests;

#[cfg(test)]
mod storage_tests;

#[cfg(test)]
mod args_tests;

#[cfg(test)]
mod operation_tests;

#[cfg(test)]
mod input_tests;

#[cfg(test)]
mod enum_tests;

#[cfg(test)]
mod result_tests;

#[cfg(test)]
mod active_record_tests;

#[cfg(test)]
mod union_tests;
#[cfg(test)]
mod model_enum_args_tests;

#[cfg(test)]
mod interface_tests;

#[cfg(test)]
mod input_list_tests;

#[cfg(test)]
mod edge_tests;

#[cfg(test)]
mod enum_list_tests;

#[cfg(test)]
mod scope_enum_tests;
#[cfg(test)]
mod subscription_args_tests;
#[cfg(test)]
mod scalar_param_tests;
#[cfg(test)]
mod order_stable_tests;
#[cfg(test)]
mod scope_default_tests;
#[cfg(test)]
mod media_tests;

#[cfg(test)]
mod enum_position_tests;

#[cfg(test)]
mod edge_from_tests;

#[cfg(test)]
mod edge_enum_tests;

#[cfg(test)]
mod enum_walk_tests;

#[cfg(test)]
mod enum_schema_conflict_tests;

#[cfg(test)]
mod upload_batch_tests;

#[cfg(test)]
mod raw_json_tests;

#[cfg(test)]
mod patch_tests;

#[cfg(test)]
mod file_field_tests;
