/// The directory of this package, read when the test runs: a test binary that embeds the path of the tree is not reusable from the cache.
#[cfg(test)]
pub(crate) fn package_dir() -> std::path::PathBuf {
    std::env::current_dir().expect("the test has a working directory")
}

mod build;
mod decl;
mod check;
mod lexer;
mod operation;
mod parser;
mod schema_check;
mod emit;
mod members;
mod enums;
mod scalars;
mod unions;
pub use emit::{emit_swift, emit_swift_with, emit_kotlin, specs_text, Access};
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
    let mut upload_errors = Vec::new();
    let steps = decl::lower_uploads(&parsed.uploads, &models, &mut upload_errors);
    if !upload_errors.is_empty() {
        return Err(locate(upload_errors));
    }
    let mut declared = build_declarations(&models, &calls).map_err(locate)?;
    stamp_upload_steps(&mut declared.models, steps);
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
    let clashes = output_clash_errors(&declared, &models);
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
    let scalars = scalars::build(&parsed.scalars)?;
    let models = decl::lower_models(&parsed.models, None)?;
    let mut call_errors = Vec::new();
    let calls = decl::lower_calls(&parsed.operations, &mut call_errors);
    if !call_errors.is_empty() {
        return Err(call_errors);
    }
    let mut upload_errors = Vec::new();
    let steps = decl::lower_uploads(&parsed.uploads, &models, &mut upload_errors);
    if !upload_errors.is_empty() {
        return Err(upload_errors);
    }
    let mut declared = build_declarations(&models, &calls)?;
    stamp_upload_steps(&mut declared.models, steps);
    let clashes = name_clash_errors(&declared, &models, &calls);
    if !clashes.is_empty() { return Err(clashes); }
    let clashes = output_clash_errors(&declared, &models);
    if !clashes.is_empty() { return Err(clashes); }
    declared.scalars = scalars;
    Ok(declared)
}

#[cfg(test)]
pub(crate) const TEST_UPLOADS: &str = "uploads do\n  create_blob :CreateBlob\nend\n\n";

/// Every model with an upload action carries the steps of the project, so `open` reads them from the models.
fn stamp_upload_steps(models: &mut [ModelSpec], steps: Option<dblayer::owners::write::upload::UploadOperations>) {
    for model in models.iter_mut().filter(|model| model.actions.iter().any(|action| action.upload)) {
        model.upload_steps = steps.clone();
    }
}

fn parse_tree(sources: &[&str]) -> Result<parser::Parsed, Vec<(usize, ParseError)>> {
    let mut nodes = Vec::new();
    let mut operation_nodes = Vec::new();
    let mut scalar_nodes = Vec::new();
    let mut upload_nodes = Vec::new();
    let mut errors = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        match lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)) {
            Ok(parsed) => {
                nodes.extend(parsed.models);
                operation_nodes.extend(parsed.operations);
                scalar_nodes.extend(parsed.scalars);
                upload_nodes.extend(parsed.uploads);
            }
            Err(error) => errors.push((file, error)),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(parser::Parsed { models: nodes, operations: operation_nodes, scalars: scalar_nodes, uploads: upload_nodes })
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
    if errors.is_empty() { Ok(Declarations { models, operations, inputs: SchemaTypes::default(), scalars: Scalars::new(), notes: Vec::new() }) } else { Err(errors) }
}

fn name_clash_errors(declared: &Declarations, declared_models: &[decl::ModelDecl], calls: &[decl::CallDecl]) -> Vec<(usize, ParseError)> {
    emit::name_clashes(&declared.models, &declared.operations, &declared.inputs).into_iter().map(|(subject, message)| {
        let at = declaration_site(declared_models, calls, &subject);
        (at.0, ParseError::new(at.1.line, at.1.column, message))
    }).collect()
}

fn output_clash_errors(declared: &Declarations, declared_models: &[decl::ModelDecl]) -> Vec<(usize, ParseError)> {
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    let mut errors = Vec::new();
    for (platform, source) in [(members::Platform::Swift, swift), (members::Platform::Kotlin, kotlin)] {
        for again in members::redeclared(&source, platform) {
            let model = declared.models.iter().filter(|model| again.outer.starts_with(model.key.as_str())).max_by_key(|model| model.key.as_str().len());
            let subject = model.map_or_else(|| again.outer.clone(), |model| model.key.as_str().to_string());
            let printed_by = model.map(|model| members::declared_names(model, &again.member)).unwrap_or_default();
            let source_names = if printed_by.is_empty() { String::new() } else { format!(" It comes from {}.", printed_by.join(" and ")) };
            let at = declaration_site(declared_models, &[], &subject);
            errors.push((at.0, ParseError::new(at.1.line, at.1.column, format!(
                "model `{subject}`: generated {} declares {} `{}` twice in `{}`.{source_names} Rename one of these in the .dbl", platform.label(), again.kind, again.shown, again.path
            ))));
        }
    }
    errors
}

fn declaration_site(models: &[decl::ModelDecl], calls: &[decl::CallDecl], subject: &str) -> (usize, decl::Pos) {
    models.iter().find(|node| node.name == subject).map(|node| (node.file, node.pos))
        .or_else(|| calls.iter().find(|call| call.name == subject).map(|call| (call.file, call.pos)))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;






























