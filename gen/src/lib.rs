mod build;
mod declarations;
mod check;
mod lexer;
mod operation;
mod parser;
mod schema_check;
mod emit;
mod enums;
mod scalars;
pub use emit::{emit_swift, emit_swift_with, emit_kotlin, Access};
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
    let mut parsed = parse_tree(&sources).map_err(&locate)?;
    let scalars = scalars::build(&parsed.scalars).map_err(&locate)?;
    let loaded = schema.map(schema_check::Schema::load).transpose().map_err(|message| vec![FileError {
        file: schema.unwrap().display().to_string(), error: ParseError::new(0, 0, message),
    }])?;
    if let Some(schema) = &loaded {
        declarations::infer(&mut parsed.models, schema).map_err(&locate)?;
    }
    declarations::prepare(&mut parsed.models).map_err(&locate)?;
    let notes = match &loaded {
        Some(schema) => schema_check::check_schema(schema, &scalars, operations, &names, &parsed.models, &parsed.operations)?.notes,
        None => Vec::new(),
    };
    let mut declared = build_declarations(parsed).map_err(locate)?;
    let failed = |message: String| vec![FileError { file: names.first().cloned().unwrap_or_default(), error: ParseError::new(0, 0, message) }];
    declared.inputs = inputs_from_schema(loaded.as_ref(), &scalars, &declared.operations).map_err(&failed)?;
    operation::resolve_result_enums(&mut declared.operations, loaded.as_ref()).map_err(&failed)?;
    declared.scalars = scalars;
    declared.notes = notes;
    Ok(declared)
}

pub fn load_schema_types(schema: Option<&Path>, scalars: &Scalars, operations: &[OperationCall]) -> Result<SchemaTypes, String> {
    let loaded = schema.map(schema_check::Schema::load).transpose()?;
    inputs_from_schema(loaded.as_ref(), scalars, operations)
}

fn inputs_from_schema(schema: Option<&schema_check::Schema>, scalars: &Scalars, operations: &[OperationCall]) -> Result<SchemaTypes, String> {
    let wanted = operation::schema_type_names(operations);
    match (schema, wanted.first()) {
        (_, None) => Ok(SchemaTypes::default()),
        (None, Some(name)) => Err(format!("an operation names the schema type `{name}`: pass --schema <schema.graphql> so the generator can read it")),
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
    let mut parsed = parse_tree(sources)?;
    declarations::prepare(&mut parsed.models)?;
    let scalars = scalars::build(&parsed.scalars)?;
    let mut declared = build_declarations(parsed)?;
    declared.scalars = scalars;
    Ok(declared)
}

fn parse_tree(sources: &[&str]) -> Result<parser::Parsed, Vec<(usize, ParseError)>> {
    let mut nodes = Vec::new();
    let mut operation_nodes = Vec::new();
    let mut scalar_nodes = Vec::new();
    let mut errors = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        match lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)) {
            Ok(parsed) => {
                nodes.extend(parsed.models);
                operation_nodes.extend(parsed.operations);
                scalar_nodes.extend(parsed.scalars);
            }
            Err(error) => errors.push((file, error)),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(parser::Parsed { models: nodes, operations: operation_nodes, scalars: scalar_nodes })
}

fn build_declarations(parsed: parser::Parsed) -> Result<Declarations, Vec<(usize, ParseError)>> {
    let mut errors = Vec::new();
    let operations = operation::build(&parsed.operations, &mut errors);
    let models = match check::check(&parsed.models) {
        Ok(models) => models,
        Err(found) => {
            errors.extend(found);
            return Err(errors);
        }
    };
    if errors.is_empty() { Ok(Declarations { models, operations, inputs: SchemaTypes::default(), scalars: Scalars::new(), notes: Vec::new() }) } else { Err(errors) }
}

#[cfg(test)]
mod lib_tests;

#[cfg(test)]
mod roundtrip_tests;

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
