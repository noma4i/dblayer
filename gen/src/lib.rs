mod build;
mod check;
mod lexer;
mod operation;
mod parser;
mod schema_check;
mod emit;
pub use emit::{emit_swift, emit_swift_with, emit_kotlin, Access};
pub use operation::OperationCall;
pub use schema_check::{InputField, InputObject};

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

pub struct Declarations {
    pub models: Vec<ModelSpec>,
    pub operations: Vec<OperationCall>,
}

pub fn parse_files(paths: &[impl AsRef<Path>]) -> Result<Vec<ModelSpec>, Vec<FileError>> {
    parse_declarations(paths).map(|declarations| declarations.models)
}

pub fn parse_declarations(paths: &[impl AsRef<Path>]) -> Result<Declarations, Vec<FileError>> {
    let (names, sources) = read_sources(paths)?;
    let sources: Vec<&str> = sources.iter().map(String::as_str).collect();
    parse_all(&sources).map_err(|found| {
        found.into_iter().map(|(file, error)| FileError { file: names[file].clone(), error }).collect()
    })
}

pub fn load_input_objects(schema: Option<&Path>, operations: &[OperationCall]) -> Result<Vec<InputObject>, String> {
    let wanted = operation::input_object_names(operations);
    match (schema, wanted.first()) {
        (_, None) => Ok(Vec::new()),
        (None, Some(name)) => Err(format!("an operation names the input object `{name}`: pass --schema <schema.graphql> so the generator can read it")),
        (Some(schema), Some(_)) => schema_check::load_input_objects(schema, &wanted),
    }
}

pub fn check_models_against_schema(schema: &Path, operations: &[PathBuf], paths: &[impl AsRef<Path>]) -> Result<Vec<String>, Vec<FileError>> {
    parse_files(paths)?;
    let (names, sources) = read_sources(paths)?;
    let mut nodes = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        nodes.extend(lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)).expect("sources parsed above").models);
    }
    schema_check::check_schema(schema, operations, &names, &nodes).map(|outcome| outcome.notes)
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
    let mut nodes = Vec::new();
    let mut operation_nodes = Vec::new();
    let mut errors = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        match lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)) {
            Ok(parsed) => {
                nodes.extend(parsed.models);
                operation_nodes.extend(parsed.operations);
            }
            Err(error) => errors.push((file, error)),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let operations = operation::build(&operation_nodes, &mut errors);
    let models = match check::check(&nodes) {
        Ok(models) => models,
        Err(found) => {
            errors.extend(found);
            return Err(errors);
        }
    };
    if errors.is_empty() { Ok(Declarations { models, operations }) } else { Err(errors) }
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
