mod build;
mod check;
mod lexer;
mod parser;

use std::fmt;
use std::fs;
use std::path::Path;

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

pub fn parse_files(paths: &[impl AsRef<Path>]) -> Result<Vec<ModelSpec>, Vec<FileError>> {
    let names: Vec<String> = paths.iter().map(|path| path.as_ref().display().to_string()).collect();
    let mut sources = Vec::new();
    let mut errors = Vec::new();
    for (path, name) in paths.iter().zip(&names) {
        match fs::read_to_string(Path::new(path.as_ref())) {
            Ok(text) => sources.push(text),
            Err(error) => errors.push(FileError { file: name.clone(), error: ParseError::new(0, 0, error.to_string()) }),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let sources: Vec<&str> = sources.iter().map(String::as_str).collect();
    parse_sources(&sources).map_err(|found| {
        found.into_iter().map(|(file, error)| FileError { file: names[file].clone(), error }).collect()
    })
}

fn parse_sources(sources: &[&str]) -> Result<Vec<ModelSpec>, Vec<(usize, ParseError)>> {
    let mut nodes = Vec::new();
    let mut errors = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        match lexer::lex(source).and_then(|tokens| parser::parse(&tokens, file)) {
            Ok(models) => nodes.extend(models),
            Err(error) => errors.push((file, error)),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    check::check(&nodes)
}

#[cfg(test)]
mod lib_tests;
