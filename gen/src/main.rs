use std::path::PathBuf;
use std::process::ExitCode;

use dblayer_gen::{check_models_against_schema, load_input_objects, parse_declarations, emit_swift_with, emit_kotlin, Access, Declarations};

const USAGE: &str = "usage: dblayer-gen [--internal] [--schema <schema.graphql> [--operations <path>]...] [<spec|swift|kotlin>] <file.dbl>...";

struct Options {
    access: Access,
    schema: Option<PathBuf>,
    operations: Vec<PathBuf>,
    rest: Vec<String>,
}

fn options() -> Option<Options> {
    let mut found = Options { access: Access::Public, schema: None, operations: Vec::new(), rest: Vec::new() };
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--internal" => found.access = Access::Internal,
            "--schema" => found.schema = Some(PathBuf::from(arguments.next()?)),
            "--operations" => found.operations.push(PathBuf::from(arguments.next()?)),
            _ => found.rest.push(argument),
        }
    }
    Some(found)
}

fn main() -> ExitCode {
    let Some(options) = options() else { return usage() };
    let (command, files) = match options.rest.split_first() {
        Some((command, files)) if ["spec", "swift", "kotlin"].contains(&command.as_str()) => (Some(command.as_str()), files),
        _ => (None, options.rest.as_slice()),
    };
    if files.is_empty() || (command.is_none() && options.schema.is_none()) {
        return usage();
    }
    if let Some(schema) = &options.schema {
        let code = check_schema(schema, options.operations, files);
        if code != ExitCode::SUCCESS || command.is_none() {
            return code;
        }
    }
    match command {
        Some(command) => generate(command, files, options.access, options.schema.as_deref()),
        None => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

fn check_schema(schema: &std::path::Path, mut operations: Vec<PathBuf>, files: &[String]) -> ExitCode {
    if operations.is_empty() {
        let beside = ["queries", "mutations"].map(|name| schema.parent().map(|parent| parent.join(name)));
        operations.extend(beside.into_iter().flatten().filter(|beside| beside.is_dir()));
    }
    match check_models_against_schema(schema, &operations, files) {
        Ok(notes) => {
            for note in notes {
                println!("note: {note}");
            }
            ExitCode::SUCCESS
        }
        Err(errors) => {
            for error in errors {
                eprintln!("{error}");
            }
            ExitCode::from(1)
        }
    }
}

fn generate(command: &str, files: &[String], access: Access, schema: Option<&std::path::Path>) -> ExitCode {
    match parse_declarations(files) {
        Ok(Declarations { models, operations }) => {
            let inputs = match load_input_objects(schema, &operations) {
                Ok(inputs) => inputs,
                Err(message) => {
                    eprintln!("{message}");
                    return ExitCode::from(1);
                }
            };
            let output = match command {
                "swift" => emit_swift_with(&models, &operations, &inputs, access),
                "kotlin" => emit_kotlin(&models, &operations, &inputs),
                _ => serde_json::to_string_pretty(&models).expect("specs serialize"),
            };
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(errors) => {
            for error in errors {
                eprintln!("{error}");
            }
            ExitCode::from(1)
        }
    }
}
