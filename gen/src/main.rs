use std::path::PathBuf;
use std::process::ExitCode;

use dblayer_gen::{compile_files, emit_swift_with, emit_kotlin, Access, Declarations};

mod explain;

const USAGE: &str = "usage: dblayer-gen [--internal] [--schema <schema.graphql> [--operations <path>]...] [<spec|swift|kotlin|explain>] <file.dbl>...";

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
    let Some(mut options) = options() else { return usage() };
    let (command, files) = match options.rest.split_first() {
        Some((command, files)) if ["spec", "swift", "kotlin", "explain"].contains(&command.as_str()) => (Some(command.as_str()), files),
        _ => (None, options.rest.as_slice()),
    };
    if files.is_empty() || (command.is_none() && options.schema.is_none()) {
        return usage();
    }
    if let Some(schema) = &options.schema {
        if options.operations.is_empty() {
            let beside = ["queries", "mutations", "subscriptions"].map(|name| schema.parent().map(|parent| parent.join(name)));
            options.operations.extend(beside.into_iter().flatten().filter(|beside| beside.is_dir()));
        }
    }
    match compile_files(files, options.schema.as_deref(), &options.operations) {
        Ok(Declarations { models, operations, inputs, notes, .. }) => {
            for note in notes {
                eprintln!("note: {note}");
            }
            let Some(command) = command else { return ExitCode::SUCCESS };
            let output = match command {
                "swift" => emit_swift_with(&models, &operations, &inputs, options.access),
                "kotlin" => emit_kotlin(&models, &operations, &inputs),
                "explain" => match explain::report(&models) {
                    Ok(report) => report,
                    Err(error) => { eprintln!("explain: {error}"); return ExitCode::from(1); }
                },
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

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}
