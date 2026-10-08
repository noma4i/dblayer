use std::process::ExitCode;

use dblayer_gen::{parse_files, emit_swift_with, emit_kotlin, Access};

fn main() -> ExitCode {
    let mut arguments: Vec<String> = std::env::args().skip(1).collect();
    let access = if arguments.iter().any(|argument| argument == "--internal") { Access::Internal } else { Access::Public };
    arguments.retain(|argument| argument != "--internal");
    match arguments.split_first() {
        Some((command, files)) if ["spec", "swift", "kotlin"].contains(&command.as_str()) && !files.is_empty() => generate(command, files, access),
        _ => {
            eprintln!("usage: dblayer-gen [--internal] <spec|swift|kotlin> <file.dbl>...");
            ExitCode::from(2)
        }
    }
}

fn generate(command: &str, files: &[String], access: Access) -> ExitCode {
    match parse_files(files) {
        Ok(models) => {
            let output = match command {
                "swift" => emit_swift_with(&models, access),
                "kotlin" => emit_kotlin(&models),
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
