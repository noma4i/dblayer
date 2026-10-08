use std::process::ExitCode;

use dblayer_gen::{parse_files, emit_swift, emit_kotlin};

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.split_first() {
        Some((command, files)) if ["spec", "swift", "kotlin"].contains(&command.as_str()) && !files.is_empty() => generate(command, files),
        _ => {
            eprintln!("usage: dblayer-gen <spec|swift|kotlin> <file.dbl>...");
            ExitCode::from(2)
        }
    }
}

fn generate(command: &str, files: &[String]) -> ExitCode {
    match parse_files(files) {
        Ok(models) => {
            let output = match command {
                "swift" => emit_swift(&models),
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
