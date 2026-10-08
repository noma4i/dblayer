use std::process::ExitCode;

use dblayer_gen::parse_files;

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.split_first() {
        Some((command, files)) if command == "spec" && !files.is_empty() => spec(files),
        _ => {
            eprintln!("usage: dblayer-gen spec <file.dbl>...");
            ExitCode::from(2)
        }
    }
}

fn spec(files: &[String]) -> ExitCode {
    match parse_files(files) {
        Ok(models) => {
            println!("{}", serde_json::to_string_pretty(&models).expect("specs serialize"));
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
