use std::fs;
use std::path::Path;

use crate::check::check_source;

fn dbl_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        match (&mut current, line.trim_end()) {
            (None, "```ruby") => current = Some(String::new()),
            (Some(block), "```") => {
                if block.trim_start().starts_with("model ") && !block.contains("...") {
                    blocks.push(std::mem::take(block));
                }
                current = None;
            }
            (Some(block), _) => {
                block.push_str(line);
                block.push('\n');
            }
            (None, _) => {}
        }
    }
    blocks
}

fn passes_gen(block: &str) -> Result<(), String> {
    let mut source = block.to_string();
    for _ in 0..8 {
        let Err(errors) = check_source(&source) else { return Ok(()) };
        let missing: Vec<String> = errors
            .iter()
            .filter_map(|error| Some(error.message.split("targets unknown model `").nth(1)?.split('`').next()?.to_string()))
            .collect();
        let cross_model = errors.iter().filter(|error| (error.message.contains("foreign key of association") && error.message.contains("is not indexed")) || error.message.contains("is not declared by any `do ... end` block")).count();
        if missing.len() + cross_model != errors.len() {
            return Err(format!("{errors:?}"));
        }
        if missing.is_empty() {
            return Ok(());
        }
        for name in missing {
            source.push_str(&format!("\nmodel {name} do\nend\n"));
        }
    }
    Err("unknown models do not resolve".into())
}

#[test]
fn inv_doc_01_every_dbl_block_in_the_docs_and_the_readme_passes_gen() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = vec![root.join("README.md")];
    for entry in fs::read_dir(root.join("docs")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "md") {
            files.push(path);
        }
    }
    let mut checked = 0;
    for file in files {
        for block in dbl_blocks(&fs::read_to_string(&file).unwrap()) {
            checked += 1;
            if let Err(errors) = passes_gen(&block) {
                panic!("{}: a .dbl block does not pass gen: {errors}\n{block}", file.display());
            }
        }
    }
    assert!(checked >= 3, "only {checked} .dbl blocks found");
}
