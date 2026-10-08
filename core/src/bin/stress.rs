use std::process::ExitCode;
use std::time::Instant;

use dblayer::sync::stress::{minimize, run_checked, StressConfig};

const USAGE: &str = "usage: stress [--seed N[,N...]] [--threads T] [--seconds S] [--ops N] [--no-minimize] [--trace]";

struct Arguments {
    seeds: Vec<u64>,
    threads: usize,
    seconds: u64,
    ops: Option<u64>,
    minimize: bool,
    trace: bool,
}

fn parse() -> Result<Arguments, String> {
    let mut arguments = Arguments { seeds: vec![1], threads: 4, seconds: 10, ops: None, minimize: true, trace: false };
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        if flag == "--no-minimize" || flag == "--trace" {
            arguments.minimize &= flag != "--no-minimize";
            arguments.trace |= flag == "--trace";
            continue;
        }
        let value = input.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let number = |text: &str| text.parse::<u64>().map_err(|_| format!("{flag}: {text} is not a number"));
        match flag.as_str() {
            "--seed" => arguments.seeds = value.split(',').map(number).collect::<Result<_, _>>()?,
            "--threads" => arguments.threads = number(&value)? as usize,
            "--seconds" => arguments.seconds = number(&value)?,
            "--ops" => arguments.ops = Some(number(&value)?),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    Ok(arguments)
}

fn main() -> ExitCode {
    let arguments = match parse() {
        Ok(arguments) => arguments,
        Err(message) => {
            eprintln!("{message}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut failed = false;
    for seed in &arguments.seeds {
        let config = StressConfig { seed: *seed, threads: arguments.threads, seconds: arguments.seconds, ops: arguments.ops, trace: arguments.trace };
        let started = Instant::now();
        let report = run_checked(&config);
        let total: u64 = report.ops.iter().sum();
        println!("seed {seed} threads {} ops {total} (per worker {:?}) requests {} in {:.1}s: {}", config.threads, report.ops, report.requests, started.elapsed().as_secs_f32(), if report.findings.is_empty() { "ok" } else { "FAILED" });
        if report.findings.is_empty() {
            continue;
        }
        failed = true;
        report.findings.iter().take(20).for_each(|finding| println!("  {finding}"));
        println!("  {} findings in total", report.findings.len());
        println!("  reproduce: {}", config.command(config.ops));
        if arguments.minimize {
            match minimize(&config, &report) {
                Some(ops) => println!("  minimal: {}", config.command(Some(ops))),
                None => println!("  minimal: the failure did not reproduce with an operation limit"),
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
