// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod monitor;

use std::{
    ffi::OsStr,
    io::{stdin, stdout, Write},
    path::Path,
    process::{exit, Command as StdCommand, Stdio},
};

use clap::{Parser, Subcommand};
use colored::Colorize;

fn check_and_install(name: &str, check: &str, install: &str, yes: bool) {
    if StdCommand::new("sh")
        .arg("-c")
        .arg(check)
        .stdout(Stdio::null())
        .status()
        .unwrap()
        .success()
    {
        return;
    }
    if !yes {
        print!(
            "Tool {name} is not installed, install it? [{y}/{n}]: ",
            name = name.magenta(),
            y = "Y".green(),
            n = "n".red()
        );
        stdout().flush().unwrap();
        let mut input = String::new();
        loop {
            stdin().read_line(&mut input).unwrap();
            match input.trim().to_lowercase().as_str() {
                "y" | "yes" | "" => {
                    break;
                }
                "n" | "no" => {
                    println!("Exit because user canceled the installation.");
                    exit(1);
                }
                _ => continue,
            }
        }
    }
    println!("Installing tool {name}...", name = name.magenta());
    if StdCommand::new("sh").arg("-c").arg(install).status().unwrap().success() {
        println!("Tool {name} installed successfully!", name = name.magenta());
    } else {
        println!(
            "Failed to install tool {name}. Please install it manually.",
            name = name.magenta()
        );
        exit(1);
    }
}

fn run(script: &str) {
    run_with_env::<Vec<(String, String)>, _, _>(script, vec![])
}

fn run_with_env<I, K, V>(script: &str, vars: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    println!("Running script: {}", script.green());
    if !StdCommand::new("sh")
        .arg("-c")
        .arg(script)
        .envs(std::env::vars())
        .envs(vars)
        .current_dir(std::env::current_dir().unwrap())
        .status()
        .unwrap()
        .success()
    {
        println!("Script `{script}` failed.", script = script.red());
        exit(1);
    }
}

fn tools(yes: bool) {
    check_and_install("typos", "which typos", "cargo install typos-cli", yes);
    check_and_install("taplo", "which taplo", "cargo install taplo-cli --locked", yes);
    check_and_install("cargo-sort", "which cargo-sort", "cargo install cargo-sort", yes);
    check_and_install(
        "cargo-machete",
        "which cargo-machete",
        "cargo install cargo-machete",
        yes,
    );
    check_and_install(
        "cargo-nextest",
        "which cargo-nextest",
        "cargo install cargo-nextest --locked",
        yes,
    );
    check_and_install(
        "license-eye",
        "which license-eye",
        &format!(
            r#"
            wget -P /tmp https://github.com/apache/skywalking-eyes/releases/download/v0.7.0/skywalking-license-eye-0.7.0-bin.tgz && \
            tar -xzf /tmp/skywalking-license-eye-0.7.0-bin.tgz -C /tmp && \
            cp /tmp/skywalking-license-eye-0.7.0-bin/bin/{}/license-eye ~/.cargo/bin/license-eye
            "#,
            if cfg!(target_os = "linux") {
                "linux"
            } else if cfg!(target_os = "macos") {
                "darwin"
            } else if cfg!(target_os = "windows") {
                "windows"
            } else {
                println!(
                    "Unsupported OS for {name} installation.",
                    name = "license-eye".magenta()
                );
                exit(1);
            },
        ),
        yes,
    );
}

fn check(fast: bool) {
    run("typos");
    json(false);
    run("cargo sort -w");
    run("taplo fmt");
    run("cargo fmt --all");
    run("cargo +nightly-2024-08-30 fmt --all -- --config-path rustfmt.nightly.toml");

    if !fast {
        run("cargo clippy --all-targets --features deadlock");
        run(r#"cargo clippy --all-targets --features tokio-console -- -A "clippy::large_enum_variant""#);
        run("cargo clippy --all-targets --features tracing");
        run("cargo clippy --all-targets --features serde");
        run("cargo clippy --all-targets --features clap");
    }
    run("cargo clippy --all-targets");
}

fn all(yes: bool, fast: bool) {
    tools(yes);
    check(fast);
    test(fast);
    example();
    udeps();
    license();
}

fn test(fast: bool) {
    run_with_env(
        r#"cargo nextest run --all --features "strict_assertions""#,
        [("RUST_BACKTRACE", "1")],
    );
    run_with_env("cargo test --doc", [("RUST_BACKTRACE", "1")]);
    run_with_env(
        r#"cargo +nightly doc --features "nightly" --no-deps"#,
        [("RUSTDOCFLAGS", "--cfg docsrs -D warnings")],
    );
    if !fast {
        run_with_env(
            r#"cargo nextest run --run-ignored ignored-only --no-capture --workspace --features "strict_assertions""#,
            [("RUST_BACKTRACE", "1")],
        );
    }
}

fn example() {
    run_with_env(r#"cargo run --example memory"#, [("RUST_BACKTRACE", "1")]);
    run_with_env(r#"cargo run --example hybrid"#, [("RUST_BACKTRACE", "1")]);
    run_with_env(r#"cargo run --example hybrid_full"#, [("RUST_BACKTRACE", "1")]);
    run_with_env(r#"cargo run --example event_listener"#, [("RUST_BACKTRACE", "1")]);
    run_with_env(
        r#"cargo run --features "tracing,jaeger" --example tail_based_tracing"#,
        [("RUST_BACKTRACE", "1")],
    );
    run_with_env(
        r#"cargo run --features "tracing,ot" --example tail_based_tracing"#,
        [("RUST_BACKTRACE", "1")],
    );
    run_with_env(r#"cargo run --example equivalent"#, [("RUST_BACKTRACE", "1")]);
    run_with_env(
        r#"cargo run --example export_metrics_prometheus_hyper"#,
        [("RUST_BACKTRACE", "1")],
    );
    run_with_env(
        r#"cargo run --features serde --example serde"#,
        [("RUST_BACKTRACE", "1")],
    );
}

fn udeps() {
    run("cargo-machete");
}

fn license() {
    run("license-eye header check");
}

fn madsim() {
    run_with_env(
        r#"cargo clippy --all-targets"#,
        [("RUSTFLAGS", r#"--cfg madsim --cfg tokio_unstable"#)],
    );
    run_with_env(
        r#"cargo nextest run --all --features "strict_assertions""#,
        [
            ("RUSTFLAGS", r#"--cfg madsim --cfg tokio_unstable"#),
            ("RUST_BACKTRACE", "1"),
        ],
    );
}

fn msrv() {
    run("cargo +1.82.0 fmt --all");
    run("cargo +1.82.0 clippy --all-targets --features deadlock");
    run("cargo +1.82.0 clippy --all-targets --features tokio-console");
    run("cargo +1.82.0 clippy --all-targets");
    run_with_env("cargo +1.82.0 nextest run --all", [("RUST_BACKTRACE", "1")]);
    run_with_env("cargo +1.82.0 test --doc", [("RUST_BACKTRACE", "1")]);
    run_with_env(
        "cargo +1.82.0 nextest run --run-ignored ignored-only --no-capture --workspace",
        [("RUST_BACKTRACE", "1")],
    );
}

fn json(check: bool) {
    const DIR: &str = "etc/grafana/dashboards";
    let entries = std::fs::read_dir(DIR).expect("Failed to read directory");
    let mut diff = false;
    for entry in entries {
        let entry = entry.expect("Failed to read entry");
        let path = entry.path();
        if path.extension().and_then(OsStr::to_str) == Some("json") {
            println!("Minimizing JSON file: {}", path.to_string_lossy().green());
            let d = minimize_json(&path, check);
            if check && d {
                println!(
                    "Json file {} is not minimized. Please run `cargo xtask json` to minimize it.",
                    path.to_string_lossy().red()
                );
            }
            diff |= d;
        }
    }
    if check && diff {
        exit(1);
    }
}

fn minimize_json(path: impl AsRef<Path>, check: bool) -> bool {
    let file_path = path.as_ref();
    let content = std::fs::read_to_string(file_path).expect("Failed to read JSON file");
    let json: serde_json::Value = serde_json::from_str(&content).expect("Failed to parse JSON");
    let minimized = serde_json::to_string(&json).expect("Failed to serialize JSON");
    let diff = content != minimized;
    if !check {
        std::fs::write(file_path, minimized).expect("Failed to write minimized JSON");
    }
    diff
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// Automatically answer yes to prompts.
    #[clap(short, long, default_value_t = false)]
    yes: bool,
    /// Skip slow or heavy checks and tests for a fast feedback.
    #[clap(short, long, default_value_t = false)]
    fast: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run all checks and tests
    All,
    /// Install necessary tools for development and testing.
    Tools,
    /// Static code analysis and checks.
    Check,
    /// Run unit tests.
    Test,
    /// Run examples.
    Example,
    /// Find unused dependeicies.
    Udeps,
    /// Check licenses headers.
    License,
    /// Run checks and tests with madsim.
    Madsim,
    /// Run checks and tests with MSRV toolchain.
    Msrv,
    /// Minimize Grafana Dashboard json files.
    Json(JsonArgs),
    /// Setup monitoring environment for foyer benchmark.
    #[clap(subcommand)]
    Monitor(monitor::MonitorCommand),
}

#[derive(Debug, Parser)]
pub struct JsonArgs {
    #[clap(short, long, default_value_t = false)]
    check: bool,
}

fn main() {
    let cli = Cli::parse();

    let command = cli.command.unwrap_or(Command::All);

    match command {
        Command::All => all(cli.yes, cli.fast),
        Command::Tools => tools(cli.yes),
        Command::Check => check(cli.fast),
        Command::Test => test(cli.fast),
        Command::Example => example(),
        Command::Udeps => udeps(),
        Command::License => license(),
        Command::Madsim => madsim(),
        Command::Msrv => msrv(),
        Command::Json(args) => json(args.check),
        Command::Monitor(cmd) => monitor::run(cmd),
    }

    println!("{}", "Done!".green());
}
