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

use clap::Subcommand;

use crate::run_with_env;

#[derive(Debug, Subcommand)]
pub enum MonitorCommand {
    /// Start the monitoring environment.
    Up,
    /// Stop the monitoring environment.
    Down,
    /// Clear the monitoring environment.
    Clear,
}

pub fn run(cmd: MonitorCommand) {
    match cmd {
        MonitorCommand::Up => up(),
        MonitorCommand::Down => down(),
        MonitorCommand::Clear => clear(),
    }
}

fn up() {
    let uid = std::env::var("UID").unwrap_or_else(|_| "1000".to_string());
    std::fs::write(
        "docker-compose.override.yaml",
        format!(
            r#"
services:
    prometheus:
        user: "{uid}"
"#
        )
        .trim(),
    )
    .unwrap();
    run_with_env("mkdir -p .tmp/prometheus", std::env::vars());
    run_with_env("docker compose up -d", std::env::vars());
}

fn down() {
    run_with_env("docker compose down", std::env::vars());
}

fn clear() {
    run_with_env("rm -rf .tmp", std::env::vars());
}
