// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source License v1.1 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod control;
mod pty;

use anyhow::{Context, anyhow};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use uuid::Uuid;

pub const REPL_CONTROL_ADDR_ENV: &str = "GOLEM_REPL_CONTROL_ADDR";
pub const REPL_CONTROL_TOKEN_ENV: &str = "GOLEM_REPL_CONTROL_TOKEN";

#[derive(Clone, Debug)]
pub struct ReplCommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ReplSessionResult {
    pub exit: CommandExit,
}

#[derive(Clone, Copy, Debug)]
pub struct CommandExit {
    pub code: Option<i32>,
    pub success: bool,
}

pub fn run_repl_session(mut node_spec: ReplCommandSpec) -> anyhow::Result<ReplSessionResult> {
    let token = Uuid::new_v4().to_string();
    let control = control::ControlServer::start(token.clone())?;

    node_spec.env.insert(
        REPL_CONTROL_ADDR_ENV.to_string(),
        control.addr().to_string(),
    );
    node_spec
        .env
        .insert(REPL_CONTROL_TOKEN_ENV.to_string(), token);

    let terminal_guard = RawTerminalGuard::enter()?;
    let result = run_repl_session_raw(node_spec, control);
    drop(terminal_guard);
    result
}

fn run_repl_session_raw(
    node_spec: ReplCommandSpec,
    control: control::ControlServer,
) -> anyhow::Result<ReplSessionResult> {
    let (event_tx, event_rx) = mpsc::channel::<SupervisorEvent>();
    let mut node = pty::spawn_pty_command(node_spec)?;
    let mut active = ActiveSession::Node;
    let mut cli_writer: Option<Box<dyn Write + Send>> = None;
    let mut pending_cli_response: Option<Sender<control::RunCliResponse>> = None;

    spawn_input_reader(event_tx.clone());
    spawn_pty_output_reader(SessionId::Node, node.reader, event_tx.clone());
    spawn_waiter(SessionId::Node, node.child, event_tx.clone());
    control.spawn_request_reader(event_tx.clone());

    while let Ok(event) = event_rx.recv() {
        match event {
            SupervisorEvent::Input(bytes) => match active {
                ActiveSession::Node => {
                    let _ = node.writer.write_all(&bytes);
                    let _ = node.writer.flush();
                }
                ActiveSession::Cli => {
                    if let Some(writer) = cli_writer.as_mut() {
                        let _ = writer.write_all(&bytes);
                        let _ = writer.flush();
                    }
                }
            },
            SupervisorEvent::Output { session, bytes } => {
                if active.accepts_output(session) {
                    let mut stdout = std::io::stdout();
                    stdout.write_all(&bytes)?;
                    stdout.flush()?;
                }
            }
            SupervisorEvent::Exited { session, exit } => match session {
                SessionId::Node => return Ok(ReplSessionResult { exit }),
                SessionId::Cli => {
                    active = ActiveSession::Node;
                    cli_writer = None;
                    if let Some(response) = pending_cli_response.take() {
                        let _ = response.send(control::RunCliResponse {
                            ok: exit.success,
                            code: exit.code,
                            stdout: None,
                            stderr: None,
                        });
                    }
                }
            },
            SupervisorEvent::RunCli(request) => {
                if pending_cli_response.is_some() {
                    let _ = request.response.send(control::RunCliResponse {
                        ok: false,
                        code: None,
                        stdout: None,
                        stderr: Some("another CLI command is already running".to_string()),
                    });
                    continue;
                }

                let cli = spawn_cli_command(request.args)?;
                active = ActiveSession::Cli;
                cli_writer = Some(cli.writer);
                pending_cli_response = Some(request.response);
                spawn_pty_output_reader(SessionId::Cli, cli.reader, event_tx.clone());
                spawn_waiter(SessionId::Cli, cli.child, event_tx.clone());
            }
        }
    }

    Err(anyhow!("REPL supervisor event loop stopped unexpectedly"))
}

fn spawn_cli_command(args: Vec<String>) -> anyhow::Result<pty::PtyChild> {
    let program = crate::binary_path_to_string()?.into();
    let cwd = crate::fs::current_dir_lexical()?;
    let spec = ReplCommandSpec {
        program,
        args,
        cwd,
        env: HashMap::new(),
    };
    pty::spawn_pty_command(spec)
}

fn spawn_input_reader(event_tx: Sender<SupervisorEvent>) {
    thread::spawn(move || {
        let mut stdin = std::io::stdin();
        let mut buffer = [0_u8; 4096];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) => return,
                Ok(n) => {
                    if event_tx
                        .send(SupervisorEvent::Input(buffer[..n].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
}

fn spawn_pty_output_reader(
    session: SessionId,
    mut reader: Box<dyn Read + Send>,
    event_tx: Sender<SupervisorEvent>,
) {
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => return,
                Ok(n) => {
                    if event_tx
                        .send(SupervisorEvent::Output {
                            session,
                            bytes: buffer[..n].to_vec(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
}

fn spawn_waiter(
    session: SessionId,
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    event_tx: Sender<SupervisorEvent>,
) {
    thread::spawn(move || {
        if let Ok(status) = child.wait() {
            let code = Some(status.exit_code() as i32);
            let _ = event_tx.send(SupervisorEvent::Exited {
                session,
                exit: CommandExit {
                    code,
                    success: code == Some(0),
                },
            });
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionId {
    Node,
    Cli,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActiveSession {
    Node,
    Cli,
}

impl ActiveSession {
    fn accepts_output(self, session: SessionId) -> bool {
        matches!(
            (self, session),
            (ActiveSession::Node, SessionId::Node) | (ActiveSession::Cli, SessionId::Cli)
        )
    }
}

enum SupervisorEvent {
    Input(Vec<u8>),
    Output {
        session: SessionId,
        bytes: Vec<u8>,
    },
    Exited {
        session: SessionId,
        exit: CommandExit,
    },
    RunCli(control::RunCliSupervisorRequest),
}

struct RawTerminalGuard;

impl RawTerminalGuard {
    fn enter() -> anyhow::Result<Self> {
        enable_raw_mode().context("Failed to enable terminal raw mode")?;
        Ok(Self)
    }
}

impl Drop for RawTerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}
