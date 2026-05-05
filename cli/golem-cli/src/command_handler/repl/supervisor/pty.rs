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

use super::ReplCommandSpec;
use anyhow::Context;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use std::io::{Read, Write};

pub struct PtyChild {
    pub reader: Box<dyn Read + Send>,
    pub writer: Box<dyn Write + Send>,
    pub child: Box<dyn portable_pty::Child + Send + Sync>,
}

pub fn spawn_pty_command(spec: ReplCommandSpec) -> anyhow::Result<PtyChild> {
    let pty_system = native_pty_system();
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let pair = pty_system
        .openpty(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("Failed to open PTY")?;

    let mut command = CommandBuilder::new(spec.program);
    command.args(spec.args);
    command.cwd(spec.cwd);
    for (key, value) in spec.env {
        command.env(key, value);
    }

    let child = pair
        .slave
        .spawn_command(command)
        .context("Failed to spawn PTY command")?;
    let reader = pair
        .master
        .try_clone_reader()
        .context("Failed to clone PTY reader")?;
    let writer = pair
        .master
        .take_writer()
        .context("Failed to take PTY writer")?;

    Ok(PtyChild {
        reader,
        writer,
        child,
    })
}
