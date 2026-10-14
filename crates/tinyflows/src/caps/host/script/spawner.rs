//! The seam a host uses to put script processes inside its own jail.
//!
//! The runner stages the script and its input on disk, then hands one
//! [`SpawnSpec`] to a [`ScriptSpawner`]. The default [`DirectSpawner`] runs the
//! interpreter as a plain child of this process (the historical behaviour). A
//! host with a sandbox installs its own spawner with [`install_script_spawner`]
//! so every script, `code` and `shell` alike, runs through the jail.
//!
//! A host that must never run an unjailed script also calls
//! [`require_script_spawner`]: from then on a run with no installed spawner
//! fails closed instead of falling back to [`DirectSpawner`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;

use super::*;

/// One fully staged process launch.
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// The program to run: a bare name or an absolute path.
    pub program: String,
    /// Every argument, in order: the interpreter's leading arguments, then the
    /// script path, then the input path.
    pub args: Vec<String>,
    /// Environment layered over the inherited one.
    pub env: BTreeMap<String, String>,
    /// The working directory.
    pub cwd: PathBuf,
    /// The directory holding the staged script and `input.json`. A jail must
    /// make it readable inside the box.
    pub staging_dir: PathBuf,
    /// Bytes written to the process's stdin.
    pub stdin: Vec<u8>,
    /// How long the process may run.
    pub timeout: Duration,
}

/// What a spawned process produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnOutput {
    /// Exit status, or `-1` when a signal terminated it.
    pub exit_code: i32,
    /// Raw stdout.
    pub stdout: Vec<u8>,
    /// Raw stderr.
    pub stderr: Vec<u8>,
}

/// Launches a staged script process, possibly inside a jail.
#[async_trait]
pub trait ScriptSpawner: Send + Sync {
    /// Run `spec` to completion.
    ///
    /// # Errors
    ///
    /// Fails when the process cannot be launched (or the jail refuses it) or it
    /// outlives `spec.timeout`.
    async fn spawn(&self, spec: SpawnSpec) -> Result<SpawnOutput>;
}

/// Runs the interpreter as an unjailed child of this process.
#[derive(Debug, Default, Clone, Copy)]
pub struct DirectSpawner;

#[async_trait]
impl ScriptSpawner for DirectSpawner {
    async fn spawn(&self, spec: SpawnSpec) -> Result<SpawnOutput> {
        use tokio::io::AsyncWriteExt;

        let SpawnSpec {
            program,
            args,
            env,
            cwd,
            stdin: body,
            timeout,
            ..
        } = spec;
        let mut command = tokio::process::Command::new(&program);
        command
            .args(&args)
            .envs(&env)
            .current_dir(&cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Tokio does not kill a child when its future is dropped, so a
            // timeout would otherwise leave an infinite script running.
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|err| {
            EngineError::Capability(format!(
                "script: cannot run `{program}` ({err}). Is it installed and on PATH?"
            ))
        })?;
        // Stdin is written on its own task while stdout/stderr drain, so a
        // script that prints before reading all of stdin cannot deadlock.
        let mut stdin = child.stdin.take();
        let writer = tokio::spawn(async move {
            if let Some(mut stdin) = stdin.take() {
                let _ = stdin.write_all(&body).await;
                let _ = stdin.shutdown().await;
            }
        });
        let output = tokio::time::timeout(timeout, async {
            let output = child.wait_with_output().await;
            let _ = writer.await;
            output
        })
        .await
        .map_err(|_| {
            EngineError::Capability(format!("script: timed out after {}s", timeout.as_secs()))
        })?
        .map_err(|err| EngineError::Capability(format!("script: {program}: {err}")))?;
        Ok(SpawnOutput {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

struct SpawnerState {
    installed: Option<Arc<dyn ScriptSpawner>>,
    required: bool,
}

static STATE: RwLock<SpawnerState> = RwLock::new(SpawnerState {
    installed: None,
    required: false,
});

/// Route every script this process runs through `spawner`.
pub fn install_script_spawner(spawner: Arc<dyn ScriptSpawner>) {
    tracing::debug!("script spawner installed");
    STATE.write().unwrap_or_else(|e| e.into_inner()).installed = Some(spawner);
}

/// Remove the installed spawner, returning to [`DirectSpawner`].
pub fn clear_script_spawner() {
    STATE.write().unwrap_or_else(|e| e.into_inner()).installed = None;
}

/// Fail closed: while `required`, a script run with no installed spawner is
/// refused rather than run unjailed.
pub fn require_script_spawner(required: bool) {
    STATE.write().unwrap_or_else(|e| e.into_inner()).required = required;
}

/// The spawner a run should use.
///
/// # Errors
///
/// Fails when a spawner is required and none is installed.
pub(super) fn current_spawner() -> Result<Arc<dyn ScriptSpawner>> {
    let state = STATE.read().unwrap_or_else(|e| e.into_inner());
    match (&state.installed, state.required) {
        (Some(spawner), _) => Ok(Arc::clone(spawner)),
        (None, true) => Err(EngineError::Capability(
            "script: refused, a script spawner is required but none is installed (fail closed)"
                .to_string(),
        )),
        (None, false) => Ok(Arc::new(DirectSpawner)),
    }
}

#[cfg(test)]
#[path = "spawner_tests.rs"]
mod tests;
