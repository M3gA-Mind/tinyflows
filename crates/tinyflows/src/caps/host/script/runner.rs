use super::*;

/// Run the script `request` describes, requiring it to succeed.
///
/// # Errors
///
/// Fails when the interpreter is missing, the script exits non-zero, or it
/// outlives `request.timeout`. The message carries the interpreter's own stderr,
/// which is the only thing that says what actually went wrong.
pub async fn run_script(request: ScriptRequest<'_>) -> Result<ScriptOutput> {
    /// Bytes of `stderr` folded into the error message. This message becomes
    /// `RunRecord::error` once the engine surfaces it, which — unlike step
    /// `input`/`output` — is not passed through `bounded_within`; a script
    /// that dumps a large stack trace must not be able to grow that field
    /// without limit.
    const MAX_STDERR_BYTES: usize = 4 * 1024;

    let program = request.program().to_string();
    let completion = run_script_capture(request).await?;
    if completion.exit_code != 0 {
        let stderr = if completion.stderr.len() > MAX_STDERR_BYTES {
            let end = completion
                .stderr
                .char_indices()
                .map(|(index, _)| index)
                .take_while(|index| *index <= MAX_STDERR_BYTES)
                .last()
                .unwrap_or(0);
            format!("{} …[truncated]", &completion.stderr[..end])
        } else {
            completion.stderr.clone()
        };
        return Err(EngineError::Capability(format!(
            "script: {program} exited with {}{}",
            completion.exit_code,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        )));
    }
    Ok(ScriptOutput {
        // Structured when the script printed JSON, the raw text otherwise — a
        // script that just prints a line should still be usable downstream.
        value: serde_json::from_str(&completion.stdout)
            .unwrap_or(Value::String(completion.stdout.clone())),
        stderr: completion.stderr,
    })
}

/// Run the script `request` describes, reporting its exit status rather than
/// treating a non-zero one as a failure.
///
/// # Errors
///
/// Fails when the interpreter cannot be spawned, the request cannot be staged
/// on disk, or the script outlives `request.timeout` — that is, when there is no
/// exit status to report at all.
pub async fn run_script_capture(request: ScriptRequest<'_>) -> Result<ScriptCompletion> {
    let ScriptRequest {
        language,
        interpreter,
        source,
        input,
        timeout,
        cwd,
        env,
    } = request;

    // Refused rather than emulated. `argv[1]` and `TINYFLOWS_INPUT` are real
    // filesystem paths this host wrote (`C:\...` on Windows), and Git Bash — the
    // only `bash` a Windows host is likely to have — cannot open a Windows path
    // without translating it, which is exactly the kind of per-platform
    // reinterpretation that would make a workflow look portable while quietly
    // behaving differently by host. `javascript` and `python` need no such
    // translation and stay available everywhere their interpreter is.
    #[cfg(windows)]
    if language == ScriptLanguage::Shell {
        return Err(EngineError::Capability(
            "script: shell scripts are not supported on Windows (no portable POSIX shell to \
             run them in); use language: \"javascript\" or \"python\" instead"
                .to_string(),
        ));
    }

    // The extension always comes from the language; only the program is
    // negotiable. A `.sh` staged for `zsh` is still a shell script.
    let (default_program, extension) = language.program();
    let (program, leading_args) = match interpreter {
        Some(chosen) => (chosen.program.as_str(), chosen.args.as_slice()),
        None => (default_program, &[][..]),
    };
    let dir =
        tempfile::tempdir().map_err(|err| EngineError::Capability(format!("script: {err}")))?;

    let script: PathBuf = match source {
        ScriptSource::Inline(source) => {
            let staged = dir.path().join(format!("script.{extension}"));
            std::fs::write(&staged, source)
                .map_err(|err| EngineError::Capability(format!("script: {err}")))?;
            staged
        }
        ScriptSource::File(path) => path.to_path_buf(),
    };

    // The input reaches the script two ways because the languages want
    // different ones: a pipe reads naturally in node and python, a path reads
    // naturally in shell. Writing both costs one small file.
    let input_path = dir.path().join("input.json");
    let body = serde_json::to_vec(input)
        .map_err(|err| EngineError::Capability(format!("script: {err}")))?;
    std::fs::write(&input_path, &body)
        .map_err(|err| EngineError::Capability(format!("script: {err}")))?;

    // Resolved before anything runs so a fail-closed host refuses here.
    let spawner = spawner::current_spawner()?;
    let mut args: Vec<String> = leading_args.to_vec();
    args.push(script.to_string_lossy().into_owned());
    args.push(input_path.to_string_lossy().into_owned());
    // Layered after `INPUT_ENV` so a workflow's own declaration wins over the
    // inherited value of the same name.
    let mut spawn_env = BTreeMap::new();
    spawn_env.insert(
        INPUT_ENV.to_string(),
        input_path.to_string_lossy().into_owned(),
    );
    spawn_env.extend(env.iter().map(|(k, v)| (k.clone(), v.clone())));
    tracing::debug!(program, "script: spawning");
    let output = spawner
        .spawn(spawner::SpawnSpec {
            program: program.to_string(),
            args,
            env: spawn_env,
            cwd: cwd.map_or_else(|| dir.path().to_path_buf(), Path::to_path_buf),
            staging_dir: dir.path().to_path_buf(),
            stdin: body,
            timeout,
        })
        .await?;

    Ok(ScriptCompletion {
        // `-1` for a signal: `ExitStatus::code()` is `None` when a process was
        // terminated rather than exiting, and a caller comparing against zero
        // must not read that as success.
        exit_code: output.exit_code,
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}
