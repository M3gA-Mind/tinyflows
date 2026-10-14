use super::*;
use serde_json::json;
use std::sync::Mutex;

/// The spawner state is process-global, so these cases run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

struct Recording(Mutex<Vec<SpawnSpec>>);

#[async_trait]
impl ScriptSpawner for Recording {
    async fn spawn(&self, spec: SpawnSpec) -> Result<SpawnOutput> {
        self.0.lock().unwrap().push(spec);
        Ok(SpawnOutput {
            exit_code: 0,
            stdout: b"{\"jailed\":true}".to_vec(),
            stderr: Vec::new(),
        })
    }
}

fn request<'a>(input: &'a Value, env: &'a BTreeMap<String, String>) -> ScriptRequest<'a> {
    ScriptRequest::plain(
        ScriptLanguage::Python,
        "print(1)",
        input,
        Duration::from_secs(5),
        env,
    )
}

#[tokio::test]
async fn an_installed_spawner_receives_the_staged_script() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let recording = Arc::new(Recording(Mutex::new(Vec::new())));
    install_script_spawner(recording.clone());
    let env = BTreeMap::from([("A".to_string(), "b".to_string())]);
    let input = json!({"n": 1});
    let output = run_script(request(&input, &env)).await.expect("runs");
    clear_script_spawner();

    assert_eq!(output.value, json!({"jailed": true}));
    let specs = recording.0.lock().unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].program, "python3");
    assert_eq!(specs[0].args.len(), 2);
    assert_eq!(specs[0].env.get("A").map(String::as_str), Some("b"));
    assert!(specs[0].env.contains_key(INPUT_ENV));
    assert_eq!(specs[0].stdin, b"{\"n\":1}");
}

#[tokio::test]
async fn a_required_spawner_fails_closed_when_none_is_installed() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_script_spawner();
    require_script_spawner(true);
    let env = BTreeMap::new();
    let input = json!(null);
    let result = run_script(request(&input, &env)).await;
    require_script_spawner(false);

    let message = result.expect_err("refused").to_string();
    assert!(message.contains("fail closed"), "{message}");
}

#[cfg(unix)]
#[tokio::test]
async fn without_a_spawner_scripts_still_run_directly() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    clear_script_spawner();
    require_script_spawner(false);
    let env = BTreeMap::new();
    let input = json!({"x": 2});
    let output = run_script(ScriptRequest::plain(
        ScriptLanguage::Shell,
        "cat",
        &input,
        Duration::from_secs(5),
        &env,
    ))
    .await
    .expect("runs");
    assert_eq!(output.value, input);
}
