use super::*;

struct HeldCapabilities {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.ScheduleDocuments")]
impl HeldCapabilities {
    async fn document(&self, request: Value) -> tinybus::Result<Value> {
        if request["op"] == "capabilities" {
            self.started.notify_one();
            self.release.notified().await;
            return Ok(json!({"value":{"transactions":true,"record_metadata":true}}));
        }
        Err(tinybus::Error::failed("unexpected document operation"))
    }
}

#[tokio::test]
async fn stop_during_preflight_retains_the_admitted_run_identity() {
    use tinyflows_bus::schedule::{
        ManualDelivery, ManualRunRequest, ManualRunState, PrepareManualRun, StartManualRun,
        StopManualRun, StoreConfig, WaitManualRun,
    };

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let (_server, host, _) = connections().await;
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        host.serve_at(
            "/test/HeldDocuments".try_into().unwrap(),
            HeldCapabilities {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        let proxy = host
            .proxy(schedule::BUS_NAME, schedule::OBJECT_PATH, schedule::INTERFACE)
            .unwrap();
        let handle: tinyflows_bus::schedule::ManualRunHandle = proxy
            .call(
                "PrepareManualRun",
                &(PrepareManualRun {
                    scope: "manual-preflight-stop-identity".into(),
                    request_id: "held-preflight".into(),
                },),
            )
            .await
            .unwrap();
        let job: CronJob = serde_json::from_value(json!({
            "id":"preflight-identity-job","expression":"","schedule":{"kind":"every","every_ms":60000},
            "command":"echo held","prompt":null,"name":null,"job_type":"shell",
            "session_target":"isolated","model":null,"agent_id":null,"enabled":true,
            "delivery":{"mode":"none"},"delete_after_run":false,
            "created_at":"2026-01-01T00:00:00Z","next_run":"2026-01-01T00:00:00Z"
        }))
        .unwrap();
        let request = ManualRunRequest {
            callback: callback(),
            config: json!({}),
            storage: serde_json::from_value::<StoreConfig>(json!({
                "backend":{"kind":"documents","callback":{
                    "destination":"test.ScheduleHost","object_path":"/test/HeldDocuments"
                }}
            }))
            .unwrap(),
            job,
            run_id: String::new(),
            delivery: ManualDelivery::NotRequested,
        };
        let start = StartManualRun {
            handle: handle.clone(),
            request: request.clone(),
        };
        proxy
            .call::<ManualRunState>("StartManualRun", &start)
            .await
            .unwrap();
        started.notified().await;
        proxy
            .call::<ManualRunState>("StartManualRun", &start)
            .await
            .unwrap();
        let stopping: ManualRunState = proxy
            .call(
                "StopManualRun",
                &(StopManualRun {
                    handle: handle.clone(),
                },),
            )
            .await
            .unwrap()
            .expect("started handle remains observable");
        let stopping_id = match stopping {
            ManualRunState::Stopping { run_id: Some(id) } => id,
            other => panic!("stop must retain the admitted run id, got {other:?}"),
        };
        release.notify_one();
        let terminal = loop {
            let state: ManualRunState = proxy
                .call::<Option<ManualRunState>>(
                    "WaitManualRun",
                    &(WaitManualRun {
                        handle: handle.clone(),
                    },),
                )
                .await
                .unwrap()
                .unwrap();
            if matches!(state, ManualRunState::Terminal { .. }) {
                break state;
            }
            tokio::task::yield_now().await;
        };
        assert!(matches!(
            terminal,
            ManualRunState::Terminal {
                outcome: tinyflows_bus::schedule::ManualRunOutcome::Stopped { run_id: Some(id) }
            } if id == stopping_id
        ));
    })
    .await
    .expect("held preflight scenario must finish within its failure cap");
}
