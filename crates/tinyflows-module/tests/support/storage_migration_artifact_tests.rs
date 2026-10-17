use serde_json::{Value, json};
use std::sync::{Arc, atomic::AtomicBool};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinyflows_drivers::schedule::CronDocuments;
use tinystoragedrivers_core::{DocumentStore, DocumentStoreExt, MemoryStorage, Precondition, Scope, StorageBackend};

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn published_module_migrates_legacy_schedule_records_through_owned_bus_call() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();

    let scoped = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let docs = scoped.documents().clone();
    docs.ensure_collection(&tinystoragedrivers_core::CollectionSpec::new("cron_jobs"))
        .await
        .unwrap();
    docs.put(
        "cron_jobs",
        "legacy-job",
        json!({
            "claim_generation": 1,
            "expression": "0 9 * * *",
            "command": "echo migrated",
            "schedule": "{\"kind\":\"cron\",\"expr\":\"0 9 * * *\",\"tz\":null,\"active_hours\":null}",
            "job_type": "shell",
            "session_target": "isolated",
            "enabled": true,
            "delivery": "{\"mode\":\"none\",\"channel\":null,\"to\":null,\"best_effort\":true}",
            "delete_after_run": false,
            "created_at": "2026-03-04T05:06:07+00:00",
            "next_run": "2026-03-05T09:00:00+00:00"
        }),
        Precondition::None,
    )
    .await
    .unwrap();

    connection
        .serve_at(
            "/test/ScheduleDocuments".try_into().unwrap(),
            super::document_callbacks::Documents {
                docs: docs.clone(),
                conflict_once: Arc::new(AtomicBool::new(false)),
                atomic_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            },
        )
        .await
        .unwrap();
    connection.request_name("test.ScheduleDocuments").await.unwrap();
    let schedule = connection
        .proxy(
            tinyflows_bus::schedule::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "storage": {
            "backend": {"kind":"documents", "callback": {
                "destination":"test.ScheduleDocuments",
                "object_path":"/test/ScheduleDocuments"
            }}
        },
        "operation": {"op":"migrate"}
    });
    let _: Value = schedule.call("ScheduleStore", &(request,)).await.unwrap();

    let jobs = CronDocuments::new(docs).list_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1, "the legacy job is visible after migration");
    assert_eq!(jobs[0].id, "legacy-job");
}
