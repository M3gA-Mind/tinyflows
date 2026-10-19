use super::*;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::catalog::{self, CatalogStorage};
use tinyflows_bus::runs::RunControlRequest;
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinystoragedrivers_core::{
    Capabilities, CollectionSpec, DocumentStore, Filter, MemoryStorage, Page, Precondition, Query,
    Scope, Sort, StorageBackend, StorageError, Version, Versioned, WriteOptions,
};

#[test]
fn terminal_payload_uses_the_engine_success_vocabulary() {
    for (status, successful) in [
        ("completed", true),
        ("completed_with_warnings", true),
        ("failed", false),
        ("cancelled", false),
        ("interrupted", false),
        ("future_terminal_status", false),
    ] {
        let payload = finished_payload(status, None, None, None, false);
        assert_eq!(payload.status, status);
        assert_eq!(payload.successful, successful, "status: {status}");
    }
}

#[test]
fn terminal_payload_preserves_engine_output_and_deadline_cause() {
    let output = serde_json::json!({"partial":true});
    let payload = finished_payload("cancelled", None, None, Some(output.clone()), false);
    assert_eq!(payload.output, Some(output));
    assert!(!payload.timed_out);

    let timeout = finished_payload("cancelled", Some("deadline".into()), None, None, true);
    assert_eq!(timeout.output, None);
    assert!(timeout.timed_out);
}

#[test]
fn resumed_node_replaces_its_parked_step_instead_of_retaining_stale_diagnostics() {
    let steps = Mutex::new(vec![FlowRunStep {
        node_id: "approval".into(),
        status: Some("success".into()),
        diagnostics: vec![serde_json::json!({"location":"parked-at-gate"})],
        ..Default::default()
    }]);

    record_step(
        &steps,
        FlowRunStep {
            node_id: "approval".into(),
            status: Some("success".into()),
            diagnostics: Vec::new(),
            output: serde_json::json!([{"json":{"approved":true}}]),
            ..Default::default()
        },
    );

    let steps = steps.into_inner().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].status.as_deref(), Some("success"));
    assert!(steps[0].diagnostics.is_empty());
    assert_eq!(
        steps[0].output,
        serde_json::json!([{"json":{"approved":true}}])
    );
}

#[tokio::test]
async fn oversized_terminal_output_falls_back_to_one_pollable_finished_event() {
    let setup = setup(FaultMode::LimitTerminalDocument(4_096)).await;
    let output = json!({"body":"x".repeat(8_192)});
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let result = writer
        .emit_terminal(finished_payload(
            "completed_with_warnings",
            Some("engine warning".into()),
            None,
            Some(output),
            false,
        ))
        .await;
    match &result {
        Ok(seq) => {
            assert_eq!(*seq, 1);
            assert_eq!(writer.flush().await, None);
            assert!(
                setup
                    .catalog
                    .mark_flow_run_terminal_event(&setup.run_id, *seq)
                    .await
                    .unwrap()
            );
        }
        Err(_) => {
            assert!(writer.flush().await.is_some());
        }
    }
    let page = poll(&setup).await;
    assert!(
        page.done,
        "terminal run stopped polling after the output-bearing event hit the size limit: {result:?}"
    );
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.status, "completed_with_warnings");
    assert!(finished.successful);
    assert_eq!(finished.error.as_deref(), Some("engine warning"));
    assert_eq!(finished.output, None);
    assert!(!finished.timed_out);
}

#[tokio::test]
async fn terminal_event_lost_ack_retries_the_exact_payload_without_duplication() {
    let setup = setup(FaultMode::CommitThenLoseAck).await;
    let output = json!({"kept":true});
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let seq = writer
        .emit_terminal(finished_payload(
            "completed",
            None,
            None,
            Some(output.clone()),
            false,
        ))
        .await
        .expect("retry should recognize the identical event already committed");

    assert_eq!(seq, 1);
    assert_eq!(writer.flush().await, None);
    assert!(
        setup
            .catalog
            .mark_flow_run_terminal_event(&setup.run_id, seq)
            .await
            .unwrap()
    );
    let page = poll(&setup).await;
    assert!(page.done);
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.output, Some(output));
}

#[tokio::test]
async fn terminal_event_stops_cleanly_when_output_and_fallback_writes_fail() {
    let setup = setup(FaultMode::RejectEveryTerminalWrite).await;
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let error = writer
        .emit_terminal(finished_payload(
            "failed",
            Some("engine failed".into()),
            None,
            Some(json!({"body":"x".repeat(8_192)})),
            false,
        ))
        .await
        .expect_err("backend failures must remain visible to the actor");

    assert!(
        error
            .to_string()
            .contains("injected terminal write failure")
    );
    assert!(writer.flush().await.is_some());
    let page = poll(&setup).await;
    assert!(!page.done);
    assert!(page.events.is_empty());
}

struct TestSetup {
    catalog: FlowCatalogDocuments,
    run_id: String,
    module: Connection,
    storage: CatalogStorage,
}

async fn setup(mode: FaultMode) -> TestSetup {
    let scoped = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let docs = Arc::new(FaultingDocuments {
        inner: scoped.documents().clone(),
        mode,
        output_attempts: AtomicUsize::new(0),
        lose_ack_once: AtomicBool::new(true),
    });
    let catalog = FlowCatalogDocuments::new(docs.clone());
    let graph = tinyflows::migrate::deserialize_graph(json!({
        "nodes":[{"id":"trigger","name":"Trigger","kind":"trigger","config":{}}],
        "edges":[]
    }))
    .unwrap();
    let flow = catalog
        .create_flow("terminal event test".into(), graph, false, true)
        .await
        .unwrap();
    let run_id = format!("terminal-{}", uuid::Uuid::new_v4());
    catalog
        .insert_flow_run(&run_id, &flow.id, &run_id, "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    catalog
        .finish_flow_run(
            &run_id,
            "completed",
            "2026-01-01T00:00:01Z",
            &[],
            &[],
            None,
            None,
        )
        .await
        .unwrap();

    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.serve_at("/terminal_docs".parse().unwrap(), StorageReader(docs))
        .await
        .unwrap();
    host.request_name("test.TerminalCatalogStorage")
        .await
        .unwrap();
    TestSetup {
        catalog,
        run_id,
        module,
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.TerminalCatalogStorage".into(),
                object_path: "/terminal_docs".into(),
            },
        },
    }
}

async fn poll(setup: &TestSetup) -> tinyflows_bus::runs::PollRun {
    super::super::poll(
        &setup.module,
        RunControlRequest {
            storage: setup.storage.clone(),
            run_id: setup.run_id.clone(),
            cursor: 0,
        },
    )
    .await
    .unwrap()
}

#[derive(Clone, Copy)]
enum FaultMode {
    LimitTerminalDocument(usize),
    CommitThenLoseAck,
    RejectEveryTerminalWrite,
}

struct FaultingDocuments {
    inner: Arc<dyn DocumentStore>,
    mode: FaultMode,
    output_attempts: AtomicUsize,
    lose_ack_once: AtomicBool,
}

impl std::fmt::Debug for FaultingDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultingDocuments").finish_non_exhaustive()
    }
}

impl FaultingDocuments {
    fn terminal_output_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events.last().is_some_and(|event| {
                    event["kind"] == "finished"
                        && event["data"]
                            .get("output")
                            .is_some_and(|value| !value.is_null())
                })
            })
    }

    fn terminal_event(collection: &str, doc: &Value) -> bool {
        collection == "flows.runs"
            && doc["events"].as_array().is_some_and(|events| {
                events
                    .last()
                    .is_some_and(|event| event["kind"] == "finished")
            })
    }
}

#[async_trait]
impl DocumentStore for FaultingDocuments {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(
        &self,
        spec: &CollectionSpec,
    ) -> tinystoragedrivers_core::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_core::Result<Option<Versioned<Value>>> {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> tinystoragedrivers_core::Result<Version> {
        self.put_with(collection, id, doc, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> tinystoragedrivers_core::Result<Version> {
        if Self::terminal_event(collection, &doc) {
            match self.mode {
                FaultMode::LimitTerminalDocument(max_bytes)
                    if Self::terminal_output_event(collection, &doc)
                        && serde_json::to_vec(&doc).unwrap().len() > max_bytes =>
                {
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::backend(
                        "injected terminal document size limit",
                    ));
                }
                FaultMode::CommitThenLoseAck
                    if Self::terminal_output_event(collection, &doc)
                        && self.lose_ack_once.swap(false, Ordering::SeqCst) =>
                {
                    let _version = self
                        .inner
                        .put_with(collection, id, doc, precondition, options)
                        .await?;
                    self.output_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(StorageError::unavailable(
                        "injected lost acknowledgement after terminal commit",
                    ));
                }
                FaultMode::RejectEveryTerminalWrite => {
                    return Err(StorageError::unavailable("injected terminal write failure"));
                }
                _ => {}
            }
        }
        self.inner
            .put_with(collection, id, doc, precondition, options)
            .await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> tinystoragedrivers_core::Result<Page<Versioned<Value>>> {
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> tinystoragedrivers_core::Result<Option<Versioned<Value>>> {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_core::Result<()> {
        self.inner.drop_collection(collection).await
    }
}

struct StorageReader(Arc<FaultingDocuments>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl StorageReader {
    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        let result = self
            .0
            .get(
                request["collection"].as_str().unwrap(),
                request["id"].as_str().unwrap(),
            )
            .await;
        Ok(match result {
            Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
            Err(error) => json!({"error":{
                "kind":error.kind().to_string().replace(' ', "_"),
                "message":error.to_string()
            }}),
        })
    }
}
