use super::*;
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(&value).unwrap();
    let decoded: T = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn import_batch_contract_is_bounded_and_round_trips_every_source_record() {
    assert_eq!(MAX_IMPORT_BATCH_RECORDS, 64);
    assert_eq!(MAX_IMPORT_BATCH_BYTES, 2 * 1024 * 1024);
    assert_eq!(CONTRACT_VERSION, 6);
    assert!(METHODS.contains(&members::IMPORT_LEGACY_BATCH));
    let storage = CatalogStorage {
        callback: CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: "/test/agent/alice/CatalogStorage".into(),
        },
    };
    for record in [
        LegacyImportRecord::Flow(LegacyFlowRecord {
            id: "flow-1".into(),
            name: "Imported".into(),
            graph_json: "{\"nodes\":[],\"edges\":[]}".into(),
            enabled: true,
            created_at: "2025-01-02T03:04:05Z".into(),
            updated_at: "2025-01-03T03:04:05Z".into(),
            last_run_at: None,
            last_status: None,
            require_approval: false,
        }),
        LegacyImportRecord::Revision(LegacyRevisionRecord {
            id: "revision-1".into(),
            flow_id: "flow-1".into(),
            graph_json: "{}".into(),
            name: "Old".into(),
            require_approval: false,
            created_at: "2025-01-01T00:00:00Z".into(),
        }),
        LegacyImportRecord::Run(LegacyRunRecord {
            id: "run-1".into(),
            flow_id: "flow-1".into(),
            thread_id: "run-1".into(),
            status: "pending_approval".into(),
            started_at: "2025-01-02T03:04:05Z".into(),
            finished_at: None,
            steps_json: "[]".into(),
            pending_approvals_json: "[]".into(),
            error: None,
            graph_hash: Some("hash".into()),
        }),
        LegacyImportRecord::State(LegacyStateRecord {
            namespace: "flow:flow-1".into(),
            key: "seen".into(),
            value_json: "[1,2]".into(),
        }),
        LegacyImportRecord::Suggestion(LegacySuggestionRecord {
            id: "suggestion-1".into(),
            title: "Suggested".into(),
            one_liner: "One line".into(),
            rationale: "Useful".into(),
            trigger_hint: None,
            steps_json: "[]".into(),
            connections_json: "[]".into(),
            slugs_json: "[]".into(),
            build_prompt: "Build".into(),
            confidence: 0.7,
            status: "dismissed".into(),
            created_at: "2025-01-02T03:04:05Z".into(),
            source_run_id: None,
        }),
        LegacyImportRecord::Draft(LegacyDraftRecord {
            file_name: "draft-1.json".into(),
            bytes: br#"{"id":"draft-1"}"#.to_vec(),
        }),
        LegacyImportRecord::Checkpoint {
            raw_json: "{\"thread_id\":\"run-1\"}".into(),
        },
        LegacyImportRecord::SqliteCheckpointWrite(LegacySqliteCheckpointWrite {
            thread_id: "run-1".into(),
            namespace_json: "[]".into(),
            checkpoint_id: "checkpoint-1".into(),
            task_id: "task-1".into(),
            idx: 0,
            node: "node-1".into(),
            channel: "output".into(),
            payload_json: "null".into(),
        }),
        LegacyImportRecord::FileCheckpointWrite {
            thread_id: "run-1".into(),
            raw_json: "{\"checkpoint_id\":\"checkpoint-1\"}".into(),
        },
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Draft,
            identity: Some("draft-too-large.json".into()),
            message: "source record exceeded the batch byte limit".into(),
        },
    ] {
        round_trip(record);
    }

    round_trip(ImportLegacyBatchRequest {
        storage,
        records: Vec::new(),
    });
    round_trip(ImportLegacyBatchResponse {
        tallies: vec![LegacyImportTally {
            kind: LegacyImportKind::Flow,
            written: 1,
            existing: 0,
            invalid: 0,
        }],
        problems: vec![LegacyImportProblem {
            kind: LegacyImportKind::Flow,
            identity: Some("flow-1".into()),
            message: "graph is not JSON".into(),
        }],
    });
    assert_eq!(
        LegacyImportKind::Flow,
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Flow,
            identity: None,
            message: "invalid".into(),
        }
        .record_kind()
    );
}
