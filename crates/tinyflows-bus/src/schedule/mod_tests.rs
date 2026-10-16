use super::*;
use serde_json::json;

fn every() -> Schedule {
    Schedule::Every { every_ms: 60_000 }
}

#[test]
fn contract_is_version_three_with_the_exact_member_set() {
    assert_eq!(CONTRACT_VERSION, 3);
    assert_eq!(METHODS.len(), 17);
    let mut unique: Vec<_> = METHODS.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), METHODS.len());
    for name in [
        "ScheduleStore",
        "ParseHumanDelay",
        "ValidateDelivery",
        "NextRun",
        "ValidateSchedule",
        "RunsCloserThan",
        "NormalizeExpression",
        "ScheduleCronExpression",
        "DefaultDelivery",
        "CheckOriginRequirements",
        "CheckPatch",
        "TruncateCronOutput",
    ] {
        assert!(METHODS.contains(&name), "{name}");
    }
}

#[test]
fn every_advertised_store_operation_round_trips_through_the_typed_form() {
    let job = json!({"id":"j1","expression":"","schedule":{"kind":"every","every_ms":60000},"command":"c","prompt":null,"name":null,"job_type":"shell","session_target":"isolated","model":null,"agent_id":null,"enabled":true,"delivery":{"mode":"none"},"delete_after_run":false,"created_at":"2026-01-01T00:00:00Z","next_run":"2026-01-01T00:00:00Z","last_run":null,"last_status":null,"last_output":null});
    let run = json!({"id":1,"job_id":"j1","started_at":"2026-01-01T00:00:00Z","finished_at":"2026-01-01T00:00:01Z","status":"ok","output":null,"duration_ms":1000});
    let spec = json!({"name":null,"schedule":{"kind":"every","every_ms":60000},"prompt":"p","session_target":"isolated","model":null,"delivery":null,"delete_after_run":false,"agent_id":null,"enabled":true,"origin":null});
    let operations = [
        json!({"op":"add","expression":"* * * * *","command":"c"}),
        json!({"op":"add_shell","name":null,"schedule":{"kind":"every","every_ms":60000},"command":"c"}),
        json!({"op":"add_agent","spec":spec}),
        json!({"op":"add_flow","flow_id":"f","schedule":{"kind":"every","every_ms":60000}}),
        json!({"op":"find_flow","flow_id":"f"}),
        json!({"op":"list"}),
        json!({"op":"get","job_id":"j"}),
        json!({"op":"remove","job_id":"j"}),
        json!({"op":"clear"}),
        json!({"op":"dedup"}),
        json!({"op":"due","now":"2026-01-01T00:00:00Z"}),
        json!({"op":"update","job_id":"j","patch":{"enabled":false}}),
        json!({"op":"record_last_run","job_id":"j","finished_at":"2026-01-01T00:00:00Z","success":true,"output":"o"}),
        json!({"op":"reschedule","job":job,"success":true,"output":"o"}),
        json!({"op":"record_run","job_id":"j","started_at":"2026-01-01T00:00:00Z","finished_at":"2026-01-01T00:00:01Z","status":"ok","output":null,"duration_ms":1000,"delivery_status":null}),
        json!({"op":"delete_queued_runs","job_id":"j"}),
        json!({"op":"list_runs","job_id":"j","limit":5}),
        json!({"op":"set_max_active_jobs","limit":3}),
        json!({"op":"import_job","job":job}),
        json!({"op":"import_run","run":run}),
    ];
    assert_eq!(operations.len(), STORE_OPERATIONS.len());
    for (operation, name) in operations.iter().zip(STORE_OPERATIONS) {
        assert_eq!(operation["op"], *name);
        let typed: StoreOperation = serde_json::from_value(operation.clone()).unwrap();
        assert_eq!(serde_json::to_value(&typed).unwrap()["op"], *name);
    }
    let cleared: StoreOperation =
        serde_json::from_value(json!({"op":"set_max_active_jobs","limit":null})).unwrap();
    assert!(matches!(
        cleared,
        StoreOperation::SetMaxActiveJobs { limit: None }
    ));
}

#[test]
fn too_frequent_wire_shape_and_display() {
    let fixed = TooFrequent::FixedInterval(chrono::Duration::seconds(90));
    assert_eq!(serde_json::to_string(&fixed).unwrap(), r#"{"fixed_interval":90000}"#);
    assert_eq!(fixed.to_string(), "fires every 1 minute 30 seconds");
    let pair: TooFrequent = serde_json::from_str(
        r#"{"consecutive_runs":{"first":"2026-01-01T00:56:00Z","second":"2026-01-01T01:00:00Z"}}"#,
    )
    .unwrap();
    assert_eq!(pair.gap(), chrono::Duration::minutes(4));
    assert!(pair.to_string().ends_with("4 minutes apart"));
    assert_eq!(serde_json::from_str::<TooFrequent>(&serde_json::to_string(&fixed).unwrap()).unwrap(), fixed);
    assert_eq!(MIN_AGENT_JOB_INTERVAL, chrono::Duration::minutes(5));
}

#[test]
fn callback_records_keep_their_json_shape() {
    let attempt: Attempt =
        serde_json::from_str(r#"{"success":true,"output":"o","retryable":false}"#).unwrap();
    assert_eq!(
        serde_json::to_string(&attempt).unwrap(),
        r#"{"success":true,"output":"o","retryable":false}"#
    );
    let record_json = r#"{"run_id":"r","job_id":"j","job_created_at":"2026-01-01T00:00:00Z","started_at":"2026-01-01T00:00:01Z","finished_at":"2026-01-01T00:00:02Z","status":"ok","output":"o","delivery_status":"delivered"}"#;
    let record: RunRecord = serde_json::from_str(record_json).unwrap();
    assert_eq!(serde_json::to_string(&record).unwrap(), record_json);
    let completion_json = r#"{"remove":false,"disable":true,"next_run":null,"finished_at":"2026-01-01T00:00:02Z","success":true,"output":"o"}"#;
    let completion: Completion = serde_json::from_str(completion_json).unwrap();
    assert_eq!(serde_json::to_string(&completion).unwrap(), completion_json);
}

#[test]
fn agent_job_spec_defaults_and_request_types_deserialize() {
    let spec = AgentJobSpec::new(every(), "hi");
    assert!(spec.enabled && spec.delivery.is_none() && spec.origin.is_none());
    let back: AgentJobSpec = serde_json::from_value(serde_json::to_value(&spec).unwrap()).unwrap();
    assert_eq!(back.prompt, "hi");

    let validate: ValidateScheduleRequest = serde_json::from_value(
        json!({"schedule":{"kind":"every","every_ms":1000},"now":"2026-01-01T00:00:00Z"}),
    )
    .unwrap();
    assert!(!validate.agent);
    let origin: CheckOriginRequest = serde_json::from_value(
        json!({"session_target":"current","delivery":{"mode":"origin"}}),
    )
    .unwrap();
    assert!(origin.origin.is_none());
}
