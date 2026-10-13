use super::*;

#[test]
fn interfaces_share_one_bus_name_with_distinct_paths() {
    let paths = [
        OBJECT_PATH,
        runs::OBJECT_PATH,
        catalog::OBJECT_PATH,
        schedule::OBJECT_PATH,
    ];
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert!(runs::INTERFACE.starts_with("ai.tinyhumans.tinyflows."));
    assert!(catalog::INTERFACE.starts_with("ai.tinyhumans.tinyflows."));
}

#[test]
fn method_constants_match_advertised_members() {
    assert_eq!(
        runs::METHODS,
        &[
            runs::methods::START_RUN,
            runs::methods::RESUME_RUN,
            runs::methods::CANCEL_RUN,
            runs::methods::POLL_RUN
        ]
    );
}

#[test]
fn run_event_round_trips() {
    let ev = runs::RunEvent {
        seq: 3,
        kind: "step".into(),
        data: serde_json::json!({"a": 1}),
    };
    let back: runs::RunEvent = serde_json::from_str(&serde_json::to_string(&ev).unwrap()).unwrap();
    assert_eq!(ev, back);
}
