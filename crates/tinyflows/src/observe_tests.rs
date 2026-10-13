use super::*;
use crate::graph::GraphEvent;
use crate::graph::ids;
use serde::Deserialize;

fn sample_observations(thread_id: &str) -> Vec<GraphObservation> {
    let node = ids::NodeId::new("fetch");
    let mk = |offset: u64, step: usize, ts_ms: u64, event: GraphEvent| GraphObservation {
        event_id: ids::EventId::new(format!("evt-{offset}")),
        run_id: ids::RunId::new("run-9"),
        root_run_id: ids::RunId::new("run-9"),
        parent_run_id: None,
        thread_id: Some(ids::ThreadId::new(thread_id)),
        graph_id: ids::GraphId::new("workflow"),
        checkpoint_id: None,
        namespace: Vec::new(),
        step,
        offset,
        ts_ms,
        event,
    };
    vec![
        mk(
            0,
            0,
            1_000,
            GraphEvent::RunStarted {
                run_id: ids::RunId::new("run-9"),
            },
        ),
        mk(
            1,
            1,
            1_010,
            GraphEvent::StepStarted {
                step: 1,
                active: vec![node.clone()],
            },
        ),
        mk(
            2,
            1,
            1_020,
            GraphEvent::NodeStarted {
                node: node.clone(),
                step: 1,
            },
        ),
        mk(
            3,
            1,
            1_050,
            GraphEvent::NodeCompleted {
                node: node.clone(),
                step: 1,
            },
        ),
        mk(4, 1, 1_060, GraphEvent::StepCompleted { step: 1 }),
    ]
}

#[test]
fn step_status_str_is_stable() {
    assert_eq!(step_status_str(&StepStatus::Success), "success");
    assert_eq!(step_status_str(&StepStatus::Error), "error");
}

#[test]
fn re_typing_preserves_every_field() {
    let engine = sample_observations("flow:flow-1:uuid-1");
    let converted: Vec<GraphObservation> = to_exporter_observations(&engine);
    assert_eq!(converted.len(), engine.len());
    for (before, after) in engine.iter().zip(converted.iter()) {
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
    }
}

#[test]
fn drifted_exporter_type_drops_observations_instead_of_failing() {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Drifted {
        field_the_engine_never_emits: String,
    }
    let converted: Vec<Drifted> = to_exporter_observations(&sample_observations("t"));
    assert!(converted.is_empty());
}

#[test]
fn exporter_observations_match_fixture_snapshot() {
    let converted: Vec<GraphObservation> =
        to_exporter_observations(&sample_observations("flow:flow-1:uuid-1"));
    let actual = serde_json::to_value(&converted).unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "observe_tests/exporter_observations.snap.json"
    ))
    .unwrap();
    assert_eq!(actual, expected, "exporter observation wire shape changed");
}
