//! The stored form of the workflow store's records (storage schema v2).
//!
//! The model types (`RunRecord`, `WorkflowNote`, `WorkflowProposal`) are also
//! the engine's wire types: camelCase, with `*_at` instants. The stored form is
//! separate and the wire form does not change: bare snake_case objects, every
//! instant one epoch-millisecond `*_ms` field, and no JSON encoded in a string.
//!
//! The conversion works on the serialized value rather than on mirror structs,
//! so a field added to a model type is stored without touching this file. Only
//! two things are named here: the instants that change their name, and the
//! keys whose values are free-form (a graph's node config, a run's inputs, a
//! step's input and output, a proposal's ops), which are carried verbatim
//! because their keys belong to the user, not to the schema.

use serde_json::{Map, Value, json};
use tinyflows::store::types::{
    RunRecord, WorkflowError, WorkflowNote, WorkflowProposal, WorkflowRecord,
};

use super::malformed;

/// Keys whose value is user data: never renamed, never entered.
const FREE_FORM: &[&str] = &["graph", "inputs", "trigger", "input", "output", "ops"];

/// Wire instants and the `*_ms` field each is stored as. The remaining wire
/// keys are renamed mechanically (`nodeId` <-> `node_id`).
const INSTANTS: &[(&str, &str)] = &[
    ("startedAt", "started_ms"),
    ("finishedAt", "finished_ms"),
    ("recordedAt", "recorded_ms"),
    ("verifiedAt", "verified_ms"),
    ("decidedAt", "decided_ms"),
    ("createdAt", "created_ms"),
    ("supersededAt", "superseded_ms"),
];

/// A run executor's process start: seconds on the wire, milliseconds stored.
const PROCESS_START: (&str, &str) = ("startedAtSecs", "process_started_ms");

fn snake(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 2);
    for c in key.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn camel(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut upper = false;
    for c in key.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.push(c.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn secs_to_ms(value: Value) -> Value {
    match value.as_u64() {
        Some(secs) => json!(secs.saturating_mul(1000)),
        None => value,
    }
}

fn ms_to_secs(value: Value) -> Value {
    match value.as_u64() {
        Some(ms) => json!(ms / 1000),
        None => value,
    }
}

/// A wire value as it is stored.
pub(crate) fn to_stored(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, inner)| {
                    if FREE_FORM.contains(&key.as_str()) {
                        return (key, inner);
                    }
                    if key == PROCESS_START.0 {
                        return (PROCESS_START.1.to_owned(), secs_to_ms(inner));
                    }
                    let renamed = INSTANTS
                        .iter()
                        .find(|(wire, _)| *wire == key)
                        .map_or_else(|| snake(&key), |(_, stored)| (*stored).to_owned());
                    (renamed, to_stored(inner))
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(to_stored).collect()),
        other => other,
    }
}

/// A stored value as the model types read it.
pub(crate) fn to_wire(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, inner)| {
                    if FREE_FORM.contains(&key.as_str()) {
                        return (key, inner);
                    }
                    if key == PROCESS_START.1 {
                        return (PROCESS_START.0.to_owned(), ms_to_secs(inner));
                    }
                    let renamed = INSTANTS
                        .iter()
                        .find(|(_, stored)| *stored == key)
                        .map_or_else(|| camel(&key), |(wire, _)| (*wire).to_owned());
                    (renamed, to_wire(inner))
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(to_wire).collect()),
        other => other,
    }
}

fn object(value: Value, what: &str) -> Result<Map<String, Value>, WorkflowError> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(malformed(what, "is not a JSON object")),
    }
}

fn serialize<T: serde::Serialize>(record: &T, what: &str) -> Result<Map<String, Value>, WorkflowError> {
    let value = serde_json::to_value(record).map_err(|error| malformed(what, error))?;
    object(value, what)
}

/// A saved workflow as its document keeps it: the record's own fields, without
/// the path it was read from (where a record was read from is not part of what
/// it is).
pub(super) fn definition_body(record: &WorkflowRecord) -> Result<Value, WorkflowError> {
    let mut body = serialize(record, "serializing workflow")?;
    body.remove("source_path");
    Ok(to_stored(Value::Object(body)))
}

/// The record a definition (or revision) body holds, its graph migrated to the
/// current schema first so a definition saved by an older build keeps loading.
pub(super) fn definition_from_body(body: Value) -> Result<WorkflowRecord, String> {
    let mut value = to_wire(body);
    if let Some(graph) = value.get_mut("graph") {
        let migrated =
            tinyflows::migrate::migrate(graph.take()).map_err(|error| error.to_string())?;
        *graph = migrated;
    }
    serde_json::from_value(value).map_err(|error| format!("invalid workflow: {error}"))
}

/// A superseded copy: the workflow's fields plus which workflow and when.
pub(super) fn revision_body(
    record: &WorkflowRecord,
    superseded_ms: i64,
) -> Result<Value, WorkflowError> {
    let mut body = serialize(record, "serializing revision")?;
    body.remove("source_path");
    body.remove("id");
    let mut body = object(to_stored(Value::Object(body)), "serializing revision")?;
    body.insert("workflow_id".into(), json!(record.id));
    body.insert("superseded_ms".into(), json!(superseded_ms));
    Ok(Value::Object(body))
}

/// The workflow a revision body holds.
pub(super) fn revision_record(body: &Value) -> Result<WorkflowRecord, String> {
    let mut fields = body.as_object().cloned().ok_or("revision is not an object")?;
    fields.remove("superseded_ms");
    let id = fields
        .remove("workflow_id")
        .ok_or("revision has no `workflow_id`")?;
    fields.insert("id".into(), id);
    definition_from_body(Value::Object(fields))
}

/// A run as its document keeps it, with `settled` beside it for the index.
pub(super) fn run_body(run: &RunRecord) -> Result<Value, WorkflowError> {
    let mut body = object(
        to_stored(Value::Object(serialize(run, "serializing run")?)),
        "serializing run",
    )?;
    body.insert("settled".into(), json!(run.status.is_settled()));
    Ok(Value::Object(body))
}

pub(super) fn run_from_body(body: &Value) -> Result<RunRecord, String> {
    serde_json::from_value(to_wire(body.clone())).map_err(|error| error.to_string())
}

/// A workflow's whole journal.
pub(super) fn journal_body(workflow_id: &str, notes: &[WorkflowNote]) -> Value {
    json!({
        "workflow_id": workflow_id,
        "notes": notes.iter().map(|note| to_stored(json!(note))).collect::<Vec<_>>(),
    })
}

pub(super) fn journal_from_body(body: &Value) -> Result<Vec<WorkflowNote>, String> {
    let notes = body
        .get("notes")
        .and_then(Value::as_array)
        .ok_or("journal has no `notes` list")?;
    notes
        .iter()
        .map(|note| serde_json::from_value(to_wire(note.clone())).map_err(|e| e.to_string()))
        .collect()
}

pub(super) fn proposal_body(proposal: &WorkflowProposal) -> Result<Value, WorkflowError> {
    Ok(to_stored(Value::Object(serialize(
        proposal,
        "serializing proposal",
    )?)))
}

pub(super) fn proposal_from_body(body: &Value) -> Result<WorkflowProposal, String> {
    serde_json::from_value(to_wire(body.clone())).map_err(|error| error.to_string())
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
