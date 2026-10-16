//! The stored form of the ledger's and the vault's records (storage schema v2).
//!
//! The record types are public API (RFC 3339 strings, a camelCase step); the
//! stored form is separate: a bare snake_case object per record, its instants
//! one epoch-millisecond `*_ms` field, no JSON encoded in a string, and the
//! tenant bucket (`scope_key`, `""` for global) beside the record's own fields.
//!
//! Every function here takes and returns plain JSON, so the live code and the
//! v1 -> v2 migration steps share one definition of the stored shape.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

/// `raw` (RFC 3339) as epoch milliseconds; `0` when it does not parse, so a
/// malformed timestamp sorts first instead of failing the write.
pub(super) fn instant_ms(raw: &str) -> i64 {
    DateTime::parse_from_rfc3339(raw).map_or(0, |at| at.with_timezone(&Utc).timestamp_millis())
}

/// Epoch milliseconds as the RFC 3339 text the records carry.
pub(super) fn rfc3339(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// A record as the JSON object it serializes to.
pub(super) fn object(record: &impl Serialize) -> Result<Map<String, Value>, String> {
    match serde_json::to_value(record).map_err(|error| error.to_string())? {
        Value::Object(map) => Ok(map),
        _ => Err("record is not a JSON object".to_owned()),
    }
}

/// `field` renamed from an RFC 3339 string to an `_ms` count.
fn instant_to_ms(map: &mut Map<String, Value>, field: &str, stored: &str) {
    if let Some(raw) = map.remove(field) {
        let ms = raw.as_str().map_or(0, instant_ms);
        map.insert(stored.to_owned(), json!(ms));
    }
}

/// `stored` renamed back to an RFC 3339 `field`.
fn ms_to_instant(map: &mut Map<String, Value>, stored: &str, field: &str) {
    let ms = map.remove(stored).and_then(|v| v.as_i64()).unwrap_or(0);
    map.insert(field.to_owned(), json!(rfc3339(ms)));
}

fn rename(map: &mut Map<String, Value>, from: &str, to: &str) {
    if let Some(value) = map.remove(from) {
        map.insert(to.to_owned(), value);
    }
}

fn finish(map: Map<String, Value>) -> Value {
    Value::Object(map)
}

pub(super) fn from_object<T: DeserializeOwned>(map: Map<String, Value>) -> Result<T, String> {
    serde_json::from_value(Value::Object(map)).map_err(|error| error.to_string())
}

fn as_map(doc: &Value) -> Result<Map<String, Value>, String> {
    doc.as_object()
        .cloned()
        .ok_or_else(|| "document is not an object".to_owned())
}

// ---- rows ----

/// A ledger row (its serialized form) as the `adaptive_rows` body.
pub(super) fn row_body(mut row: Map<String, Value>, scope: &str, seq: u64) -> Value {
    row.remove("id");
    instant_to_ms(&mut row, "at", "at_ms");
    row.insert("scope_key".into(), json!(scope));
    row.insert("seq".into(), json!(seq));
    finish(row)
}

/// The row a body holds (its id is the document's).
pub(super) fn row_from(doc: &Value) -> Result<Map<String, Value>, String> {
    let mut row = as_map(doc)?;
    row.remove("scope_key");
    row.remove("seq");
    ms_to_instant(&mut row, "at_ms", "at");
    Ok(row)
}

// ---- lessons ----

/// A lesson (its serialized form) as the `adaptive_lessons` body; the counters
/// are the lesson's own `applied` and `helped`.
pub(super) fn lesson_body(mut lesson: Map<String, Value>, scope: &str, seq: u64) -> Value {
    lesson.remove("id");
    lesson.insert("scope_key".into(), json!(scope));
    lesson.insert("seq".into(), json!(seq));
    finish(lesson)
}

pub(super) fn lesson_from(doc: &Value) -> Result<Map<String, Value>, String> {
    let mut lesson = as_map(doc)?;
    lesson.remove("seq");
    lesson.remove("scope_key");
    Ok(lesson)
}

// ---- episodes ----

pub(super) fn episode_body(mut episode: Map<String, Value>, scope: &str) -> Value {
    rename(&mut episode, "id", "episode_id");
    instant_to_ms(&mut episode, "started_at", "started_ms");
    instant_to_ms(&mut episode, "updated_at", "updated_ms");
    let running = episode
        .get("status")
        .and_then(|status| status.get("state"))
        .and_then(Value::as_str)
        == Some("running");
    episode.insert("running".into(), json!(running));
    episode.insert("scope_key".into(), json!(scope));
    finish(episode)
}

pub(super) fn episode_from(doc: &Value) -> Result<Map<String, Value>, String> {
    let mut episode = as_map(doc)?;
    episode.remove("running");
    rename(&mut episode, "episode_id", "id");
    ms_to_instant(&mut episode, "started_ms", "started_at");
    ms_to_instant(&mut episode, "updated_ms", "updated_at");
    let scope = episode
        .remove("scope_key")
        .and_then(|v| v.as_str().map(str::to_owned))
        .filter(|scope| !scope.is_empty());
    episode.insert("scope_key".into(), json!(scope));
    Ok(episode)
}

// ---- steps ----

/// A step (its camelCase wire form) as the `adaptive_steps` body. `output` is
/// the node's own data and is kept verbatim.
pub(super) fn step_body(mut step: Map<String, Value>, scope: &str, row_id: &str, seq: u64) -> Value {
    rename(&mut step, "nodeId", "node_id");
    rename(&mut step, "durationMs", "duration_ms");
    rename(&mut step, "nullBindings", "null_bindings");
    if let Some(Value::Array(entries)) = step.get_mut("transcript") {
        for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
            rename(entry, "atMs", "at_ms");
        }
    }
    step.insert("scope_key".into(), json!(scope));
    step.insert("row_id".into(), json!(row_id));
    step.insert("seq".into(), json!(seq));
    finish(step)
}

pub(super) fn step_from(doc: &Value) -> Result<Map<String, Value>, String> {
    let mut step = as_map(doc)?;
    for stored in ["scope_key", "row_id", "seq"] {
        step.remove(stored);
    }
    rename(&mut step, "node_id", "nodeId");
    rename(&mut step, "duration_ms", "durationMs");
    rename(&mut step, "null_bindings", "nullBindings");
    if let Some(Value::Array(entries)) = step.get_mut("transcript") {
        for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
            rename(entry, "at_ms", "atMs");
        }
    }
    Ok(step)
}

// ---- vault ----

/// A workflow record (its serialized form) as the `adaptive_workflows` body.
pub(super) fn workflow_body(mut record: Map<String, Value>, scope: &str) -> Value {
    if let Some(id) = record.remove("id") {
        record.insert("workflow_id".into(), id);
    }
    record.remove("source_path");
    record.insert("scope_key".into(), json!(scope));
    finish(record)
}

pub(super) fn workflow_from(doc: &Value) -> Result<Map<String, Value>, String> {
    let mut record = as_map(doc)?;
    record.remove("scope_key");
    rename(&mut record, "workflow_id", "id");
    Ok(record)
}
