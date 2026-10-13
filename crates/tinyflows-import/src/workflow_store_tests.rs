use super::*;
use crate::test_support::{docs, docs_in, expected, fixtures, json, tree_digest};
use serde_json::{Value, json};
use tinyflows::store::WorkflowStore;
use tinystoragedrivers_core::MemoryStorage;

fn base() -> PathBuf {
    fixtures().join("workflow-store")
}

/// The layout the committed store was written with. The revisions directory is
/// named by a hash of the path it was generated at, so it is found, not derived.
fn layout() -> FileStoreLayout {
    let hashed = std::fs::read_dir(base().join("state/definitions"))
        .unwrap()
        .next()
        .expect("one definition state directory")
        .unwrap()
        .path();
    FileStoreLayout {
        definitions: base().join("workflows"),
        runs: base().join("state/runs"),
        journal: base().join("state/journal"),
        proposals: base().join("state/proposals"),
        revisions: vec![
            hashed.join("revisions"),
            base().join("workflows/.revisions"),
        ],
    }
}

fn target() -> DriverWorkflowStore {
    DriverWorkflowStore::new(docs())
}

/// `value` without the path a record was read from, which no store keeps.
fn portable(mut value: Value) -> Value {
    fn strip(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("source_path");
                map.values_mut().for_each(strip);
            }
            Value::Array(items) => items.iter_mut().for_each(strip),
            _ => {}
        }
    }
    strip(&mut value);
    value
}

#[tokio::test]
async fn the_whole_store_comes_across_with_its_ids_and_every_field() {
    let store = target();
    let report = import_workflow_store(&layout(), &store).await.unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    let expected = expected();
    let expected = &expected["workflow_store"];

    // Definitions: the legacy store's `load()`, id for id.
    for legacy in expected["workflows"].as_array().unwrap() {
        let id = legacy["id"].as_str().unwrap();
        let imported = store
            .get(id)
            .unwrap()
            .unwrap_or_else(|| panic!("{id} imported"));
        assert_eq!(
            portable(json(&imported)),
            portable(legacy.clone()),
            "workflow {id}"
        );
    }
    assert_eq!(store.list().unwrap().len(), 2);
    // Revisions, including the one a delete left behind.
    for (id, legacy) in expected["revisions"].as_object().unwrap() {
        let imported = store.list_revisions(id).unwrap();
        assert_eq!(
            portable(Value::Array(imported.iter().map(json).collect())),
            portable(legacy.clone()),
            "revisions of {id}"
        );
    }
    assert!(store.get("retired").unwrap().is_none());
    assert_eq!(store.list_revisions("retired").unwrap().len(), 1);
    // Runs.
    for (id, legacy) in expected["runs"].as_object().unwrap() {
        let imported = store.list_runs(id).unwrap();
        assert_eq!(
            Value::Array(imported.iter().map(json).collect()),
            *legacy,
            "runs of {id}"
        );
    }
    assert_eq!(store.unsettled_runs().unwrap().len(), 1);
    // Notes and proposals.
    let notes = store.list_notes("sweep").unwrap();
    assert_eq!(
        Value::Array(notes.iter().map(json).collect()),
        expected["notes"]
    );
    let proposals = store.list_proposals("sweep").unwrap();
    assert_eq!(
        Value::Array(proposals.iter().map(json).collect()),
        expected["proposals"]
    );
}

#[tokio::test]
async fn the_report_counts_every_kind() {
    let report = import_workflow_store(&layout(), &target()).await.unwrap();
    for (kind, written) in [
        ("workflows", 2),
        ("revisions", 3),
        ("runs", 3),
        ("notes", 2),
        ("proposals", 1),
    ] {
        assert_eq!(report.tally(kind).written, written, "{kind}");
    }
}

#[tokio::test]
async fn importing_again_writes_nothing() {
    let store = target();
    let first = import_workflow_store(&layout(), &store).await.unwrap();
    let second = import_workflow_store(&layout(), &store).await.unwrap();
    assert_eq!(second.total_written(), 0);
    for (kind, tally) in first.kinds() {
        assert_eq!(second.tally(kind).existing, tally.written, "{kind}");
    }
    assert_eq!(store.list_revisions("sweep").unwrap().len(), 2);
}

#[tokio::test]
async fn the_legacy_files_are_left_exactly_as_they_were() {
    let before = tree_digest(&fixtures());
    import_workflow_store(&layout(), &target()).await.unwrap();
    assert_eq!(tree_digest(&fixtures()), before);
}

#[tokio::test]
async fn nothing_the_target_already_holds_is_replaced() {
    let storage = MemoryStorage::new();
    let store = DriverWorkflowStore::new(docs_in(&storage, "local"));
    let mut live = tinyflows::store::parse_workflow(
        &json!({
            "id": "sweep", "name": "Live",
            "nodes": [{ "id": "t", "kind": "trigger", "name": "start", "config": { "trigger_kind": "manual" } }],
            "edges": []
        })
        .to_string(),
        "sweep",
    )
    .unwrap();
    live.description = "live description".into();
    store.save(&live).unwrap();

    let report = import_workflow_store(&layout(), &store).await.unwrap();

    assert_eq!(report.tally("workflows").existing, 1);
    assert_eq!(
        store.get("sweep").unwrap().unwrap().description,
        "live description"
    );
}

#[tokio::test]
async fn files_that_do_not_parse_are_counted_and_the_rest_imported() {
    let root = tempfile::tempdir().unwrap();
    let layout =
        FileStoreLayout::with_state(root.path().join("workflows"), &root.path().join("state"));
    let write = |path: PathBuf, body: &str| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    };
    let good = json!({
        "name": "Good",
        "nodes": [{ "id": "t", "kind": "trigger", "name": "start", "config": { "trigger_kind": "manual" } }],
        "edges": []
    })
    .to_string();
    // An id from the file name, since the document has none.
    write(layout.definitions.join("good.json"), &good);
    write(layout.definitions.join("junk.json"), "{nope");
    write(
        layout.definitions.join("hollow.json"),
        r#"{"nodes":[],"edges":[]}"#,
    );
    write(layout.definitions.join("notes.txt"), "ignored");
    write(layout.runs.join("bad.json"), "{nope");
    write(layout.journal.join("w.json"), "not an array");
    write(layout.proposals.join("bad.json"), "[]");
    write(layout.revisions[0].join("good").join("bad.json"), "{nope");

    let store = target();
    let report = import_workflow_store(&layout, &store).await.unwrap();

    assert_eq!(report.tally("workflows").written, 1);
    assert_eq!(report.tally("workflows").invalid, 2);
    for kind in ["runs", "notes", "proposals", "revisions"] {
        assert_eq!(report.tally(kind).invalid, 1, "{kind}");
    }
    assert_eq!(store.get("good").unwrap().unwrap().name, "Good");
}

#[tokio::test]
async fn notes_filed_under_another_workflow_are_counted_invalid() {
    let root = tempfile::tempdir().unwrap();
    let layout = FileStoreLayout::with_state(root.path().join("w"), &root.path().join("s"));
    std::fs::create_dir_all(&layout.journal).unwrap();
    use tinyflows::store::types::{NoteKind, NoteSource, WorkflowNote};
    let note = |workflow: &str, id: &str| WorkflowNote {
        id: id.to_string(),
        workflow_id: workflow.to_string(),
        kind: NoteKind::Observation,
        text: "t".to_string(),
        recorded_at: 1,
        source: NoteSource::System,
        run_ids: Vec::new(),
        superseded_by: None,
        pinned: false,
    };
    let body = serde_json::to_vec(&[note("mine", "a"), note("other", "b")]).unwrap();
    std::fs::write(layout.journal.join("mine.json"), body).unwrap();
    let store = target();
    let report = import_workflow_store(&layout, &store).await.unwrap();
    assert_eq!(report.tally("notes").written, 1);
    assert_eq!(report.tally("notes").invalid, 1);
    assert_eq!(store.list_notes("mine").unwrap().len(), 1);
    assert!(store.list_notes("other").unwrap().is_empty());
}

#[tokio::test]
async fn an_empty_or_absent_store_is_an_empty_report() {
    let root = tempfile::tempdir().unwrap();
    let layout = FileStoreLayout::with_state(root.path().join("w"), &root.path().join("s"));
    let report = import_workflow_store(&layout, &target()).await.unwrap();
    assert_eq!(report.total_written(), 0);
    assert!(report.is_clean());
}

#[tokio::test]
async fn a_definitions_path_that_is_a_file_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("workflows");
    std::fs::write(&file, "x").unwrap();
    let layout = FileStoreLayout::with_state(file, &root.path().join("s"));
    assert!(import_workflow_store(&layout, &target()).await.is_err());
}

#[test]
fn with_state_puts_each_kind_where_the_old_constructor_did() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let definitions = root.path().join("workflows");
    let layout = FileStoreLayout::with_state(&definitions, &state);
    assert_eq!(layout.definitions, definitions);
    assert_eq!(layout.runs, state.join("runs"));
    assert_eq!(layout.journal, state.join("journal"));
    assert_eq!(layout.proposals, state.join("proposals"));
    assert_eq!(layout.revisions.len(), 2);
    let hashed = &layout.revisions[0];
    assert_eq!(hashed.file_name().unwrap(), "revisions");
    let scope = hashed.parent().unwrap();
    assert_eq!(scope.parent().unwrap(), state.join("definitions"));
    assert_eq!(scope.file_name().unwrap().len(), 64, "a hex SHA-256");
    assert_eq!(layout.revisions[1], definitions.join(".revisions"));
}

#[test]
fn the_revisions_directory_does_not_depend_on_how_the_write_directory_is_spelled() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("workflows")).unwrap();
    std::fs::create_dir_all(root.path().join("elsewhere")).unwrap();
    let state = root.path().join("state");
    let plain = FileStoreLayout::with_state(root.path().join("workflows"), &state);
    let roundabout = FileStoreLayout::with_state(
        root.path().join("elsewhere").join("..").join("workflows"),
        &state,
    );
    assert_eq!(plain.revisions[0], roundabout.revisions[0]);
    let other = FileStoreLayout::with_state(root.path().join("elsewhere"), &state);
    assert_ne!(plain.revisions[0], other.revisions[0]);
}

#[cfg(unix)]
#[test]
fn a_symlinked_write_directory_names_the_same_state_as_its_target() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    std::fs::create_dir_all(&real).unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let state = root.path().join("state");
    assert_eq!(
        FileStoreLayout::with_state(&real, &state).revisions[0],
        FileStoreLayout::with_state(&link, &state).revisions[0]
    );
}

#[test]
fn a_relative_write_directory_resolves_against_the_working_directory() {
    let state = tempfile::tempdir().unwrap();
    let relative = FileStoreLayout::with_state("relative-workflows", state.path());
    let absolute = FileStoreLayout::with_state(
        std::env::current_dir().unwrap().join("relative-workflows"),
        state.path(),
    );
    assert_eq!(relative.revisions[0], absolute.revisions[0]);
}

#[test]
fn discover_scopes_runs_to_the_workspace_and_definitions_to_home() {
    let home = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let layout = FileStoreLayout::discover(home.path(), workspace.path());
    let state = home.path().join("state/workflows");
    assert_eq!(layout.definitions, home.path().join("workflows"));
    let scope = layout.runs.parent().unwrap();
    assert_eq!(scope.parent().unwrap(), state.join("scopes"));
    assert_eq!(scope.file_name().unwrap().len(), 16);
    assert_eq!(layout.journal, scope.join("journal"));
    assert_eq!(layout.proposals, scope.join("proposals"));
    assert!(layout.revisions[0].starts_with(state.join("definitions")));
    // A different workspace gets its own run state but the same definition state.
    let other = tempfile::tempdir().unwrap();
    let elsewhere = FileStoreLayout::discover(home.path(), other.path());
    assert_ne!(layout.runs, elsewhere.runs);
    assert_eq!(layout.revisions, elsewhere.revisions);
}

#[test]
fn a_workspace_that_does_not_exist_yet_still_has_a_scope() {
    let home = tempfile::tempdir().unwrap();
    let layout = FileStoreLayout::discover(home.path(), Path::new("/no/such/workspace/yet"));
    assert_eq!(layout.runs.parent().unwrap().file_name().unwrap().len(), 16);
}
