use super::*;
use crate::test_support::{docs, expected, fixtures, json, tree_digest};
use serde_json::Value;
use tinystoragedrivers_sqlite::rusqlite::Connection;

fn jobs_db() -> std::path::PathBuf {
    fixtures().join("cron/jobs.db")
}

fn target() -> CronDocuments {
    CronDocuments::new(docs())
}

/// `value` without its `id` / `job_id`, which a flow-schedule job changes.
fn without_ids(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("id");
        object.remove("job_id");
    }
    value
}

#[tokio::test]
async fn every_job_and_run_comes_across_with_its_fields() {
    let cron = target();
    let report = import_cron(&jobs_db(), &cron).await.unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    assert_eq!(report.tally("jobs").written, 3);
    assert_eq!(report.tally("runs").written, 4);

    let expected = expected();
    let expected = &expected["cron"];
    for legacy in expected["jobs"].as_array().unwrap() {
        let legacy_id = legacy["id"].as_str().unwrap();
        let is_flow = legacy["job_type"] == "flow";
        let new_id = if is_flow {
            format!("flow:{}", legacy["command"].as_str().unwrap())
        } else {
            legacy_id.to_string()
        };
        let imported = cron.get_job(&new_id).await.unwrap();
        assert_eq!(
            without_ids(json(&imported)),
            without_ids(legacy.clone()),
            "job {legacy_id}"
        );
        if !is_flow {
            assert_eq!(imported.id, legacy_id, "an ordinary job keeps its id");
        }
        let runs = cron.list_runs(&new_id, 100).await.unwrap();
        let legacy_runs: Vec<Value> = expected["runs"][legacy_id].as_array().unwrap().clone();
        assert_eq!(
            runs.iter().map(|r| without_ids(json(r))).collect::<Vec<_>>(),
            legacy_runs.into_iter().map(without_ids).collect::<Vec<_>>(),
            "runs of {legacy_id}"
        );
        for run in &runs {
            assert_eq!(run.job_id, new_id, "runs follow their job");
        }
    }
}

#[tokio::test]
async fn run_numbers_are_kept_and_new_runs_come_after_them() {
    let cron = target();
    import_cron(&jobs_db(), &cron).await.unwrap();
    let expected = expected();
    let highest = expected["cron"]["runs"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|runs| runs.as_array().unwrap())
        .map(|run| run["id"].as_i64().unwrap())
        .max()
        .unwrap();
    let job = cron.list_jobs().await.unwrap().remove(0);
    let now = chrono::Utc::now();
    cron.record_run(&job.id, now, now, "ok", None, 1).await.unwrap();
    let newest = cron.list_runs(&job.id, 1).await.unwrap();
    assert_eq!(newest[0].id, highest + 1);
}

#[tokio::test]
async fn a_flow_schedule_job_is_found_by_its_flow_after_the_import() {
    let cron = target();
    import_cron(&jobs_db(), &cron).await.unwrap();
    let found = cron.find_flow_schedule_job("flow-beta").await.unwrap().unwrap();
    assert_eq!(found.command, "flow-beta");
    assert_eq!(found.id, "flow:flow-beta");
    // Registering the flow again returns it rather than adding a second.
    let again = cron
        .add_flow_schedule_job("flow-beta", tinyflows_schedule::Schedule::Every { every_ms: 5_000 })
        .await
        .unwrap();
    assert_eq!(again.id, found.id);
    assert_eq!(cron.list_jobs().await.unwrap().len(), 3);
}

#[tokio::test]
async fn importing_again_writes_nothing() {
    let cron = target();
    import_cron(&jobs_db(), &cron).await.unwrap();
    let second = import_cron(&jobs_db(), &cron).await.unwrap();
    assert_eq!(second.total_written(), 0);
    assert_eq!(second.tally("jobs").existing, 3);
    assert_eq!(second.tally("runs").existing, 4);
    assert_eq!(cron.list_jobs().await.unwrap().len(), 3);
}

#[tokio::test]
async fn the_legacy_files_are_left_exactly_as_they_were() {
    let before = tree_digest(&fixtures());
    import_cron(&jobs_db(), &target()).await.unwrap();
    assert_eq!(tree_digest(&fixtures()), before);
}

#[tokio::test]
async fn a_missing_file_is_an_empty_report() {
    let dir = tempfile::tempdir().unwrap();
    let report = import_cron(&dir.path().join("jobs.db"), &target()).await.unwrap();
    assert_eq!(report.total_written(), 0);
}

#[tokio::test]
async fn a_database_without_the_jobs_table_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jobs.db");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE other (x INTEGER);")
        .unwrap();
    assert_eq!(import_cron(&path, &target()).await.unwrap().total_written(), 0);
}

fn legacy(dir: &std::path::Path, sql: &str) -> std::path::PathBuf {
    let path = dir.join("jobs.db");
    Connection::open(&path).unwrap().execute_batch(sql).unwrap();
    path
}

#[tokio::test]
async fn an_early_schema_with_only_an_expression_and_a_side_policy_table_reads_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = legacy(
        dir.path(),
        "CREATE TABLE cron_jobs (id TEXT PRIMARY KEY, expression TEXT NOT NULL, command TEXT NOT NULL,
             created_at TEXT NOT NULL, next_run TEXT NOT NULL, last_run TEXT, last_status TEXT, last_output TEXT);
         INSERT INTO cron_jobs VALUES ('old', '0 9 * * *', 'echo old', '2024-01-01T00:00:00Z', '2024-01-02T09:00:00Z', NULL, NULL, NULL);
         CREATE TABLE cron_job_policies (job_id TEXT PRIMARY KEY, retries INTEGER, single_flight INTEGER NOT NULL);
         INSERT INTO cron_job_policies VALUES ('old', 3, 1);
         CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, started_at TEXT NOT NULL,
             finished_at TEXT NOT NULL, status TEXT NOT NULL, output TEXT, duration_ms INTEGER);
         INSERT INTO cron_runs VALUES (7, 'old', '2024-01-01T09:00:00Z', '2024-01-01T09:00:02Z', 'ok', NULL, 2000);",
    );
    let cron = target();
    let report = import_cron(&path, &cron).await.unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    let job = cron.get_job("old").await.unwrap();
    assert_eq!(
        job.schedule,
        tinyflows_schedule::Schedule::Cron {
            expr: "0 9 * * *".into(),
            tz: None,
            active_hours: None
        }
    );
    assert!(job.enabled);
    assert_eq!(job.job_type, JobType::Shell);
    assert_eq!(job.policy.retries, Some(3));
    assert!(job.policy.single_flight);
    assert_eq!(job.claim_generation, 0);
    let runs = cron.list_runs("old", 10).await.unwrap();
    assert_eq!(runs[0].id, 7);
    assert!(runs[0].delivery_status.is_none());
}

#[tokio::test]
async fn rows_that_do_not_decode_cost_only_themselves() {
    let dir = tempfile::tempdir().unwrap();
    let path = legacy(
        dir.path(),
        "CREATE TABLE cron_jobs (id TEXT PRIMARY KEY, expression TEXT NOT NULL, command TEXT NOT NULL, schedule TEXT,
             job_type TEXT NOT NULL DEFAULT 'shell', created_at TEXT NOT NULL, next_run TEXT NOT NULL,
             last_run TEXT, last_status TEXT, last_output TEXT);
         INSERT INTO cron_jobs VALUES ('good', '0 9 * * *', 'echo', NULL, 'shell', '2024-01-01T00:00:00Z', '2024-01-02T09:00:00Z', NULL, NULL, NULL);
         INSERT INTO cron_jobs VALUES ('no-schedule', '', 'echo', NULL, 'shell', '2024-01-01T00:00:00Z', '2024-01-02T09:00:00Z', NULL, NULL, NULL);
         INSERT INTO cron_jobs VALUES ('bad-schedule', '', 'echo', '{not json', 'shell', '2024-01-01T00:00:00Z', '2024-01-02T09:00:00Z', NULL, NULL, NULL);
         INSERT INTO cron_jobs VALUES ('bad-time', '0 9 * * *', 'echo', NULL, 'shell', 'yesterday', '2024-01-02T09:00:00Z', NULL, NULL, NULL);
         CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, started_at TEXT NOT NULL,
             finished_at TEXT NOT NULL, status TEXT NOT NULL, output TEXT, duration_ms INTEGER);
         INSERT INTO cron_runs VALUES (1, 'good', '2024-01-01T09:00:00Z', '2024-01-01T09:00:02Z', 'ok', NULL, 2000);
         INSERT INTO cron_runs VALUES (2, 'good', 'when', '2024-01-01T09:00:02Z', 'ok', NULL, 2000);
         INSERT INTO cron_runs VALUES (3, 'bad-time', '2024-01-01T09:00:00Z', '2024-01-01T09:00:02Z', 'ok', NULL, 2000);",
    );
    let cron = target();
    let report = import_cron(&path, &cron).await.unwrap();

    assert_eq!(report.tally("jobs").written, 1);
    assert_eq!(report.tally("jobs").invalid, 3);
    assert_eq!(report.tally("runs").written, 1);
    assert_eq!(report.tally("runs").invalid, 2, "{:?}", report.problems());
    assert_eq!(cron.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_file_that_is_not_a_database_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jobs.db");
    std::fs::write(&path, b"not a sqlite database, just a line of text that is long enough").unwrap();
    assert!(import_cron(&path, &target()).await.is_err());
}

#[tokio::test]
async fn a_job_over_the_targets_active_cap_stops_the_import_with_an_error() {
    let cron = target();
    cron.set_max_active_jobs(Some(1)).await.unwrap();
    let error = import_cron(&jobs_db(), &cron).await.unwrap_err();
    assert!(format!("{error:#}").contains("active job limit exceeded"));
}
