//! `jobs.db`: the cron store before it moved to documents, copied into a
//! `CronDocuments` store. See `legacy_cron` for the reader.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use tinyflows_drivers::schedule::CronDocuments;

use crate::legacy_cron::read_legacy_cron;
use crate::report::ImportReport;

/// The jobs and run history in `jobs_db` (the file the old store was opened
/// with), copied into `cron`.
///
/// A flow-schedule job is filed under the id the document store derives from
/// its flow, and its runs go with it. Jobs are imported with their next run,
/// last run and schedule exactly as stored; a job that is due is simply due.
///
/// # Errors
///
/// When the file exists but cannot be read, or the target store fails (for
/// example an active-job cap the jobs would exceed). A row that cannot be
/// decoded is counted in the report as invalid.
pub async fn import_cron(jobs_db: &Path, cron: &CronDocuments) -> Result<ImportReport> {
    let legacy = read_legacy_cron(jobs_db).await?;
    let mut report = legacy.report;
    let mut filed_as = HashMap::new();
    for job in &legacy.jobs {
        let imported = cron.import_job(job).await?;
        report.wrote("jobs", imported.written);
        filed_as.insert(job.id.clone(), imported.id);
    }
    for mut run in legacy.runs {
        let Some(id) = filed_as.get(&run.job_id) else {
            report.invalid(
                "runs",
                format!("run {}: job '{}' was not imported", run.id, run.job_id),
            );
            continue;
        };
        run.job_id = id.clone();
        report.wrote("runs", cron.import_run(&run).await?);
    }
    tracing::info!(
        target: "tinyflows_import",
        written = report.total_written(),
        problems = report.problems().len(),
        "[import] cron jobs"
    );
    Ok(report)
}

#[cfg(test)]
#[path = "cron_tests.rs"]
mod tests;
