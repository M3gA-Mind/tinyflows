//! Pieces every job store shares, whatever it stores into: the spec an agent
//! job is created from and the bound on stored run output.
//!
//! Every job store persists [`CronJob`]s and [`CronRun`]s (`tinyflows-drivers`
//! does, and `tinyflows-sqlite` did); keeping these here means stores cannot
//! drift on what an agent job defaults to or how much output a run keeps.
//!
//! [`CronJob`]: crate::CronJob
//! [`CronRun`]: crate::CronRun

use crate::{CronJob, CronJobPatch, JobType};

pub use tinyflows_bus::schedule::{AgentJobSpec, MAX_CRON_OUTPUT_BYTES, TRUNCATED_OUTPUT_MARKER};

/// Refuses a patch no store may apply to `job`.
///
/// A flow-schedule job's `command` is the id of the flow it fires, and each
/// flow has at most one such job: the SQLite store keeps them unique by
/// `command`, the document store keys them `flow:<flow_id>`. Re-targeting
/// one by patching `command` would leave the job filed under its old flow,
/// so a later registration for the new flow would add a second job for it.
/// Remove the job and register one for the other flow instead. Setting
/// `command` to its current value is allowed.
///
/// # Errors
///
/// When `patch` changes a flow-schedule job's `command`.
pub fn check_patch(job: &CronJob, patch: &CronJobPatch) -> anyhow::Result<()> {
    if job.job_type == JobType::Flow
        && patch
            .command
            .as_ref()
            .is_some_and(|command| *command != job.command)
    {
        anyhow::bail!(
            "Cron job '{}' fires flow '{}'; its command cannot be changed. Remove it and \
             register a schedule job for the other flow instead",
            job.id,
            job.command
        );
    }
    Ok(())
}

/// `output` bounded to [`MAX_CRON_OUTPUT_BYTES`]: unchanged when it fits,
/// otherwise cut at a char boundary and ended with [`TRUNCATED_OUTPUT_MARKER`].
pub fn truncate_cron_output(output: &str) -> String {
    if output.len() <= MAX_CRON_OUTPUT_BYTES {
        return output.to_string();
    }

    if MAX_CRON_OUTPUT_BYTES <= TRUNCATED_OUTPUT_MARKER.len() {
        return TRUNCATED_OUTPUT_MARKER.to_string();
    }

    let mut cutoff = MAX_CRON_OUTPUT_BYTES - TRUNCATED_OUTPUT_MARKER.len();
    while cutoff > 0 && !output.is_char_boundary(cutoff) {
        cutoff -= 1;
    }

    let mut truncated = output[..cutoff].to_string();
    truncated.push_str(TRUNCATED_OUTPUT_MARKER);
    truncated
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
