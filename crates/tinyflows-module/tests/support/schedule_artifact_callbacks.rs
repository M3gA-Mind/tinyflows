// Shared fixture for the compiled module test; client uses only the bus wire vocabulary.
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use tinybus::Result;
use tinyflows_bus::schedule::{ManualClaimReply, OccurrenceClaim, RunJobRefusal};
#[derive(Default)]
pub struct State {
    pub records: Mutex<Vec<Value>>,
    pub completions: Mutex<Vec<Value>>,
    pub triggers: Mutex<Vec<String>>,
    pub trigger_jobs: Mutex<Vec<Value>>,
    pub defer_first: std::sync::atomic::AtomicBool,
    pub started: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub exclusions: Mutex<Vec<Vec<String>>>,
    pub leases: Mutex<HashMap<String, (String, DateTime<Utc>)>>,
    pub fail_send: std::sync::atomic::AtomicBool,
    pub sends: std::sync::atomic::AtomicUsize,
    pub projections: Mutex<Vec<Value>>,
    pub executions: Mutex<Vec<String>>,
    pub defer_execute: std::sync::atomic::AtomicBool,
    pub execute_started: tokio::sync::Notify,
    pub execute_release: tokio::sync::Notify,
    pub terminal_projection: tokio::sync::Notify,
}
pub struct Callbacks(pub Arc<State>);
#[tinybus::interface(name = "ai.tinyhumans.tinyflows.ScheduleHost")]
impl Callbacks {
    async fn due_jobs(&self, _now: String) -> Result<Vec<Value>> {
        if !self.0.defer_first.load(std::sync::atomic::Ordering::SeqCst)
            && !self.0.completions.lock().unwrap().is_empty()
        {
            return Ok(vec![]);
        }
        Ok(vec![
            json!({"id":"artifact-flow-job", "expression":"", "schedule":{"kind":"at", "at":"2026-01-01T00:00:00Z"}, "command":"artifact-flow", "job_type":"flow", "session_target":"isolated", "enabled":true, "delivery":{"mode":"none"}, "delete_after_run":false, "created_at":"2026-01-01T00:00:00Z", "next_run":"2026-01-01T00:00:00Z", "prompt":null, "name":null, "model":null, "agent_id":null, "last_run":null, "last_status":null, "last_output":null}),
        ])
    }
    async fn due_jobs_excluding(&self, now: String, excluded: Vec<String>) -> Result<Vec<Value>> {
        self.0.exclusions.lock().unwrap().push(excluded.clone());
        let mut jobs = self.due_jobs(now).await?;
        if self.0.defer_first.load(std::sync::atomic::Ordering::SeqCst) {
            let mut later = jobs[0].clone();
            later["id"] = json!("later-flow-job");
            later["command"] = json!("later-flow");
            jobs.push(later);
        }
        jobs.retain(|job| !excluded.iter().any(|id| job["id"] == *id));
        jobs.truncate(1);
        Ok(jobs)
    }
    async fn claim_due_occurrence(
        &self,
        value: Value,
        owner: String,
        now: DateTime<Utc>,
        lease_ms: i64,
    ) -> Result<Option<OccurrenceClaim>> {
        let job: tinyflows_bus::schedule::CronJob = serde_json::from_value(value)?;
        let mut leases = self.0.leases.lock().unwrap();
        if leases.get(&job.id).is_some_and(|(_, expiry)| *expiry > now) {
            return Ok(None);
        }
        let receipt = uuid::Uuid::new_v4().to_string();
        leases.insert(
            job.id.clone(),
            (receipt.clone(), now + chrono::Duration::milliseconds(lease_ms)),
        );
        Ok(Some(OccurrenceClaim {
            receipt,
            occurrence: job.next_run,
            started_at: now,
            owner,
            job,
            run_id: None,
        }))
    }
    async fn claim_manual_occurrence(
        &self,
        job: tinyflows_bus::schedule::CronJob,
        owner: String,
        now: DateTime<Utc>,
        lease_ms: i64,
    ) -> Result<ManualClaimReply> {
        let mut leases = self.0.leases.lock().unwrap();
        if leases.get(&job.id).is_some_and(|(_, expiry)| *expiry > now) {
            return Ok(ManualClaimReply::Refused {
                reason: RunJobRefusal::AlreadyRunning,
            });
        }
        let receipt = uuid::Uuid::new_v4().to_string();
        leases.insert(
            job.id.clone(),
            (receipt.clone(), now + chrono::Duration::milliseconds(lease_ms)),
        );
        Ok(ManualClaimReply::Claimed {
            claim: OccurrenceClaim {
                receipt,
                occurrence: job.next_run,
                started_at: now,
                owner,
                job,
                run_id: None,
            },
        })
    }
    async fn renew_occurrence(
        &self,
        receipt: String,
        now: DateTime<Utc>,
        lease_ms: i64,
    ) -> Result<bool> {
        let mut leases = self.0.leases.lock().unwrap();
        let Some((_, expiry)) = leases
            .values_mut()
            .find(|(current, expiry)| current == &receipt && *expiry > now)
        else {
            return Ok(false);
        };
        *expiry = (*expiry).max(now + chrono::Duration::milliseconds(lease_ms));
        Ok(true)
    }
    async fn release_occurrence(&self, receipt: String, _: DateTime<Utc>) -> Result<bool> {
        let mut leases = self.0.leases.lock().unwrap();
        let Some(job_id) = leases
            .iter()
            .find_map(|(id, (current, _))| (current == &receipt).then(|| id.clone()))
        else {
            return Ok(false);
        };
        leases.remove(&job_id);
        Ok(true)
    }
    async fn queue_occurrence(&self, _: String, record: Value) -> Result<bool> {
        self.0.records.lock().unwrap().push(record);
        Ok(true)
    }
    async fn complete_occurrence(
        &self,
        receipt: String,
        record: Value,
        completion: Value,
    ) -> Result<bool> {
        let mut leases = self.0.leases.lock().unwrap();
        let Some(job_id) = leases
            .iter()
            .find_map(|(id, (current, _))| (current == &receipt).then(|| id.clone()))
        else {
            return Ok(false);
        };
        leases.remove(&job_id);
        self.0.records.lock().unwrap().push(record);
        self.0.completions.lock().unwrap().push(completion);
        Ok(true)
    }
    async fn execute(&self, job: Value, run_id: String) -> Result<Value> {
        if job["job_type"] == "flow" {
            return Err(tinybus::Error::failed("a flow must use ScheduleTrigger"));
        }
        if self
            .0
            .defer_execute
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.0.execute_started.notify_one();
            self.0.execute_release.notified().await;
        }
        self.0.executions.lock().unwrap().push(run_id);
        Ok(json!({
            "success":true,"output":"manual output","retryable":false,
            "raw_detail":"opaque callback classification"
        }))
    }
    async fn failed(&self, _job: Value, _run_id: String, _attempt: Value) -> Result<()> {
        Ok(())
    }
    async fn send(&self, _job: Value, _run_id: String, _attempt: Value) -> Result<String> {
        self.0.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(if self
            .0
            .fail_send
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            "failed"
        } else {
            "not_requested"
        }
        .into())
    }
    async fn deliver(&self, projection: Value) -> Result<()> {
        self.0.projections.lock().unwrap().push(projection);
        self.0.terminal_projection.notify_one();
        Ok(())
    }
    async fn schedule_trigger(&self, job: Value, _run_id: String) -> Result<Value> {
        let flow_id = job["command"].as_str().unwrap_or_default().to_owned();
        self.0.triggers.lock().unwrap().push(flow_id.clone());
        self.0.trigger_jobs.lock().unwrap().push(job.clone());
        if flow_id == "artifact-flow"
            && self.0.defer_first.load(std::sync::atomic::Ordering::SeqCst)
        {
            self.0.started.notify_one();
            self.0.release.notified().await;
        }
        Ok(json!({
            "success":true,"output":"scheduled","retryable":false,
            "raw_detail":"opaque trigger classification"
        }))
    }
}
