//! Separate Schedule object in the same loadable TinyFlows module.
#[path = "schedule_clock.rs"]
mod clock;
#[path = "schedule_manual_workers.rs"]
mod manual_workers;
#[path = "schedule_stop_supervisor.rs"]
mod stop_supervisor;
use super::schedule_callbacks::HostCallbacks;
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};
use tinybus::{Connection, Error, Result};
use tinyflows_bus::schedule::{
    self, AcknowledgeManualRun, CheckOriginRequest, CheckPatchRequest, ManualRunHandle,
    ManualRunState, NextRunRequest, PrepareManualRun, RunJobNow, RunJobReply,
    RunsCloserThanRequest, StartManualRun, StartScheduler, StopManualRun, TooFrequent,
    ValidateScheduleRequest, WaitManualRun,
};
use tinyflows_schedule::{
    DeliveryConfig, JobOrigin, Schedule, runtime::*, schedule as maths, store, validation,
};
use tokio::{
    sync::{Mutex, oneshot, watch},
    task::JoinHandle,
};

pub(super) enum Running {
    /// A host-driven scheduler and the calendar clock its passes read.
    Manual(Arc<Mutex<Scheduler>>, Arc<clock::HostClock>),
    Loop {
        task: JoinHandle<()>,
        shutdown: Option<oneshot::Sender<()>>,
    },
    Stopping(watch::Sender<Option<std::result::Result<(), String>>>),
}

pub(super) struct ScheduleService {
    connection: Connection,
    runtimes: Arc<StdMutex<HashMap<String, Running>>>,
    manual_workers: Mutex<manual_workers::ManualWorkers>,
    stop_lifetime: watch::Sender<bool>,
}
impl Drop for ScheduleService {
    /// Signal teardown to stop supervisors and abort active polling loops.
    fn drop(&mut self) {
        self.stop_lifetime.send_replace(true);
        for running in self
            .runtimes
            .lock()
            .expect("scheduler registry poisoned")
            .values()
        {
            if let Running::Loop { task, .. } = running {
                task.abort();
            }
        }
        self.manual_workers.get_mut().request_stop_all();
    }
}
impl ScheduleService {
    /// Create a scheduler service bound to this module connection.
    pub(super) fn new(connection: Connection) -> Self {
        let (stop_lifetime, _) = watch::channel(false);
        Self {
            connection,
            runtimes: Arc::new(StdMutex::new(HashMap::new())),
            manual_workers: Mutex::new(manual_workers::ManualWorkers::default()),
            stop_lifetime,
        }
    }
    /// Return an active manually-driven scheduler, rejecting other states.
    async fn manual(&self, handle: &str) -> Result<Arc<Mutex<Scheduler>>> {
        self.manual_with_clock(handle).await.map(|(runtime, _)| runtime)
    }
    /// Return an active manually-driven scheduler and its calendar clock.
    async fn manual_with_clock(
        &self,
        handle: &str,
    ) -> Result<(Arc<Mutex<Scheduler>>, Arc<clock::HostClock>)> {
        match self
            .runtimes
            .lock()
            .expect("scheduler registry poisoned")
            .get(handle)
        {
            Some(Running::Manual(runtime, clock)) => Ok((runtime.clone(), clock.clone())),
            Some(Running::Stopping(_)) => Err(Error::failed("scheduler is stopping")),
            Some(_) => Err(Error::failed("scheduler owns its poll loop")),
            None => Err(Error::failed("unknown scheduler handle")),
        }
    }
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::failed(error.to_string())
}

async fn persist_manual_queue(
    store: &dyn JobStore,
    claim: &tinyflows_bus::schedule::OccurrenceClaim,
    record: RunRecord,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    match store.queue_occurrence(claim, record).await {
        Ok(true) => Ok(true),
        Ok(false) => {
            let _ = store.release_occurrence(claim, now).await;
            Ok(false)
        }
        Err(error) => {
            if let Err(release_error) = store.release_occurrence(claim, now).await {
                tracing::warn!(%release_error, "manual admission failed to release its queue claim");
            }
            Err(error)
        }
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Schedule")]
impl ScheduleService {
    async fn schedule_store(
        &self,
        request: tinyflows_bus::schedule::StoreRequest,
    ) -> Result<Value> {
        super::schedule_store::Store::request(self.connection.clone(), request)
            .await
            .map_err(failure)
    }

    async fn start_scheduler(&self, request: StartScheduler) -> Result<String> {
        let now: Option<chrono::DateTime<chrono::Utc>> = request
            .config
            .get("now")
            .map(|now| serde_json::from_value(now.clone()))
            .transpose()
            .map_err(failure)?;
        if now.is_some() && !request.manual_ticks {
            return Err(Error::failed("a fixed calendar time requires manual ticks"));
        }
        let config: SchedulerConfig = serde_json::from_value(request.config).map_err(failure)?;
        let mut ports = HostCallbacks::ports(self.connection.clone(), request.callback);
        // A manual scheduler reads the host's calendar clock: fixed by
        // `config.now`, moved by `TickSchedulerAt`, system time otherwise.
        let manual_clock = request.manual_ticks.then(|| {
            Arc::new(now.map_or_else(clock::HostClock::system, clock::HostClock::fixed))
        });
        if let Some(manual_clock) = &manual_clock {
            ports.clock = manual_clock.clone();
        }
        if let Some(storage) = request.storage {
            ports.store = Arc::new(
                super::schedule_store::Store::new(self.connection.clone(), storage)
                    .await
                    .map_err(failure)?,
            );
        }
        let mut scheduler = Scheduler::new(request.scope, config, ports);
        if !scheduler.reserve_slot() {
            return Err(Error::failed("a scheduler already owns this scope"));
        }
        let handle = uuid::Uuid::new_v4().to_string();
        let running = if let Some(manual_clock) = manual_clock {
            Running::Manual(Arc::new(Mutex::new(scheduler)), manual_clock)
        } else {
            let (shutdown, stopped) = oneshot::channel();
            let task = tokio::spawn(async move {
                tokio::select! {
                    result = scheduler.run() => if let Err(error) = result { tracing::warn!(%error, "schedule loop stopped"); },
                    _ = stopped => {},
                }
                scheduler.shutdown().await;
            });
            Running::Loop {
                task,
                shutdown: Some(shutdown),
            }
        };
        self.runtimes
            .lock()
            .expect("scheduler registry poisoned")
            .insert(handle.clone(), running);
        Ok(handle)
    }

    /// Join the shared shutdown supervisor, or no-op for an absent handle.
    async fn stop_scheduler(&self, handle: String) -> Result<()> {
        let (mut stopped, start_cleanup) = {
            let mut runtimes = self.runtimes.lock().expect("scheduler registry poisoned");
            match runtimes.get(&handle) {
                None => return Ok(()),
                Some(Running::Stopping(completion)) => (completion.subscribe(), None),
                Some(_) => {
                    let running = runtimes
                        .remove(&handle)
                        .expect("registered scheduler exists");
                    let (completion, receiver) = watch::channel(None);
                    runtimes.insert(handle.clone(), Running::Stopping(completion.clone()));
                    (receiver, Some((running, completion)))
                }
            }
        };

        if let Some((running, completion)) = start_cleanup {
            let runtimes = self.runtimes.clone();
            let cleanup_handle = handle.clone();
            let lifetime = self.stop_lifetime.subscribe();
            let abort_on_cancel = stop_supervisor::abort_handle(&running);
            tokio::spawn(async move {
                let mut guard =
                    stop_supervisor::StopCompletionGuard::new(completion, abort_on_cancel);
                let result = tokio::select! {
                    result = stop_supervisor::shutdown_scheduler(running) => result,
                    _ = stop_supervisor::service_dropped(lifetime) => {
                        Err("scheduler service dropped before shutdown was confirmed".into())
                    }
                };
                if result.is_ok() {
                    runtimes
                        .lock()
                        .expect("scheduler registry poisoned")
                        .remove(&cleanup_handle);
                }
                guard.finish(result);
            });
        }

        loop {
            if let Some(result) = stopped.borrow().clone() {
                return result.map_err(failure);
            }
            if stopped.changed().await.is_err() {
                return Err(Error::failed(
                    "scheduler stop ended without confirming quiescence",
                ));
            }
        }
    }

    async fn tick_scheduler(&self, handle: String) -> Result<usize> {
        self.manual(&handle)
            .await?
            .lock()
            .await
            .tick()
            .await
            .map_err(failure)
    }
    /// One manual pass at the host-supplied calendar instant `now`.
    async fn tick_scheduler_at(
        &self,
        handle: String,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize> {
        let (runtime, clock) = self.manual_with_clock(&handle).await?;
        let mut scheduler = runtime.lock().await;
        // Set under the scheduler lock so no other pass reads a half-moved clock.
        clock.set(now);
        scheduler.tick().await.map_err(failure)
    }
    async fn drain_scheduler(&self, handle: String) -> Result<()> {
        self.manual(&handle)
            .await?
            .lock()
            .await
            .drain()
            .await
            .map_err(failure)
    }

    async fn run_job_now(&self, request: RunJobNow) -> Result<RunJobReply> {
        let config: SchedulerConfig = serde_json::from_value(request.config).map_err(failure)?;
        let ports = HostCallbacks::ports(self.connection.clone(), request.callback);
        let mut ports = ports;
        ports.store = Arc::new(
            super::schedule_store::Store::new(self.connection.clone(), request.storage)
                .await
                .map_err(failure)?,
        );
        let now = ports.clock.now();
        let owner = uuid::Uuid::new_v4().to_string();
        let claim = match ports
            .store
            .claim_manual_occurrence(&request.job, &owner, now, chrono::Duration::hours(2))
            .await
            .map_err(failure)?
        {
            Ok(claim) => claim,
            Err(reason) => return Ok(RunJobReply::Refused { reason }),
        };
        let mut claim = claim;
        let run_id = if request.run_id.trim().is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            request.run_id
        };
        claim.run_id = Some(run_id.clone());
        let queued_at = ports.clock.now();
        let queued = tinyflows_schedule::runtime::RunRecord {
            run_id: run_id.clone(),
            job_id: claim.job.id.clone(),
            job_created_at: claim.job.created_at,
            started_at: claim.started_at,
            finished_at: queued_at,
            status: "queued".into(),
            output: String::new(),
            delivery_status: None,
        };
        if !persist_manual_queue(ports.store.as_ref(), &claim, queued, queued_at)
            .await
            .map_err(failure)?
        {
            return Ok(RunJobReply::Refused {
                reason: tinyflows_bus::schedule::RunJobRefusal::DefinitionChanged,
            });
        }
        let scheduler = Scheduler::new(request.scope, config, ports);
        if request.wait {
            let outcome = scheduler
                .run_manual_claim(claim, request.delivery)
                .await
                .map_err(failure)?;
            Ok(RunJobReply::Completed {
                run_id,
                attempt: outcome.attempt,
                success: outcome.success,
                delivery_status: outcome.delivery_status,
            })
        } else {
            tokio::spawn(async move {
                if let Err(error) = scheduler.run_manual_claim(claim, request.delivery).await {
                    tracing::warn!(%error, "manual schedule run did not reach terminal persistence");
                }
            });
            Ok(RunJobReply::Accepted { run_id })
        }
    }

    async fn prepare_manual_run(&self, request: PrepareManualRun) -> Result<ManualRunHandle> {
        Ok(self.manual_workers.lock().await.prepare(request))
    }

    async fn start_manual_run(&self, request: StartManualRun) -> Result<ManualRunState> {
        let mut workers = self.manual_workers.lock().await;
        let reservation = workers
            .get(&request.handle)
            .ok_or_else(|| Error::failed("unknown, expired, or acknowledged manual run handle"))?;
        let fingerprint = serde_json::to_value(&request.request).map_err(failure)?;
        let (should_spawn, run_id) = reservation
            .entry
            .begin_start_with_run_id(fingerprint, &request.request.run_id)
            .map_err(failure)?;
        let state = reservation.entry.state();
        if should_spawn {
            let connection = self.connection.clone();
            let entry = reservation.entry.clone();
            let scope = reservation.scope.clone();
            let mut run_request = request.request;
            run_request.run_id = run_id;
            tokio::spawn(async move {
                manual_workers::run_manual_worker(connection, scope, run_request, entry).await;
            });
        }
        // The entry is already atomically Starting before the worker task is
        // scheduled. Duplicate identical starts only observe this state.
        let _ = &reservation.request_id;
        drop(workers);
        Ok(state)
    }

    async fn wait_manual_run(&self, request: WaitManualRun) -> Result<Option<ManualRunState>> {
        Ok(self
            .manual_workers
            .lock()
            .await
            .get(&request.handle)
            .map(|reservation| reservation.entry.state()))
    }

    async fn stop_manual_run(&self, request: StopManualRun) -> Result<Option<ManualRunState>> {
        Ok(self
            .manual_workers
            .lock()
            .await
            .get(&request.handle)
            .map(|reservation| reservation.entry.request_stop()))
    }

    async fn acknowledge_manual_run(&self, request: AcknowledgeManualRun) -> Result<bool> {
        Ok(self
            .manual_workers
            .lock()
            .await
            .acknowledge(&request.handle))
    }

    async fn parse_human_delay(&self, delay: String) -> Result<i64> {
        validation::parse_human_delay(&delay)
            .map(|d| d.num_milliseconds())
            .map_err(failure)
    }
    async fn validate_delivery(
        &self,
        delivery: Value,
        origin: Option<Value>,
        allowed_users: Option<Vec<String>>,
    ) -> Result<()> {
        let delivery: DeliveryConfig = serde_json::from_value(delivery).map_err(failure)?;
        let origin: Option<JobOrigin> = origin
            .map(serde_json::from_value)
            .transpose()
            .map_err(failure)?;
        validation::validate_delivery(&delivery, origin.as_ref(), allowed_users.as_deref())
            .map_err(failure)
    }

    async fn next_run(&self, request: NextRunRequest) -> Result<chrono::DateTime<chrono::Utc>> {
        // The contract promises an occurrence strictly after `from`; the
        // in-process function returns a one-shot's instant unchanged, so an
        // elapsed `At` is refused here instead of handed back as "next".
        if let Schedule::At { at } = &request.schedule
            && *at <= request.from
        {
            return Err(Error::failed(format!(
                "one-shot schedule at {at} is not after {}",
                request.from
            )));
        }
        maths::next_run_for_schedule(&request.schedule, request.from).map_err(failure)
    }
    async fn validate_schedule(&self, request: ValidateScheduleRequest) -> Result<()> {
        if request.agent {
            maths::validate_agent_schedule(&request.schedule, request.now)
        } else {
            maths::validate_schedule(&request.schedule, request.now)
        }
        .map_err(failure)
    }
    async fn runs_closer_than(
        &self,
        request: RunsCloserThanRequest,
    ) -> Result<Option<TooFrequent>> {
        let min_gap = chrono::Duration::try_milliseconds(request.min_gap_ms)
            .ok_or_else(|| Error::failed("min_gap_ms out of range"))?;
        Ok(maths::runs_closer_than(
            &request.schedule,
            request.from,
            min_gap,
        ))
    }
    async fn normalize_expression(&self, expression: String) -> Result<String> {
        maths::normalize_expression(&expression).map_err(failure)
    }
    async fn schedule_cron_expression(&self, schedule: Schedule) -> Result<Option<String>> {
        Ok(maths::schedule_cron_expression(&schedule))
    }
    async fn default_delivery(&self, origin: Option<JobOrigin>) -> Result<DeliveryConfig> {
        Ok(validation::default_delivery(origin.as_ref()))
    }
    async fn check_origin_requirements(&self, request: CheckOriginRequest) -> Result<()> {
        validation::check_origin_requirements(
            &request.session_target,
            &request.delivery,
            request.origin.as_ref(),
        )
        .map_err(Error::failed)
    }
    async fn check_patch(&self, request: CheckPatchRequest) -> Result<()> {
        store::check_patch(&request.job, &request.patch).map_err(failure)
    }
    async fn truncate_cron_output(&self, output: String) -> Result<String> {
        Ok(store::truncate_cron_output(&output))
    }
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(
            schedule::OBJECT_PATH.try_into()?,
            ScheduleService::new(connection.clone()),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "schedule_members_tests.rs"]
mod members_tests;
#[cfg(test)]
#[path = "schedule_service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schedule_clock_tests.rs"]
mod clock_tests;
