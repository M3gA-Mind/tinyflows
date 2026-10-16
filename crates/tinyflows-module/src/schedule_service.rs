//! Separate Schedule object in the same loadable TinyFlows module.
use super::schedule_callbacks::HostCallbacks;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tinybus::{Connection, Error, Result};
use tinyflows_bus::schedule::{
    self, CallbackAddress, CheckOriginRequest, CheckPatchRequest, NextRunRequest,
    RunsCloserThanRequest, StartScheduler, TooFrequent, ValidateScheduleRequest,
};
use tinyflows_schedule::{
    CronJob, DeliveryConfig, JobOrigin, Schedule, runtime::*, schedule as maths, store, validation,
};
use tokio::{
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

enum Running {
    Manual(Arc<Mutex<Scheduler>>),
    Loop {
        task: JoinHandle<()>,
        shutdown: Option<oneshot::Sender<()>>,
    },
}

pub(super) struct ScheduleService {
    connection: Connection,
    runtimes: Mutex<HashMap<String, Running>>,
}
impl Drop for ScheduleService {
    fn drop(&mut self) {
        for running in self.runtimes.get_mut().values() {
            if let Running::Loop { task, .. } = running {
                task.abort();
            }
        }
    }
}
impl ScheduleService {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            connection,
            runtimes: Mutex::new(HashMap::new()),
        }
    }
    async fn manual(&self, handle: &str) -> Result<Arc<Mutex<Scheduler>>> {
        match self.runtimes.lock().await.get(handle) {
            Some(Running::Manual(runtime)) => Ok(runtime.clone()),
            Some(_) => Err(Error::failed("scheduler owns its poll loop")),
            None => Err(Error::failed("unknown scheduler handle")),
        }
    }
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::failed(error.to_string())
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
        let config: SchedulerConfig = serde_json::from_value(request.config).map_err(failure)?;
        let mut ports = HostCallbacks::ports(self.connection.clone(), request.callback);
        if let Some(storage) = request.storage {
            ports.store = Arc::new(
                super::schedule_store::Store::new(self.connection.clone(), storage)
                    .await
                    .map_err(failure)?,
            );
        }
        let mut scheduler = Scheduler::new(request.scope, config, ports);
        let handle = uuid::Uuid::new_v4().to_string();
        let running = if request.manual_ticks {
            Running::Manual(Arc::new(Mutex::new(scheduler)))
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
        self.runtimes.lock().await.insert(handle.clone(), running);
        Ok(handle)
    }

    async fn stop_scheduler(&self, handle: String) -> Result<()> {
        let running = self
            .runtimes
            .lock()
            .await
            .remove(&handle)
            .ok_or_else(|| Error::failed("unknown scheduler handle"))?;
        // Shut down and join the owning loop, then release every queued and
        // running job claim before a replacement scheduler can poll.
        match running {
            Running::Loop {
                mut task,
                mut shutdown,
            } => {
                if let Some(shutdown) = shutdown.take() {
                    let _ = shutdown.send(());
                }
                (&mut task).await.map_err(failure)?;
            }
            Running::Manual(runtime) => runtime.lock().await.shutdown().await,
        }
        Ok(())
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
    async fn drain_scheduler(&self, handle: String) -> Result<()> {
        self.manual(&handle)
            .await?
            .lock()
            .await
            .drain()
            .await
            .map_err(failure)
    }

    async fn execute_job(
        &self,
        scope: String,
        callback: CallbackAddress,
        config: Value,
        job: Value,
        run_id: String,
    ) -> Result<Value> {
        let config: SchedulerConfig = serde_json::from_value(config).map_err(failure)?;
        let job: CronJob = serde_json::from_value(job).map_err(failure)?;
        let _claim = RunClaim::acquire(&scope, &job.id)
            .ok_or_else(|| Error::failed("job is already running"))?;
        let ports = HostCallbacks::ports(self.connection.clone(), callback);
        let result = execute_with_retry(&ports, &config, &job, &run_id)
            .await
            .map_err(failure)?;
        serde_json::to_value(result).map_err(failure)
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
        if let Schedule::At { at } = &request.schedule {
            if *at <= request.from {
                return Err(Error::failed(format!(
                    "one-shot schedule at {at} is not after {}",
                    request.from
                )));
            }
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
