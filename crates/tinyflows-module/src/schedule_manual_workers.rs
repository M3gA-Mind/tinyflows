//! Module-owned reservations and retained lifecycle state for detached manual runs.
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tinyflows_bus::schedule::{
    ManualRunHandle, ManualRunOutcome, ManualRunState, PrepareManualRun,
};
use tokio::{sync::watch, time::Instant};

const PREPARED_TTL: Duration = Duration::from_secs(15 * 60);
const TERMINAL_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub(super) struct ManualWorkerEntry {
    control: Mutex<Control>,
    pub(super) stop: watch::Sender<bool>,
}

struct Control {
    state: ManualRunState,
    fingerprint: Option<serde_json::Value>,
    stop_requested: bool,
    expires_at: Instant,
}

impl ManualWorkerEntry {
    fn prepared() -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            control: Mutex::new(Control {
                state: ManualRunState::Prepared,
                fingerprint: None,
                stop_requested: false,
                expires_at: Instant::now() + PREPARED_TTL,
            }),
            stop,
        }
    }

    pub(super) fn state(&self) -> ManualRunState {
        self.control.lock().expect("manual worker control poisoned").state.clone()
    }

    pub(super) fn begin_start(&self, fingerprint: serde_json::Value) -> anyhow::Result<bool> {
        let mut control = self.control.lock().expect("manual worker control poisoned");
        if let Some(existing) = &control.fingerprint {
            anyhow::ensure!(existing == &fingerprint, "manual worker start payload changed");
            return Ok(false);
        }
        anyhow::ensure!(
            matches!(control.state, ManualRunState::Prepared),
            "manual worker handle is no longer startable"
        );
        control.fingerprint = Some(fingerprint);
        control.state = ManualRunState::Starting;
        Ok(true)
    }

    pub(super) fn stopped(&self) -> bool {
        self.control
            .lock()
            .expect("manual worker control poisoned")
            .stop_requested
    }

    pub(super) fn set_state(&self, state: ManualRunState) {
        let mut control = self.control.lock().expect("manual worker control poisoned");
        if matches!(state, ManualRunState::Terminal { .. }) {
            control.expires_at = Instant::now() + TERMINAL_TTL;
        }
        control.state = state;
    }

    pub(super) fn set_running(&self, run_id: String) {
        let mut control = self.control.lock().expect("manual worker control poisoned");
        control.state = if control.stop_requested {
            ManualRunState::Stopping {
                run_id: Some(run_id),
            }
        } else {
            ManualRunState::Running { run_id }
        };
    }

    pub(super) fn request_stop(&self) -> ManualRunState {
        let mut control = self.control.lock().expect("manual worker control poisoned");
        match control.state.clone() {
            ManualRunState::Prepared => {
                control.stop_requested = true;
                control.state = ManualRunState::Terminal {
                    outcome: ManualRunOutcome::Stopped { run_id: None },
                };
                control.expires_at = Instant::now() + TERMINAL_TTL;
            }
            ManualRunState::Starting => {
                control.stop_requested = true;
                control.state = ManualRunState::Stopping { run_id: None };
                let _ = self.stop.send(true);
            }
            ManualRunState::Running { run_id } => {
                control.stop_requested = true;
                control.state = ManualRunState::Stopping {
                    run_id: Some(run_id),
                };
                let _ = self.stop.send(true);
            }
            ManualRunState::Stopping { .. } | ManualRunState::CleanupPending { .. } => {
                control.stop_requested = true;
                let _ = self.stop.send(true);
            }
            ManualRunState::Terminal { .. } => {}
        }
        control.state.clone()
    }

    fn expired(&self, now: Instant) -> bool {
        let control = self.control.lock().expect("manual worker control poisoned");
        matches!(
            control.state,
            ManualRunState::Prepared | ManualRunState::Terminal { .. }
        ) && control.expires_at <= now
    }
}

struct Reservation {
    scope: String,
    request_id: String,
    entry: std::sync::Arc<ManualWorkerEntry>,
}

#[derive(Default)]
pub(super) struct ManualWorkers {
    by_handle: HashMap<String, Reservation>,
    by_request: HashMap<(String, String), String>,
}

impl ManualWorkers {
    pub(super) fn prepare(&mut self, request: PrepareManualRun) -> ManualRunHandle {
        self.prune_expired();
        let key = (request.scope.clone(), request.request_id.clone());
        if let Some(handle) = self.by_request.get(&key) {
            return ManualRunHandle {
                token: handle.clone(),
            };
        }
        let handle = uuid::Uuid::new_v4().to_string();
        self.by_request.insert(key, handle.clone());
        self.by_handle.insert(
            handle.clone(),
            Reservation {
                scope: request.scope,
                request_id: request.request_id,
                entry: std::sync::Arc::new(ManualWorkerEntry::prepared()),
            },
        );
        ManualRunHandle { token: handle }
    }

    pub(super) fn get(&mut self, handle: &ManualRunHandle) -> Option<ReservationView> {
        self.prune_expired();
        self.by_handle.get(&handle.token).map(|reservation| ReservationView {
            scope: reservation.scope.clone(),
            request_id: reservation.request_id.clone(),
            entry: reservation.entry.clone(),
        })
    }

    pub(super) fn acknowledge(&mut self, handle: &ManualRunHandle) -> bool {
        self.prune_expired();
        let Some(reservation) = self.by_handle.get(&handle.token) else {
            return false;
        };
        if !matches!(reservation.entry.state(), ManualRunState::Terminal { .. }) {
            return false;
        }
        let reservation = self.by_handle.remove(&handle.token).unwrap();
        self.by_request.remove(&(reservation.scope, reservation.request_id));
        true
    }

    pub(super) fn request_stop_all(&mut self) {
        for reservation in self.by_handle.values() {
            reservation.entry.request_stop();
        }
    }

    fn prune_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .by_handle
            .iter()
            .filter_map(|(handle, reservation)| reservation.entry.expired(now).then(|| handle.clone()))
            .collect();
        for handle in expired {
            if let Some(reservation) = self.by_handle.remove(&handle) {
                self.by_request
                    .remove(&(reservation.scope, reservation.request_id));
            }
        }
    }
}

pub(super) struct ReservationView {
    pub(super) scope: String,
    pub(super) request_id: String,
    pub(super) entry: std::sync::Arc<ManualWorkerEntry>,
}

#[cfg(test)]
#[path = "schedule_manual_workers_tests.rs"]
mod tests;
