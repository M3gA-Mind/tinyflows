//! TinyBus entry points for module-owned workflow run lifecycle.

use tinybus::{Connection, Error, Result};
use tinyflows_bus::runs::{self, CancelRunResponse, PollRun, RunControlRequest, StartRun};

struct Runs {
    connection: Connection,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Runs")]
impl Runs {
    /// Validate and reserve a run before acknowledging it to the host.
    async fn start_run(&self, request: StartRun) -> Result<runs::StartRunResponse> {
        super::run_manager::start(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Read persisted run events after a monotonically increasing cursor.
    async fn poll_run(&self, request: RunControlRequest) -> Result<PollRun> {
        super::run_manager::poll(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Signal a live run or atomically cancel a parked run.
    async fn cancel_run(&self, request: RunControlRequest) -> Result<CancelRunResponse> {
        super::run_manager::cancel(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(
            runs::OBJECT_PATH.try_into()?,
            Runs {
                connection: connection.clone(),
            },
        )
        .await
}
