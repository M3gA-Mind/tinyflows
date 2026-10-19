//! Compiled-module entry points for host-neutral preparation helpers.

use serde_json::Value;
use tinybus::{Connection, Error, Result};

struct Portability;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Portability")]
impl Portability {
    /// Return a method/URL summary and optional credential name before the
    /// host resolves any secret. The request may contain inline values; headers
    /// and body are omitted from the result, but the URL may contain sensitive
    /// data.
    async fn inspect_http_request(
        &self,
        request: tinyflows_bus::portability::InspectHttpRequest,
    ) -> Result<tinyflows_bus::portability::HttpRequestDetails> {
        let credential_name = match request.connection_ref.as_deref() {
            Some(connection_ref)
                if connection_ref
                    .starts_with(tinyflows::caps::http_credential::HTTP_CRED_PREFIX) =>
            {
                tinyflows::caps::http_credential::http_cred_name(Some(connection_ref))
                    .map_err(|_| Error::failed("HTTP request inspection failed"))?
                    .map(str::to_owned)
            }
            _ => None,
        };
        Ok(tinyflows_bus::portability::HttpRequestDetails {
            credential_name,
            redacted_summary: tinyflows::caps::http_credential::redacted_summary(&request.request),
        })
    }

    /// Merge a host-resolved credential into a request, failing closed on bad
    /// request/header shapes. The credential value is never included in errors.
    async fn prepare_http_request(
        &self,
        request: tinyflows_bus::portability::PrepareHttpRequest,
    ) -> Result<Value> {
        let credential = match request.credential {
            tinyflows_bus::portability::HttpCredentialTemplate::Bearer { secret } => {
                tinyflows::caps::HttpCredentialTemplate::Bearer { secret }
            }
            tinyflows_bus::portability::HttpCredentialTemplate::Basic { username, secret } => {
                tinyflows::caps::HttpCredentialTemplate::Basic { username, secret }
            }
            tinyflows_bus::portability::HttpCredentialTemplate::Header { header, secret } => {
                tinyflows::caps::HttpCredentialTemplate::Header { header, secret }
            }
            tinyflows_bus::portability::HttpCredentialTemplate::Resolved { header, value } => {
                tinyflows::caps::HttpCredentialTemplate::Resolved { header, value }
            }
        };
        let credential = credential
            .render()
            .map_err(|_| Error::failed("HTTP credential preparation failed"))?;
        tinyflows::caps::inject_credential(request.request, &credential)
            .map_err(|_| Error::failed("HTTP request preparation failed"))
    }

    /// Return only normalized trigger data used by host dispatch bindings.
    async fn describe_trigger(
        &self,
        request: tinyflows_bus::portability::DescribeTrigger,
    ) -> Result<tinyflows_bus::portability::TriggerDetails> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph)
            .map_err(|_| Error::failed("invalid workflow graph"))?;
        let Some(trigger) = graph.trigger() else {
            return Ok(tinyflows_bus::portability::TriggerDetails {
                kind: None,
                schedule: None,
            });
        };
        let Some(kind) = tinyflows::triggers::graph_trigger_kind(&graph) else {
            return Ok(tinyflows_bus::portability::TriggerDetails {
                kind: None,
                schedule: None,
            });
        };
        let kind_label = tinyflows::triggers::trigger_kind_label(&kind).to_owned();
        let schedule = if matches!(kind, tinyflows::model::TriggerKind::Schedule) {
            let raw_schedule = trigger
                .config
                .get("schedule")
                .cloned()
                .ok_or_else(|| Error::failed("schedule trigger is missing its schedule"))?;
            Some(
                serde_json::from_value(raw_schedule)
                    .map_err(|_| Error::failed("schedule trigger has an invalid schedule"))?,
            )
        } else {
            None
        };
        Ok(tinyflows_bus::portability::TriggerDetails {
            kind: Some(kind_label),
            schedule,
        })
    }

    /// Match a graph's sole app-event trigger.
    async fn app_event_binding(
        &self,
        request: tinyflows_bus::portability::AppEventBinding,
    ) -> Result<bool> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph)
            .map_err(|_| Error::failed("invalid workflow graph"))?;
        Ok(tinyflows::triggers::app_event_binding(
            &graph,
            &request.toolkit,
            &request.trigger_slug,
        ))
    }

    /// Return literal inputs pinned on a graph's sole trigger.
    async fn pinned_inputs(
        &self,
        request: tinyflows_bus::portability::PinnedInputs,
    ) -> Result<Value> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph)
            .map_err(|_| Error::failed("invalid workflow graph"))?;
        serde_json::to_value(tinyflows::triggers::pinned_inputs(&graph))
            .map_err(|_| Error::failed("pinned trigger inputs could not be encoded"))
    }
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(tinyflows_bus::OBJECT_PATH.try_into()?, Portability)
        .await
}

#[cfg(test)]
#[path = "portability_service_tests.rs"]
mod tests;
