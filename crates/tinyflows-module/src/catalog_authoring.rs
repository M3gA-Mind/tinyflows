//! Pure graph authoring operations exposed on the Catalog interface.

use serde_json::Value;
use tinybus::{Connection, Error};
use tinyflows_bus::catalog::{
    self, ApplyGraphOpsError, ApplyGraphOpsRequest, ApplyGraphOpsResult, ApprovalManifestRequest,
    ClassifyManifestCandidatesRequest,
};

pub(super) fn node_kinds() -> Vec<tinyflows_bus::node_kinds::NodeKindContract> {
    tinyflows_bus::node_kinds::all_contracts()
}

/// Apply structural edits in order to a private graph copy.
pub(super) fn apply_graph_ops(request: ApplyGraphOpsRequest) -> ApplyGraphOpsResult {
    let mut graph = match tinyflows::migrate::deserialize_graph(request.graph) {
        Ok(graph) => graph,
        Err(details) => {
            return ApplyGraphOpsResult {
                graph: None,
                error: Some(ApplyGraphOpsError {
                    index: 0,
                    op: "decode_graph".into(),
                    details,
                }),
            };
        }
    };
    for (index, raw) in request.ops.into_iter().enumerate() {
        match serde_json::from_value::<tinyflows::graph_ops::GraphOp>(raw) {
            Ok(op) => match tinyflows::graph_ops::apply_ops(&graph, &[op]) {
                Ok(updated) => graph = updated,
                Err(error) => {
                    return ApplyGraphOpsResult {
                        graph: None,
                        error: Some(ApplyGraphOpsError {
                            index,
                            op: error.op.into(),
                            details: error.kind.to_string(),
                        }),
                    };
                }
            },
            Err(error) => {
                return ApplyGraphOpsResult {
                    graph: None,
                    error: Some(ApplyGraphOpsError {
                        index,
                        op: "decode_operation".into(),
                        details: error.to_string(),
                    }),
                };
            }
        }
    }
    ApplyGraphOpsResult {
        graph: serde_json::to_value(graph).ok(),
        error: None,
    }
}

/// Classify graph-derived candidates with the scope-bound host, then assemble
/// the portable approval manifest. Missing or malformed policy replies fail closed.
pub(super) async fn approval_manifest(
    connection: &Connection,
    request: ApprovalManifestRequest,
) -> tinybus::Result<Vec<Value>> {
    let graph = tinyflows::migrate::deserialize_graph(request.graph).map_err(Error::failed)?;
    let candidates =
        tinyflows_catalog::approval_manifest::manifest_candidates(&graph, &request.native_prefix);
    let wire_candidates = candidates.iter().map(wire_candidate).collect::<Vec<_>>();
    let callback = connection.proxy(
        &request.host.destination,
        &request.host.object_path,
        catalog::APPROVAL_MANIFEST_HOST_INTERFACE,
    )?;
    let classes: catalog::ClassifyManifestCandidatesResponse = callback
        .call(
            catalog::CLASSIFY_MANIFEST_CANDIDATES,
            (ClassifyManifestCandidatesRequest {
                candidates: wire_candidates.clone(),
            },),
        )
        .await?;
    if classes.len() != candidates.len() {
        return Err(Error::failed(format!(
            "manifest classifier returned {} classifications for {} candidates",
            classes.len(),
            candidates.len()
        )));
    }
    for (index, (candidate, class)) in wire_candidates.iter().zip(&classes).enumerate() {
        let requires_class = matches!(
            candidate,
            catalog::ManifestCandidate::Http { .. }
                | catalog::ManifestCandidate::Code { .. }
                | catalog::ManifestCandidate::NativeTool { .. }
                | catalog::ManifestCandidate::ConnectorTool { .. }
        );
        if requires_class != class.is_some() {
            return Err(Error::failed(format!(
                "manifest classifier returned malformed classification at candidate {index}"
            )));
        }
    }
    let classes = classes
        .into_iter()
        .map(|class| {
            class.map(
                |class| tinyflows_catalog::approval_manifest::ManifestClass {
                    name: class.name,
                    blocked: class.blocked,
                    skip: class.skip,
                },
            )
        })
        .collect::<Vec<_>>();
    Ok(tinyflows_catalog::approval_manifest::assemble_manifest(
        &candidates,
        &classes,
    ))
}

fn wire_candidate(
    candidate: &tinyflows_catalog::approval_manifest::ManifestCandidate,
) -> catalog::ManifestCandidate {
    use tinyflows_catalog::approval_manifest::ManifestCandidate as Candidate;
    match candidate {
        Candidate::Http { node_id, url } => catalog::ManifestCandidate::Http {
            node_id: node_id.clone(),
            url: url.clone(),
        },
        Candidate::Code { node_id } => catalog::ManifestCandidate::Code {
            node_id: node_id.clone(),
        },
        Candidate::DynamicTool { node_id } => catalog::ManifestCandidate::DynamicTool {
            node_id: node_id.clone(),
        },
        Candidate::NativeTool {
            node_id,
            tool_name,
            args,
        } => catalog::ManifestCandidate::NativeTool {
            node_id: node_id.clone(),
            tool_name: tool_name.clone(),
            args: args.clone(),
        },
        Candidate::ConnectorTool { node_id, slug } => catalog::ManifestCandidate::ConnectorTool {
            node_id: node_id.clone(),
            slug: slug.clone(),
        },
        Candidate::Agent { node_id } => catalog::ManifestCandidate::Agent {
            node_id: node_id.clone(),
        },
    }
}
