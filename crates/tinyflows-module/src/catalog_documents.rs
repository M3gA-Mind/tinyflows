//! Scoped, allowlisted bridge from the catalog driver to the host document port.

use async_trait::async_trait;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use tinybus::Connection;
use tinyflows_bus::catalog::{
    CallbackAddress, CatalogStorageReply, DocAtomicBatchRequest, DocCapabilities,
    DocCapabilitiesRequest, DocCountRequest, DocDeleteRequest, DocDeleteWhereRequest,
    DocEnsureRequest, DocGetRequest, DocPutRequest, DocQueryRequest, STORAGE_INTERFACE, storage,
};
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query,
    Result, Sort, StorageError, Version, Versioned, WriteOp, WriteResult,
};

pub(super) struct Documents {
    connection: Connection,
    address: CallbackAddress,
    capabilities: Capabilities,
}

impl std::fmt::Debug for Documents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDocuments").finish_non_exhaustive()
    }
}

impl CatalogStorageReply {
    fn into_error(self) -> StorageError {
        let Self::Error { error } = self else {
            unreachable!("only error replies convert to a storage error")
        };
        match error.kind.as_str() {
            "conflict" => StorageError::conflict(error.message),
            "already_exists" => StorageError::already_exists(error.message),
            "not_found" => StorageError::not_found(error.message),
            "invalid_input" => StorageError::invalid_input(error.message),
            "serialization" => StorageError::new(
                tinystoragedrivers_core::ErrorKind::Serialization,
                error.message,
            ),
            "unavailable" => StorageError::unavailable(error.message),
            _ => StorageError::backend(error.message),
        }
    }
}

impl Documents {
    pub(super) async fn new(
        connection: Connection,
        address: CallbackAddress,
    ) -> anyhow::Result<Self> {
        connection.proxy(
            &address.destination,
            &address.object_path,
            STORAGE_INTERFACE,
        )?;
        let mut documents = Self {
            connection,
            address,
            capabilities: Capabilities::none(),
        };
        let report: DocCapabilities = documents
            .request(storage::CAPABILITIES, DocCapabilitiesRequest {})
            .await?;
        if report.transactions {
            documents.capabilities = documents.capabilities.with(Capability::Transactions);
        }
        Ok(documents)
    }

    fn validate_collection(collection: &str) -> Result<()> {
        if ![
            "flows_definitions",
            "flows_revisions",
            "flows_runs",
            "flows_kv",
            "flows_suggestions",
            "flows_drafts",
        ]
        .contains(&collection)
        {
            return Err(StorageError::invalid_input(
                "collection is outside the workflow catalog port",
            ));
        }
        Ok(())
    }

    async fn call<R: DeserializeOwned, A: serde::Serialize>(
        &self,
        member: &str,
        request: A,
    ) -> Result<R> {
        let request = serde_json::to_value(request)?;
        if let Some(collection) = request.get("collection").and_then(Value::as_str) {
            Self::validate_collection(collection)?;
        }
        self.request(member, request).await
    }

    async fn request<R: DeserializeOwned, A: serde::Serialize>(
        &self,
        member: &str,
        request: A,
    ) -> Result<R> {
        let proxy = self
            .connection
            .proxy(
                &self.address.destination,
                &self.address.object_path,
                STORAGE_INTERFACE,
            )
            .map_err(|error| StorageError::backend(error.to_string()))?;
        let reply: CatalogStorageReply = proxy
            .call(member, &(request,))
            .await
            .map_err(|error| StorageError::unavailable(error.to_string()))?;
        match reply {
            CatalogStorageReply::Error { .. } => Err(reply.into_error()),
            CatalogStorageReply::Value { value } => Ok(serde_json::from_value(value)?),
        }
    }
}

#[async_trait]
impl DocumentStore for Documents {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.capabilities.require(Capability::Transactions)?;
        for operation in &ops {
            let collection = match operation {
                WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. } => collection,
            };
            Self::validate_collection(collection)?;
        }
        self.request(storage::ATOMIC_BATCH, DocAtomicBatchRequest { ops })
            .await
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        Self::validate_collection(&spec.name)?;
        self.call(
            storage::ENSURE,
            DocEnsureRequest {
                spec: serde_json::to_value(spec)?,
            },
        )
        .await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.call(
            storage::GET,
            DocGetRequest {
                collection: collection.into(),
                id: id.into(),
            },
        )
        .await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.call(
            storage::PUT,
            DocPutRequest {
                collection: collection.into(),
                id: id.into(),
                doc,
                precondition: serde_json::to_value(precondition)?,
            },
        )
        .await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        self.call(
            storage::DELETE,
            DocDeleteRequest {
                collection: collection.into(),
                id: id.into(),
                precondition: serde_json::to_value(precondition)?,
            },
        )
        .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        self.call(
            storage::QUERY,
            DocQueryRequest {
                collection: collection.into(),
                query: serde_json::to_value(query)?,
            },
        )
        .await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        self.call(
            storage::COUNT,
            DocCountRequest {
                collection: collection.into(),
                filter: serde_json::to_value(filter)?,
            },
        )
        .await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        self.call(
            storage::DELETE_WHERE,
            DocDeleteWhereRequest {
                collection: collection.into(),
                filter: serde_json::to_value(filter)?,
            },
        )
        .await
    }

    async fn claim(
        &self,
        _collection: &str,
        _filter: &Filter,
        _sort: &[Sort],
        _patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Err(StorageError::backend(
            "claim is outside the catalog document port",
        ))
    }

    async fn drop_collection(&self, _collection: &str) -> Result<()> {
        Err(StorageError::backend(
            "drop_collection is outside the catalog document port",
        ))
    }
}
