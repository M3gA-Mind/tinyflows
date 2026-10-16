//! Serde-only requests and replies for the host's CatalogStorage callback.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request body for `DocEnsure`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocEnsureRequest {
    /// Collection and index declaration, encoded in the storage driver's JSON shape.
    pub spec: Value,
}

/// Request body for `DocGet`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocGetRequest {
    /// Allowlisted catalog collection.
    pub collection: String,
    /// Document id.
    pub id: String,
}

/// Request body for `DocPut`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocPutRequest {
    /// Allowlisted catalog collection.
    pub collection: String,
    /// Document id.
    pub id: String,
    /// Document content.
    pub doc: Value,
    /// Storage-driver precondition in its stable JSON shape.
    pub precondition: Value,
}

/// Request body for `DocDelete`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocDeleteRequest {
    /// Allowlisted catalog collection.
    pub collection: String,
    /// Document id.
    pub id: String,
    /// Storage-driver precondition in its stable JSON shape.
    pub precondition: Value,
}

/// Request body for `DocQuery`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocQueryRequest {
    /// Allowlisted catalog collection.
    pub collection: String,
    /// Filter, sort and pagination in the storage driver's JSON shape.
    pub query: Value,
}

/// Request body for `DocDeleteWhere`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DocDeleteWhereRequest {
    /// Allowlisted catalog collection.
    pub collection: String,
    /// Filter in the storage driver's JSON shape.
    pub filter: Value,
}

/// Error returned by a storage callback; `kind` is a stable storage error tag.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogStorageFailure {
    /// One of conflict, already_exists, not_found, invalid_input,
    /// serialization, unavailable, or backend.
    pub kind: String,
    /// Safe error description without the document value that caused it.
    pub message: String,
}

/// Standard storage reply envelope for a value or a structured failure.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CatalogStorageReply {
    /// Successful document-driver result in the driver's JSON shape.
    Value { value: Value },
    /// Failure without the value that caused it.
    Error { error: CatalogStorageFailure },
}
