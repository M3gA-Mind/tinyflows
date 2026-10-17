//! Portable HTTP credential preparation shared by module and host adapters.

use serde_json::Value;

use crate::error::{EngineError, Result};

/// Portable credential forms rendered before the HTTP transport is called.
pub enum HttpCredentialTemplate {
    /// `Authorization: Bearer <secret>`.
    Bearer {
        /// Secret token.
        secret: String,
    },
    /// `Authorization: Basic <base64(username:secret)>`.
    Basic {
        /// Basic authentication username.
        username: String,
        /// Basic authentication password.
        secret: String,
    },
    /// A secret written to a caller-selected header.
    Header {
        /// Header name.
        header: String,
        /// Secret header value.
        secret: String,
    },
    /// A host-resolved header value that must pass through unchanged.
    Resolved {
        /// Header name.
        header: String,
        /// Fully resolved header value.
        value: String,
    },
}

impl HttpCredentialTemplate {
    /// Render the template without including secret material in failures.
    pub fn render(self) -> Result<HttpCredential> {
        use base64::Engine as _;
        let (header, value) = match self {
            Self::Bearer { secret } => {
                if secret.trim().is_empty() {
                    return Err(EngineError::Capability(
                        "http_request: bearer credential is empty".into(),
                    ));
                }
                ("Authorization".into(), format!("Bearer {secret}"))
            }
            Self::Basic { username, secret } => (
                "Authorization".into(),
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD
                        .encode(format!("{username}:{secret}"))
                ),
            ),
            Self::Header { header, secret } => {
                if header.trim().is_empty() {
                    return Err(EngineError::Capability(
                        "http_request: custom credential header is empty".into(),
                    ));
                }
                (header, secret)
            }
            Self::Resolved { header, value } => (header, value),
        };
        Ok(HttpCredential { header, value })
    }
}

/// Header and fully resolved value supplied by the trusted host.
#[derive(Debug, Clone)]
pub struct HttpCredential {
    /// Header name to set.
    pub header: String,
    /// Opaque header value; never log or return it in diagnostics.
    pub value: String,
}

/// Inject a trusted credential into a request, preserving fail-closed shape
/// validation and the host credential's precedence over inline headers.
pub fn inject_credential(mut request: Value, cred: &HttpCredential) -> Result<Value> {
    let object = request
        .as_object_mut()
        .ok_or_else(|| EngineError::Capability("http_request: request must be an object".into()))?;
    let headers = object
        .entry("headers")
        .or_insert_with(|| Value::Object(Default::default()));
    if headers.is_null() {
        *headers = Value::Object(Default::default());
    }
    let headers = headers.as_object_mut().ok_or_else(|| {
        EngineError::Capability("http_request: headers must be an object or null".into())
    })?;
    headers.retain(|name, _| !name.eq_ignore_ascii_case(&cred.header));
    headers.insert(cred.header.clone(), Value::String(cred.value.clone()));
    Ok(request)
}
