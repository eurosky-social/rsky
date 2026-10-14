use crate::common::encode_uri_component;
use crate::types::DidCache;
use anyhow::{bail, Result};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DidPlcResolver {
    pub plc_url: String,
    pub timeout: Duration,
    pub cache: Option<Arc<dyn DidCache>>,
    /// Built once and shared by every clone (reqwest clients are reference
    /// counted), so lookups reuse pooled connections to the directory. A
    /// client per lookup reloaded TLS roots and opened a fresh TCP+TLS
    /// connection every time; at a few hundred lookups in flight that storm
    /// of new connections is refused by the directory's front end.
    client: reqwest::Client,
}

impl DidPlcResolver {
    pub fn new(plc_url: String, timeout: Duration, cache: Option<Arc<dyn DidCache>>) -> Self {
        Self::with_dns(plc_url, timeout, cache, crate::dns::shared())
    }

    /// Resolves the directory's host name through `dns`. The default,
    /// [`crate::dns::shared`], is the one cache for the whole process: a
    /// client's own (reqwest's per-client hickory) starts cold, and every
    /// `IdResolver` built per event then cost a query per connection.
    pub fn with_dns(
        plc_url: String,
        timeout: Duration,
        cache: Option<Arc<dyn DidCache>>,
        dns: Arc<crate::dns::CachingResolver>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .dns_resolver(Arc::new(crate::dns::Handle(dns)))
            .build()
            .unwrap_or_default();
        Self {
            plc_url,
            timeout,
            cache,
            client,
        }
    }

    pub async fn resolve_no_check(&self, did: String) -> Result<Option<Value>> {
        let response = self
            .client
            .get(format!("{0}/{1}", self.plc_url, encode_uri_component(&did)))
            .timeout(self.timeout)
            .send()
            .await?;
        let res = &response;
        match res.error_for_status_ref() {
            Ok(_) => Ok(Some(response.json::<Value>().await?)),
            // Positively not found, versus due to e.g. network error
            Err(error) if error.status() == Some(reqwest::StatusCode::NOT_FOUND) => Ok(None),
            Err(error) => bail!(error.to_string()),
        }
    }
}
