//! Ports for persistence, credentials, and one outbound inference attempt.

use async_trait::async_trait;
use slg_domain::{
    AttemptId, CredentialReference, InferenceRequest, ProviderFailure, RouteCandidate,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AttemptRecord {
    /// Stable across primary-persistence retries and spool reconciliation.
    pub attempt_id: AttemptId,
    pub request_id: String,
    pub route_id: String,
    pub outcome: String,
    pub failure_category: Option<String>,
}

/// Durable, local-only fallback for immutable usage-attempt evidence.
///
/// Implementations must retain only [`AttemptRecord`] data. Prompt bodies,
/// response bodies, and credentials are deliberately absent from this port.
#[async_trait]
pub trait UsageSpool: Send + Sync {
    /// Queues an attempt after the primary usage persistence operation fails.
    ///
    /// A successful return means the local spool durably accepted the attempt;
    /// it does not imply that the primary control plane has recovered.
    async fn append_attempt(&self, attempt: AttemptRecord) -> Result<(), String>;
}

#[async_trait]
pub trait ConfigurationRepository: Send + Sync {
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String>;
    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String>;
    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String>;
    async fn mark_route_success(&self, route_id: &str) -> Result<(), String>;
    async fn mark_route_failure(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String>;
    async fn mark_account_failure(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String>;
}

#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolves a credential only after its reference has passed domain
    /// validation. Implementations must never accept a raw secret value.
    async fn resolve(&self, reference: &CredentialReference) -> Result<String, String>;
}

#[async_trait]
pub trait InferenceExecutor: Send + Sync {
    async fn execute(
        &self,
        route: &RouteCandidate,
        request: &InferenceRequest,
        credential: &str,
    ) -> Result<serde_json::Value, ProviderFailure>;
}
