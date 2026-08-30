//! Ports for persistence, credentials, and one outbound inference attempt.

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use slg_domain::{
    AttemptId, CredentialReference, InferenceRequest, ProviderAuthoritativeEvidence,
    ProviderBillingRecord, ProviderFailure, ProviderQuotaSnapshot, RouteCandidate,
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

/// Persistence and inspection boundary for provider-authoritative accounting.
///
/// Implementations store only facts supplied by a provider capability. This
/// port deliberately offers no method for estimating quota, price, balance, or
/// eligibility; routing policy belongs to the application layer.
#[async_trait]
pub trait AuthoritativeAccountingRepository: Send + Sync {
    /// Appends one immutable quota/plan/balance/usage observation.
    ///
    /// Repeated delivery of the same `snapshot_id` is idempotent.
    async fn record_quota_snapshot(&self, snapshot: ProviderQuotaSnapshot) -> Result<(), String>;

    /// Appends one immutable provider-reported billing record.
    ///
    /// Repeated delivery of the same `record_id` is idempotent. Individual
    /// billable units and currency remain provider-reported facts.
    async fn record_billing_record(&self, record: ProviderBillingRecord) -> Result<(), String>;

    /// Returns authoritative quota evidence for safe operator inspection.
    async fn quota_snapshots(
        &self,
        provider_account_id: &str,
    ) -> Result<Vec<ProviderQuotaSnapshot>, String>;

    /// Returns provider-reported billing evidence for one gateway attempt.
    async fn billing_records(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Vec<ProviderBillingRecord>, String>;
}

#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Resolves a credential only after its reference has passed domain
    /// validation. Implementations must never accept a raw secret value.
    async fn resolve(&self, reference: &CredentialReference) -> Result<String, String>;
}

/// A provider execution result preserving the exact client response plus any
/// optional facts explicitly supplied by that provider.
///
/// Evidence is deliberately separate from `response`: inbound adapters return
/// only `response` to the client, while the application may persist validated
/// authoritative facts for operator inspection.
pub type InferenceStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, InferenceStreamError>> + Send + 'static>>;

/// A sanitized terminal failure after an upstream streaming response committed.
///
/// The selected route owns the response after commitment. Consumers terminate
/// that response on this error and must never replay the request on another
/// route.
#[derive(Debug, thiserror::Error)]
#[error("committed upstream stream ended unexpectedly")]
pub struct InferenceStreamError;

/// One outbound attempt, separated by its response commitment semantics.
pub enum InferenceExecution {
    Complete {
        response: serde_json::Value,
        authoritative_evidence: Option<Box<ProviderAuthoritativeEvidence>>,
    },
    /// Successful upstream headers have committed this attempt. Errors yielded
    /// by `body` are post-commit and therefore never eligible for fallback.
    Streaming { body: InferenceStream },
}

impl InferenceExecution {
    #[must_use]
    pub const fn without_evidence(response: serde_json::Value) -> Self {
        Self::Complete {
            response,
            authoritative_evidence: None,
        }
    }

    #[must_use]
    pub fn streaming(body: InferenceStream) -> Self {
        Self::Streaming { body }
    }
}

#[async_trait]
pub trait InferenceExecutor: Send + Sync {
    async fn execute(
        &self,
        route: &RouteCandidate,
        request: &InferenceRequest,
        credential: &str,
    ) -> Result<InferenceExecution, ProviderFailure>;
}
