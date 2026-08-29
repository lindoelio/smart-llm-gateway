//! Use cases that coordinate the pure router with external ports.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use slg_domain::{
    AttemptId, InferenceRequest, ProviderAuthoritativeEvidence, ProviderBillingRecord,
};
use slg_ports::{
    AttemptRecord, AuthoritativeAccountingRepository, ConfigurationRepository, InferenceExecutor,
    SecretResolver, UsageSpool,
};

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("authentication failed")]
    Unauthorized,
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("streaming is not supported by this gateway version")]
    StreamingUnsupported,
    #[error("no upstream route is currently available")]
    UpstreamUnavailable,
    #[error("no eligible route succeeded: {0}")]
    Upstream(String),
}

pub struct Gateway<C, E, S> {
    configuration: C,
    executor: E,
    secrets: S,
    usage_spool: Option<Arc<dyn UsageSpool>>,
    accounting: Option<Arc<dyn AuthoritativeAccountingRepository>>,
}

impl<C, E, S> Gateway<C, E, S> {
    #[must_use]
    pub const fn new(configuration: C, executor: E, secrets: S) -> Self {
        Self {
            configuration,
            executor,
            secrets,
            usage_spool: None,
            accounting: None,
        }
    }

    /// Enables durable local usage fallback for primary usage-store failures.
    ///
    /// This does not make control-plane failures available: authentication,
    /// candidate lookup, and circuit-state writes continue to fail explicitly.
    #[must_use]
    pub fn with_usage_spool<T>(mut self, usage_spool: T) -> Self
    where
        T: UsageSpool + 'static,
    {
        self.usage_spool = Some(Arc::new(usage_spool));
        self
    }

    /// Enables best-effort storage of facts explicitly returned by a provider.
    ///
    /// Accounting persistence is observability only. Its availability and its
    /// contents never influence route selection, fallback, or client output.
    #[must_use]
    pub fn with_accounting_repository<T>(mut self, accounting: T) -> Self
    where
        T: AuthoritativeAccountingRepository + 'static,
    {
        self.accounting = Some(Arc::new(accounting));
        self
    }
}

impl<C, E, S> Gateway<C, E, S>
where
    C: ConfigurationRepository,
    E: InferenceExecutor,
    S: SecretResolver,
{
    async fn persist_attempt(&self, attempt: AttemptRecord) -> Result<(), ApplicationError> {
        if self
            .configuration
            .record_attempt(attempt.clone())
            .await
            .is_ok()
        {
            return Ok(());
        }

        let Some(usage_spool) = &self.usage_spool else {
            return Err(ApplicationError::Configuration(
                "control plane unavailable while persisting usage attempt; no durable local spool is configured"
                    .into(),
            ));
        };
        usage_spool.append_attempt(attempt).await.map_err(|_| {
            ApplicationError::Configuration(
                "control plane unavailable while persisting usage attempt; durable local spool is unavailable"
                    .into(),
            )
        })
    }

    async fn persist_authoritative_evidence(
        &self,
        provider_account_id: &str,
        attempt_id: &AttemptId,
        evidence: ProviderAuthoritativeEvidence,
    ) {
        let Some(accounting) = &self.accounting else {
            return;
        };
        if evidence.is_empty() || evidence.validate_for_account(provider_account_id).is_err() {
            return;
        }
        for snapshot in evidence.quota_snapshots {
            let _ = accounting.record_quota_snapshot(snapshot).await;
        }
        let Some(billing) = evidence.billing else {
            return;
        };
        let observed_at_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
            });
        let _ = accounting
            .record_billing_record(ProviderBillingRecord {
                record_id: attempt_id.to_string(),
                attempt_id: attempt_id.clone(),
                provider_account_id: provider_account_id.into(),
                provider_request_id: billing.provider_request_id,
                billed_units: billing.billed_units,
                charge: billing.charge,
                observed_at_unix,
                // A usage observation reports no renewable capacity window.
                // Its freshness is the exact observation instant, not an
                // estimate of future quota or balance.
                fresh_until_unix: observed_at_unix,
                source: billing.source,
            })
            .await;
    }

    pub async fn infer(
        &self,
        raw_key: &str,
        request: InferenceRequest,
    ) -> Result<serde_json::Value, ApplicationError> {
        if request.stream {
            return Err(ApplicationError::StreamingUnsupported);
        }
        if !self
            .configuration
            .authenticate(raw_key)
            .await
            .map_err(ApplicationError::Configuration)?
        {
            return Err(ApplicationError::Unauthorized);
        }
        let candidates = self
            .configuration
            .candidates(&request.model)
            .await
            .map_err(|error| {
                if route_unavailable(&error) {
                    ApplicationError::UpstreamUnavailable
                } else {
                    ApplicationError::Configuration(error)
                }
            })?;
        let mut last_failure = None;
        for route in candidates {
            let credential = match self.secrets.resolve(&route.credential_ref).await {
                Ok(value) => value,
                Err(error) => {
                    last_failure = Some(error);
                    continue;
                }
            };
            match self.executor.execute(&route, &request, &credential).await {
                Ok(execution) => {
                    let attempt_id = AttemptId::new();
                    self.persist_attempt(AttemptRecord {
                        attempt_id: attempt_id.clone(),
                        request_id: request.request_id.to_string(),
                        route_id: route.route_id.clone(),
                        outcome: "succeeded".into(),
                        failure_category: None,
                    })
                    .await?;
                    if let Some(evidence) = execution.authoritative_evidence {
                        self.persist_authoritative_evidence(
                            &route.provider_account_id,
                            &attempt_id,
                            evidence,
                        )
                        .await;
                    }
                    self.configuration
                        .mark_route_success(&route.route_id)
                        .await
                        .map_err(ApplicationError::Configuration)?;
                    return Ok(execution.response);
                }
                Err(failure) => {
                    let category = format!("{:?}", failure.category);
                    self.persist_attempt(AttemptRecord {
                        attempt_id: AttemptId::new(),
                        request_id: request.request_id.to_string(),
                        route_id: route.route_id.clone(),
                        outcome: "failed".into(),
                        failure_category: Some(category),
                    })
                    .await?;
                    if failure.category.blocks_account() {
                        self.configuration
                            .mark_account_failure(&route.provider_account_id, &failure)
                            .await
                            .map_err(ApplicationError::Configuration)?;
                    }
                    if failure.category.opens_route() {
                        self.configuration
                            .mark_route_failure(&route.route_id, &failure)
                            .await
                            .map_err(ApplicationError::Configuration)?;
                    }
                    last_failure = Some(failure.message);
                }
            }
        }
        Err(ApplicationError::Upstream(
            last_failure.unwrap_or_else(|| "no eligible route".into()),
        ))
    }
}

fn route_unavailable(error: &str) -> bool {
    error.starts_with("no eligible route exists for")
        // Storage adapters filter open routes before candidate planning. Their
        // current repository contract consequently reports this case as an
        // unknown model; expose availability, not a control-plane 400.
        || error.starts_with("requested logical model")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use slg_domain::{
        AuthoritativeSource, CredentialReference, ErrorCategory, FixedDecimal,
        ProviderAuthoritativeEvidence, ProviderBillingEvidence, ProviderFailure,
        ProviderReportedQuantity, ProviderUnit, ProviderUnitKind, RouteCandidate,
    };
    use uuid::Uuid;

    use super::*;
    use slg_ports::InferenceExecution;

    #[derive(Clone)]
    struct TestConfiguration {
        candidates: Vec<RouteCandidate>,
        primary_usage_available: bool,
        route_state_available: bool,
    }

    #[async_trait]
    impl ConfigurationRepository for TestConfiguration {
        async fn authenticate(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }

        async fn candidates(&self, _: &str) -> Result<Vec<RouteCandidate>, String> {
            Ok(self.candidates.clone())
        }

        async fn record_attempt(&self, _: AttemptRecord) -> Result<(), String> {
            self.primary_usage_available
                .then_some(())
                .ok_or_else(|| "primary usage store unavailable".into())
        }

        async fn mark_route_success(&self, _: &str) -> Result<(), String> {
            self.route_state_available
                .then_some(())
                .ok_or_else(|| "control plane unavailable".into())
        }

        async fn mark_route_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            Ok(())
        }

        async fn mark_account_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            Ok(())
        }
    }

    struct TestSecrets;

    #[async_trait]
    impl SecretResolver for TestSecrets {
        async fn resolve(&self, _: &CredentialReference) -> Result<String, String> {
            Ok("credential-that-must-not-be-spooled".into())
        }
    }

    struct TestExecutor;

    #[async_trait]
    impl InferenceExecutor for TestExecutor {
        async fn execute(
            &self,
            route: &RouteCandidate,
            _: &InferenceRequest,
            _: &str,
        ) -> Result<InferenceExecution, ProviderFailure> {
            if route.route_id == "failing-route" {
                return Err(ProviderFailure {
                    category: ErrorCategory::ProviderUnavailable,
                    message: "provider unavailable".into(),
                    status: Some(503),
                    retry_at_unix: None,
                });
            }
            Ok(InferenceExecution::without_evidence(
                serde_json::json!({"id": "response"}),
            ))
        }
    }

    struct PanicExecutor;

    #[async_trait]
    impl InferenceExecutor for PanicExecutor {
        async fn execute(
            &self,
            _: &RouteCandidate,
            _: &InferenceRequest,
            _: &str,
        ) -> Result<InferenceExecution, ProviderFailure> {
            panic!("streaming requests must not reach the executor")
        }
    }

    struct UnavailableConfiguration;

    #[async_trait]
    impl ConfigurationRepository for UnavailableConfiguration {
        async fn authenticate(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }

        async fn candidates(&self, _: &str) -> Result<Vec<RouteCandidate>, String> {
            Err("requested logical model `logical-model` is not configured".into())
        }

        async fn record_attempt(&self, _: AttemptRecord) -> Result<(), String> {
            panic!("unavailable routes must not record an attempt")
        }

        async fn mark_route_success(&self, _: &str) -> Result<(), String> {
            panic!("unavailable routes must not alter route state")
        }

        async fn mark_route_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            panic!("unavailable routes must not alter route state")
        }

        async fn mark_account_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            panic!("unavailable routes must not alter account state")
        }
    }

    #[derive(Clone, Default)]
    struct RecordingSpool {
        attempts: Arc<Mutex<Vec<AttemptRecord>>>,
    }

    #[async_trait]
    impl UsageSpool for RecordingSpool {
        async fn append_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
            self.attempts
                .lock()
                .map_err(|_| "test spool mutex poisoned".to_owned())?
                .push(attempt);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingAccounting {
        billing: Arc<Mutex<Vec<ProviderBillingRecord>>>,
        fail_writes: bool,
    }

    #[async_trait]
    impl AuthoritativeAccountingRepository for RecordingAccounting {
        async fn record_quota_snapshot(
            &self,
            _: slg_domain::ProviderQuotaSnapshot,
        ) -> Result<(), String> {
            if self.fail_writes {
                Err("accounting store unavailable".into())
            } else {
                Ok(())
            }
        }

        async fn record_billing_record(&self, record: ProviderBillingRecord) -> Result<(), String> {
            if self.fail_writes {
                return Err("accounting store unavailable".into());
            }
            self.billing
                .lock()
                .map_err(|_| "test accounting mutex poisoned".to_owned())?
                .push(record);
            Ok(())
        }

        async fn quota_snapshots(
            &self,
            _: &str,
        ) -> Result<Vec<slg_domain::ProviderQuotaSnapshot>, String> {
            Ok(Vec::new())
        }

        async fn billing_records(
            &self,
            _: &AttemptId,
        ) -> Result<Vec<ProviderBillingRecord>, String> {
            Ok(Vec::new())
        }
    }

    struct EvidenceExecutor;

    #[async_trait]
    impl InferenceExecutor for EvidenceExecutor {
        async fn execute(
            &self,
            _: &RouteCandidate,
            _: &InferenceRequest,
            _: &str,
        ) -> Result<InferenceExecution, ProviderFailure> {
            Ok(InferenceExecution {
                response: serde_json::json!({"id": "response", "usage": {"total_tokens": 8}}),
                authoritative_evidence: Some(ProviderAuthoritativeEvidence {
                    quota_snapshots: Vec::new(),
                    billing: Some(ProviderBillingEvidence {
                        provider_request_id: None,
                        billed_units: vec![ProviderReportedQuantity {
                            unit: ProviderUnit {
                                kind: ProviderUnitKind::TotalTokens,
                                currency_code: None,
                                custom_name: None,
                            },
                            value: FixedDecimal {
                                unscaled: 8,
                                scale: 0,
                            },
                        }],
                        charge: None,
                        source: AuthoritativeSource {
                            source_id: "test-provider.usage".into(),
                            evidence_version: Some("test-v1".into()),
                        },
                    }),
                }),
            })
        }
    }

    fn route(id: &str) -> RouteCandidate {
        RouteCandidate {
            route_id: id.into(),
            logical_model: "logical-model".into(),
            provider_account_id: "provider-account".into(),
            provider: "provider".into(),
            credential_ref: CredentialReference::parse("env:TEST_CREDENTIAL").unwrap(),
            base_url: "https://provider.example.test".into(),
            upstream_model: "upstream-model".into(),
            priority: 1,
        }
    }

    fn request() -> InferenceRequest {
        InferenceRequest {
            request_id: Uuid::new_v4(),
            model: "logical-model".into(),
            messages: vec![slg_domain::ChatMessage {
                role: "user".into(),
                content: "prompt-that-must-not-be-spooled".into(),
            }],
            stream: false,
            temperature: None,
            max_tokens: None,
        }
    }

    #[tokio::test]
    async fn spools_usage_when_primary_persistence_fails_without_sensitive_request_data() {
        let spool = RecordingSpool::default();
        let gateway = Gateway::new(
            TestConfiguration {
                candidates: vec![route("working-route")],
                primary_usage_available: false,
                route_state_available: true,
            },
            TestExecutor,
            TestSecrets,
        )
        .with_usage_spool(spool.clone());

        assert_eq!(
            gateway.infer("gateway-key", request()).await.unwrap(),
            serde_json::json!({"id": "response"})
        );

        let attempts = spool.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, "succeeded");
        let serialized = serde_json::to_string(&attempts[0]).unwrap();
        assert!(!serialized.contains("prompt-that-must-not-be-spooled"));
        assert!(!serialized.contains("credential-that-must-not-be-spooled"));
    }

    #[tokio::test]
    async fn spools_each_attempt_before_safe_fallback() {
        let spool = RecordingSpool::default();
        let gateway = Gateway::new(
            TestConfiguration {
                candidates: vec![route("failing-route"), route("working-route")],
                primary_usage_available: false,
                route_state_available: true,
            },
            TestExecutor,
            TestSecrets,
        )
        .with_usage_spool(spool.clone());

        gateway.infer("gateway-key", request()).await.unwrap();

        let attempts = spool.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].outcome, "failed");
        assert_eq!(attempts[1].outcome, "succeeded");
        assert_ne!(attempts[0].attempt_id, attempts[1].attempt_id);
    }

    #[tokio::test]
    async fn control_plane_failure_remains_visible_after_usage_is_spooled() {
        let spool = RecordingSpool::default();
        let gateway = Gateway::new(
            TestConfiguration {
                candidates: vec![route("working-route")],
                primary_usage_available: false,
                route_state_available: false,
            },
            TestExecutor,
            TestSecrets,
        )
        .with_usage_spool(spool.clone());

        assert!(matches!(
            gateway.infer("gateway-key", request()).await,
            Err(ApplicationError::Configuration(message)) if message == "control plane unavailable"
        ));
        assert_eq!(spool.attempts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn open_route_is_reported_as_normalized_upstream_unavailability() {
        let gateway = Gateway::new(UnavailableConfiguration, PanicExecutor, TestSecrets);

        assert!(matches!(
            gateway.infer("gateway-key", request()).await,
            Err(ApplicationError::UpstreamUnavailable)
        ));
    }

    #[tokio::test]
    async fn streaming_is_rejected_before_executor_or_control_plane_access() {
        let gateway = Gateway::new(UnavailableConfiguration, PanicExecutor, TestSecrets);
        let mut streaming_request = request();
        streaming_request.stream = true;

        assert!(matches!(
            gateway.infer("gateway-key", streaming_request).await,
            Err(ApplicationError::StreamingUnsupported)
        ));
    }

    #[tokio::test]
    async fn persists_explicit_provider_evidence_without_changing_client_response() {
        let accounting = RecordingAccounting::default();
        let gateway = Gateway::new(
            TestConfiguration {
                candidates: vec![route("working-route")],
                primary_usage_available: true,
                route_state_available: true,
            },
            EvidenceExecutor,
            TestSecrets,
        )
        .with_accounting_repository(accounting.clone());

        assert_eq!(
            gateway.infer("gateway-key", request()).await.unwrap(),
            serde_json::json!({"id": "response", "usage": {"total_tokens": 8}})
        );
        let billing = accounting.billing.lock().unwrap();
        assert_eq!(billing.len(), 1);
        assert_eq!(billing[0].provider_account_id, "provider-account");
        assert_eq!(billing[0].billed_units[0].value.unscaled, 8);
        assert_eq!(billing[0].charge, None);
        assert_eq!(
            billing[0].source.evidence_version.as_deref(),
            Some("test-v1")
        );
    }

    #[tokio::test]
    async fn absent_or_unavailable_accounting_evidence_never_changes_routing() {
        let absent = RecordingAccounting::default();
        let gateway_without_evidence = Gateway::new(
            TestConfiguration {
                candidates: vec![route("working-route")],
                primary_usage_available: true,
                route_state_available: true,
            },
            TestExecutor,
            TestSecrets,
        )
        .with_accounting_repository(absent.clone());
        assert_eq!(
            gateway_without_evidence
                .infer("gateway-key", request())
                .await
                .unwrap(),
            serde_json::json!({"id": "response"})
        );
        assert!(absent.billing.lock().unwrap().is_empty());

        let unavailable = RecordingAccounting {
            fail_writes: true,
            ..RecordingAccounting::default()
        };
        let gateway_with_failed_accounting = Gateway::new(
            TestConfiguration {
                candidates: vec![route("failing-route"), route("working-route")],
                primary_usage_available: true,
                route_state_available: true,
            },
            EvidenceExecutor,
            TestSecrets,
        )
        .with_accounting_repository(unavailable);
        assert_eq!(
            gateway_with_failed_accounting
                .infer("gateway-key", request())
                .await
                .unwrap(),
            serde_json::json!({"id": "response", "usage": {"total_tokens": 8}})
        );
    }
}
