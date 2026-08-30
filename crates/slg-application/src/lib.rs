//! Use cases that coordinate the pure router with external ports.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use slg_domain::{
    AttemptId, InferenceRequest, ProviderAuthoritativeEvidence, ProviderBillingRecord,
    ProviderFailure,
};
use slg_ports::{
    AttemptOutcome, AttemptRecord, AuthoritativeAccountingRepository, ConfigurationRepository,
    InferenceExecution, InferenceExecutor, InferenceStream, InferenceStreamEvent, SecretResolver,
    UsageSpool,
};

#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("authentication failed")]
    Unauthorized,
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("no upstream route is currently available")]
    UpstreamUnavailable,
    #[error("no eligible route succeeded: {0}")]
    Upstream(String),
}

/// A protocol-neutral application response. Streaming bodies are already
/// committed to one upstream route and must be forwarded without replay.
pub enum InferenceResponse {
    Complete(serde_json::Value),
    Streaming(InferenceStream),
}

pub struct Gateway<C, E, S> {
    configuration: Arc<C>,
    executor: Arc<E>,
    secrets: Arc<S>,
    usage_spool: Option<Arc<dyn UsageSpool>>,
    accounting: Option<Arc<dyn AuthoritativeAccountingRepository>>,
}

impl<C, E, S> Gateway<C, E, S> {
    #[must_use]
    pub fn new(configuration: C, executor: E, secrets: S) -> Self {
        Self {
            configuration: Arc::new(configuration),
            executor: Arc::new(executor),
            secrets: Arc::new(secrets),
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

struct StreamAttempt<C> {
    configuration: Arc<C>,
    usage_spool: Option<Arc<dyn UsageSpool>>,
    attempt_id: AttemptId,
    request_id: String,
    route_id: String,
    provider_account_id: String,
}

impl<C> Clone for StreamAttempt<C> {
    fn clone(&self) -> Self {
        Self {
            configuration: self.configuration.clone(),
            usage_spool: self.usage_spool.clone(),
            attempt_id: self.attempt_id.clone(),
            request_id: self.request_id.clone(),
            route_id: self.route_id.clone(),
            provider_account_id: self.provider_account_id.clone(),
        }
    }
}

impl<C> StreamAttempt<C>
where
    C: ConfigurationRepository + 'static,
{
    fn record(&self, outcome: AttemptOutcome, failure_category: Option<String>) -> AttemptRecord {
        AttemptRecord {
            attempt_id: self.attempt_id.clone(),
            request_id: self.request_id.clone(),
            route_id: self.route_id.clone(),
            outcome,
            failure_category,
        }
    }

    async fn persist(&self, record: AttemptRecord) -> Result<(), ()> {
        if self
            .configuration
            .record_attempt(record.clone())
            .await
            .is_ok()
        {
            return Ok(());
        }
        let Some(spool) = &self.usage_spool else {
            return Err(());
        };
        spool.append_attempt(record).await.map_err(|_| ())
    }

    async fn succeed(self) {
        if self
            .persist(self.record(AttemptOutcome::Succeeded, None))
            .await
            .is_ok()
        {
            let _ = self.configuration.mark_route_success(&self.route_id).await;
        }
    }

    async fn fail(self, failure: ProviderFailure) {
        let _ = self
            .persist(self.record(
                AttemptOutcome::PartialFailed,
                Some(format!("{:?}", failure.category)),
            ))
            .await;
        if failure.category.blocks_account() {
            let _ = self
                .configuration
                .mark_account_failure(&self.provider_account_id, &failure)
                .await;
        }
        if failure.category.opens_route() {
            let _ = self
                .configuration
                .mark_route_failure(&self.route_id, &failure)
                .await;
        }
    }

    async fn cancel(self) {
        let _ = self
            .persist(self.record(AttemptOutcome::Cancelled, None))
            .await;
    }
}

type TerminalFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct ManagedInferenceStream<C: ConfigurationRepository + 'static> {
    upstream: InferenceStream,
    attempt: StreamAttempt<C>,
    pending_terminal: Option<(InferenceStreamEvent, TerminalFuture)>,
    terminal_emitted: bool,
}

impl<C: ConfigurationRepository + 'static> Unpin for ManagedInferenceStream<C> {}

impl<C> futures_util::Stream for ManagedInferenceStream<C>
where
    C: ConfigurationRepository + 'static,
{
    type Item = InferenceStreamEvent;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.terminal_emitted {
                return Poll::Ready(None);
            }
            if let Some((_, future)) = self.pending_terminal.as_mut() {
                if future.as_mut().poll(context).is_pending() {
                    return Poll::Pending;
                }
                let (event, _) = self
                    .pending_terminal
                    .take()
                    .expect("terminal future exists");
                self.terminal_emitted = true;
                return Poll::Ready(Some(event));
            }
            match self.upstream.as_mut().poll_next(context) {
                Poll::Ready(Some(event @ InferenceStreamEvent::Frame(_))) => {
                    return Poll::Ready(Some(event));
                }
                Poll::Ready(Some(event @ InferenceStreamEvent::Completed)) => {
                    let attempt = self.attempt.clone();
                    self.pending_terminal = Some((
                        event,
                        Box::pin(async move {
                            attempt.succeed().await;
                        }),
                    ));
                }
                Poll::Ready(Some(event @ InferenceStreamEvent::Failed(_))) => {
                    let InferenceStreamEvent::Failed(failure) = &event else {
                        unreachable!();
                    };
                    let failure = failure.clone();
                    let attempt = self.attempt.clone();
                    self.pending_terminal = Some((
                        event,
                        Box::pin(async move {
                            attempt.fail(failure).await;
                        }),
                    ));
                }
                Poll::Ready(None) => {
                    let failure = sanitized_post_commit_failure();
                    let event = InferenceStreamEvent::Failed(failure.clone());
                    let attempt = self.attempt.clone();
                    self.pending_terminal = Some((
                        event,
                        Box::pin(async move {
                            attempt.fail(failure).await;
                        }),
                    ));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<C: ConfigurationRepository + 'static> Drop for ManagedInferenceStream<C> {
    fn drop(&mut self) {
        if self.terminal_emitted {
            return;
        }
        let attempt = self.attempt.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                attempt.cancel().await;
            });
        }
    }
}

fn sanitized_post_commit_failure() -> ProviderFailure {
    ProviderFailure {
        category: slg_domain::ErrorCategory::ProviderUnavailable,
        message: "provider stream could not be completed".into(),
        status: None,
        retry_at_unix: None,
    }
}

impl<C, E, S> Gateway<C, E, S>
where
    C: ConfigurationRepository + 'static,
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

    async fn handle_execution(
        &self,
        request: &InferenceRequest,
        route: slg_domain::RouteCandidate,
        execution: InferenceExecution,
    ) -> Result<InferenceResponse, ApplicationError> {
        let attempt_id = AttemptId::new();
        match execution {
            InferenceExecution::Complete {
                response,
                authoritative_evidence,
            } => {
                self.persist_attempt(AttemptRecord {
                    attempt_id: attempt_id.clone(),
                    request_id: request.request_id.to_string(),
                    route_id: route.route_id.clone(),
                    outcome: AttemptOutcome::Succeeded,
                    failure_category: None,
                })
                .await?;
                self.configuration
                    .mark_route_success(&route.route_id)
                    .await
                    .map_err(ApplicationError::Configuration)?;
                if let Some(evidence) = authoritative_evidence {
                    self.persist_authoritative_evidence(
                        &route.provider_account_id,
                        &attempt_id,
                        *evidence,
                    )
                    .await;
                }
                Ok(InferenceResponse::Complete(response))
            }
            InferenceExecution::Streaming { body } => {
                let attempt = StreamAttempt {
                    configuration: self.configuration.clone(),
                    usage_spool: self.usage_spool.clone(),
                    attempt_id,
                    request_id: request.request_id.to_string(),
                    route_id: route.route_id,
                    provider_account_id: route.provider_account_id,
                };
                attempt
                    .persist(attempt.record(AttemptOutcome::Committed, None))
                    .await
                    .map_err(|()| {
                        ApplicationError::Configuration(
                            "control plane unavailable while persisting committed attempt".into(),
                        )
                    })?;
                Ok(InferenceResponse::Streaming(Box::pin(
                    ManagedInferenceStream {
                        upstream: body,
                        attempt,
                        pending_terminal: None,
                        terminal_emitted: false,
                    },
                )))
            }
        }
    }

    async fn handle_pre_commit_failure(
        &self,
        request: &InferenceRequest,
        route: &slg_domain::RouteCandidate,
        failure: &ProviderFailure,
    ) -> Result<(), ApplicationError> {
        self.persist_attempt(AttemptRecord {
            attempt_id: AttemptId::new(),
            request_id: request.request_id.to_string(),
            route_id: route.route_id.clone(),
            outcome: AttemptOutcome::Failed,
            failure_category: Some(format!("{:?}", failure.category)),
        })
        .await?;
        if failure.category.blocks_account() {
            self.configuration
                .mark_account_failure(&route.provider_account_id, failure)
                .await
                .map_err(ApplicationError::Configuration)?;
        }
        if failure.category.opens_route() {
            self.configuration
                .mark_route_failure(&route.route_id, failure)
                .await
                .map_err(ApplicationError::Configuration)?;
        }
        Ok(())
    }

    pub async fn infer(
        &self,
        raw_key: &str,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ApplicationError> {
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
                Ok(execution) => return self.handle_execution(&request, route, execution).await,
                Err(failure) => {
                    self.handle_pre_commit_failure(&request, &route, &failure)
                        .await?;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures_util::{StreamExt, stream};
    use slg_domain::{
        AuthoritativeSource, CredentialReference, ErrorCategory, FixedDecimal,
        ProviderAuthoritativeEvidence, ProviderBillingEvidence, ProviderFailure,
        ProviderReportedQuantity, ProviderUnit, ProviderUnitKind, RouteCandidate,
    };
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::*;
    use slg_ports::{
        InferenceExecution, OpenAiChatCompletionChoice, OpenAiChatCompletionChunk,
        OpenAiChatCompletionDelta,
    };

    fn frame(content: &str) -> InferenceStreamEvent {
        InferenceStreamEvent::Frame(Box::new(OpenAiChatCompletionChunk {
            id: None,
            object: None,
            created: None,
            model: None,
            service_tier: None,
            system_fingerprint: None,
            choices: vec![OpenAiChatCompletionChoice {
                index: None,
                delta: OpenAiChatCompletionDelta {
                    content: Some(content.into()),
                    ..OpenAiChatCompletionDelta::default()
                },
                logprobs: None,
                finish_reason: None,
            }],
            usage: None,
        }))
    }

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

    #[derive(Clone)]
    struct LifecycleConfiguration {
        candidates: Vec<RouteCandidate>,
        attempts: Arc<Mutex<Vec<AttemptRecord>>>,
        route_successes: Arc<AtomicUsize>,
        route_failures: Arc<AtomicUsize>,
        cancellation_recorded: Arc<Notify>,
    }

    impl LifecycleConfiguration {
        fn new(route_id: &str) -> Self {
            Self {
                candidates: vec![route(route_id)],
                attempts: Arc::new(Mutex::new(Vec::new())),
                route_successes: Arc::new(AtomicUsize::new(0)),
                route_failures: Arc::new(AtomicUsize::new(0)),
                cancellation_recorded: Arc::new(Notify::new()),
            }
        }
    }

    #[async_trait]
    impl ConfigurationRepository for LifecycleConfiguration {
        async fn authenticate(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }

        async fn candidates(&self, _: &str) -> Result<Vec<RouteCandidate>, String> {
            Ok(self.candidates.clone())
        }

        async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
            let cancelled = attempt.outcome == AttemptOutcome::Cancelled;
            self.attempts.lock().unwrap().push(attempt);
            if cancelled {
                self.cancellation_recorded.notify_one();
            }
            Ok(())
        }

        async fn mark_route_success(&self, _: &str) -> Result<(), String> {
            self.route_successes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn mark_route_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            self.route_failures.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn mark_account_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            Ok(())
        }
    }

    struct LifecycleExecutor;

    #[async_trait]
    impl InferenceExecutor for LifecycleExecutor {
        async fn execute(
            &self,
            route: &RouteCandidate,
            _: &InferenceRequest,
            _: &str,
        ) -> Result<InferenceExecution, ProviderFailure> {
            let frame = frame("one");
            let body: InferenceStream = match route.route_id.as_str() {
                "success-route" => Box::pin(stream::iter([frame, InferenceStreamEvent::Completed])),
                "failure-route" => Box::pin(stream::iter([
                    frame,
                    InferenceStreamEvent::Failed(ProviderFailure {
                        category: ErrorCategory::ProviderUnavailable,
                        message: "sanitized stream failure".into(),
                        status: None,
                        retry_at_unix: None,
                    }),
                ])),
                "cancel-route" => {
                    Box::pin(stream::once(async move { frame }).chain(stream::pending()))
                }
                _ => panic!("unexpected lifecycle route"),
            };
            Ok(InferenceExecution::streaming(body))
        }
    }

    struct TestExecutor;

    #[async_trait]
    impl InferenceExecutor for TestExecutor {
        async fn execute(
            &self,
            route: &RouteCandidate,
            request: &InferenceRequest,
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
            if request.stream {
                return Ok(InferenceExecution::streaming(Box::pin(stream::iter([
                    frame("ok"),
                    InferenceStreamEvent::Completed,
                ]))));
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

    #[derive(Clone, Default)]
    struct CommittedStreamExecutor {
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl InferenceExecutor for CommittedStreamExecutor {
        async fn execute(
            &self,
            route: &RouteCandidate,
            _: &InferenceRequest,
            _: &str,
        ) -> Result<InferenceExecution, ProviderFailure> {
            self.calls.lock().unwrap().push(route.route_id.clone());
            if route.route_id == "committed-route" {
                return Ok(InferenceExecution::streaming(Box::pin(stream::iter([
                    frame("partial"),
                    InferenceStreamEvent::Failed(ProviderFailure {
                        category: ErrorCategory::ProviderUnavailable,
                        message: "sanitized stream failure".into(),
                        status: None,
                        retry_at_unix: None,
                    }),
                ]))));
            }
            Ok(InferenceExecution::streaming(Box::pin(stream::iter([
                frame("fallback-must-not-run"),
                InferenceStreamEvent::Completed,
            ]))))
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
            Ok(InferenceExecution::Complete {
                response: serde_json::json!({"id": "response", "usage": {"total_tokens": 8}}),
                authoritative_evidence: Some(Box::new(ProviderAuthoritativeEvidence {
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
                })),
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

    fn complete_response(response: InferenceResponse) -> serde_json::Value {
        match response {
            InferenceResponse::Complete(value) => value,
            InferenceResponse::Streaming(_) => panic!("expected a complete response"),
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
            complete_response(gateway.infer("gateway-key", request()).await.unwrap()),
            serde_json::json!({"id": "response"})
        );

        let attempts = spool.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, AttemptOutcome::Succeeded);
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
        assert_eq!(attempts[0].outcome, AttemptOutcome::Failed);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Succeeded);
        assert_ne!(attempts[0].attempt_id, attempts[1].attempt_id);
    }

    #[tokio::test]
    async fn streaming_failure_before_commit_falls_back_to_next_route() {
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
        let mut streaming_request = request();
        streaming_request.stream = true;

        let InferenceResponse::Streaming(body) = gateway
            .infer("gateway-key", streaming_request)
            .await
            .unwrap()
        else {
            panic!("expected fallback route to return a stream");
        };
        let chunks = body.collect::<Vec<_>>().await;
        assert_eq!(chunks.len(), 2);
        assert!(matches!(chunks[0], InferenceStreamEvent::Frame(_)));
        assert_eq!(chunks[1], InferenceStreamEvent::Completed);
        let attempts = spool.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 3);
        assert_eq!(attempts[0].outcome, AttemptOutcome::Failed);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Committed);
        assert_eq!(attempts[2].outcome, AttemptOutcome::Succeeded);
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
    async fn unavailable_streaming_route_is_reported_before_executor_access() {
        let gateway = Gateway::new(UnavailableConfiguration, PanicExecutor, TestSecrets);
        let mut streaming_request = request();
        streaming_request.stream = true;

        assert!(matches!(
            gateway.infer("gateway-key", streaming_request).await,
            Err(ApplicationError::UpstreamUnavailable)
        ));
    }

    #[tokio::test]
    async fn post_commit_stream_failure_never_falls_back() {
        let spool = RecordingSpool::default();
        let executor = CommittedStreamExecutor::default();
        let gateway = Gateway::new(
            TestConfiguration {
                candidates: vec![route("committed-route"), route("fallback-route")],
                primary_usage_available: false,
                route_state_available: true,
            },
            executor.clone(),
            TestSecrets,
        )
        .with_usage_spool(spool.clone());
        let mut streaming_request = request();
        streaming_request.stream = true;

        let InferenceResponse::Streaming(mut body) = gateway
            .infer("gateway-key", streaming_request)
            .await
            .unwrap()
        else {
            panic!("expected a streaming response");
        };
        assert!(matches!(
            body.next().await,
            Some(InferenceStreamEvent::Frame(_))
        ));
        assert!(matches!(
            body.next().await,
            Some(InferenceStreamEvent::Failed(_))
        ));
        assert_eq!(
            executor.calls.lock().unwrap().as_slice(),
            ["committed-route"]
        );
        let attempts = spool.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].outcome, AttemptOutcome::Committed);
        assert_eq!(attempts[1].outcome, AttemptOutcome::PartialFailed);
        let serialized = serde_json::to_string(&attempts[0]).unwrap();
        assert!(!serialized.contains("partial"));
        assert!(!serialized.contains("prompt-that-must-not-be-spooled"));
    }

    #[tokio::test]
    async fn route_success_and_succeeded_transition_wait_for_done() {
        let configuration = LifecycleConfiguration::new("success-route");
        let gateway = Gateway::new(configuration.clone(), LifecycleExecutor, TestSecrets);
        let mut streaming_request = request();
        streaming_request.stream = true;
        let InferenceResponse::Streaming(mut body) = gateway
            .infer("gateway-key", streaming_request)
            .await
            .unwrap()
        else {
            panic!("expected streaming response");
        };

        assert_eq!(configuration.route_successes.load(Ordering::SeqCst), 0);
        assert_eq!(
            configuration.attempts.lock().unwrap()[0].outcome,
            AttemptOutcome::Committed
        );
        assert!(matches!(
            body.next().await,
            Some(InferenceStreamEvent::Frame(_))
        ));
        assert_eq!(configuration.route_successes.load(Ordering::SeqCst), 0);
        assert_eq!(body.next().await, Some(InferenceStreamEvent::Completed));
        assert_eq!(configuration.route_successes.load(Ordering::SeqCst), 1);
        let attempts = configuration.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Succeeded);
    }

    #[tokio::test]
    async fn client_disconnect_transitions_committed_attempt_to_cancelled() {
        let configuration = LifecycleConfiguration::new("cancel-route");
        let gateway = Gateway::new(configuration.clone(), LifecycleExecutor, TestSecrets);
        let mut streaming_request = request();
        streaming_request.stream = true;
        let InferenceResponse::Streaming(mut body) = gateway
            .infer("gateway-key", streaming_request)
            .await
            .unwrap()
        else {
            panic!("expected streaming response");
        };
        assert!(matches!(
            body.next().await,
            Some(InferenceStreamEvent::Frame(_))
        ));
        drop(body);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            configuration.cancellation_recorded.notified(),
        )
        .await
        .expect("cancelled transition was not persisted");

        let attempts = configuration.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].outcome, AttemptOutcome::Committed);
        assert_eq!(attempts[1].outcome, AttemptOutcome::Cancelled);
        assert_eq!(configuration.route_successes.load(Ordering::SeqCst), 0);
        assert_eq!(configuration.route_failures.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn post_commit_provider_failure_updates_future_route_state() {
        let configuration = LifecycleConfiguration::new("failure-route");
        let gateway = Gateway::new(configuration.clone(), LifecycleExecutor, TestSecrets);
        let mut streaming_request = request();
        streaming_request.stream = true;
        let InferenceResponse::Streaming(body) = gateway
            .infer("gateway-key", streaming_request)
            .await
            .unwrap()
        else {
            panic!("expected streaming response");
        };
        let events = body.collect::<Vec<_>>().await;
        assert!(matches!(
            events.as_slice(),
            [
                InferenceStreamEvent::Frame(_),
                InferenceStreamEvent::Failed(_)
            ]
        ));
        assert_eq!(configuration.route_failures.load(Ordering::SeqCst), 1);
        let attempts = configuration.attempts.lock().unwrap();
        assert_eq!(attempts[1].outcome, AttemptOutcome::PartialFailed);
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
            complete_response(gateway.infer("gateway-key", request()).await.unwrap()),
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
            complete_response(
                gateway_without_evidence
                    .infer("gateway-key", request())
                    .await
                    .unwrap(),
            ),
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
            complete_response(
                gateway_with_failed_accounting
                    .infer("gateway-key", request())
                    .await
                    .unwrap(),
            ),
            serde_json::json!({"id": "response", "usage": {"total_tokens": 8}})
        );
    }
}
