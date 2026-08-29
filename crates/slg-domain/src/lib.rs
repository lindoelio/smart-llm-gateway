//! Protocol-neutral types and pure routing rules.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferenceRequest {
    pub request_id: Uuid,
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub stream: bool,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

/// Immutable identity for one provider-attempt outcome.
///
/// The application creates this identifier before attempting primary
/// persistence. It travels with the attempt through the durable spool, so a
/// recovery worker can safely retry delivery after a process restart without
/// charging the same outcome more than once.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct AttemptId(Uuid);

impl AttemptId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses an attempt identity read from durable storage.
    pub fn parse(value: &str) -> Result<Self, uuid::Error> {
        Uuid::parse_str(value).map(Self)
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

/// Exact decimal reported by a provider, represented without floating point.
///
/// The gateway does not calculate this value. `unscaled` and `scale` preserve
/// the provider's reported precision for quotas, billable units, and money.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FixedDecimal {
    pub unscaled: i64,
    pub scale: u8,
}

/// The kind of capacity or billing unit a provider reported.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderUnitKind {
    Requests,
    InputTokens,
    CachedInputTokens,
    OutputTokens,
    ReasoningTokens,
    TotalTokens,
    ConcurrentRequests,
    Currency,
    Custom,
}

/// An explicit provider-reported unit. Currency and provider-specific units
/// retain their code/name rather than being converted to another unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderUnit {
    pub kind: ProviderUnitKind,
    pub currency_code: Option<String>,
    pub custom_name: Option<String>,
}

impl ProviderUnit {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        let valid_currency = self.currency_code.as_deref().is_some_and(|code| {
            code.len() == 3 && code.bytes().all(|byte| byte.is_ascii_uppercase())
        });
        let valid_custom_name = self
            .custom_name
            .as_deref()
            .is_some_and(|name| !name.trim().is_empty());
        match self.kind {
            ProviderUnitKind::Currency if !valid_currency || self.custom_name.is_some() => {
                Err(AuthoritativeDataError::InvalidUnit)
            }
            ProviderUnitKind::Custom if !valid_custom_name || self.currency_code.is_some() => {
                Err(AuthoritativeDataError::InvalidUnit)
            }
            ProviderUnitKind::Currency | ProviderUnitKind::Custom => Ok(()),
            _ if self.currency_code.is_none() && self.custom_name.is_none() => Ok(()),
            _ => Err(AuthoritativeDataError::InvalidUnit),
        }
    }
}

/// Identifies the provider response, export, or API version that reported a
/// fact. It deliberately contains no credentials or raw response body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthoritativeSource {
    pub source_id: String,
    pub evidence_version: Option<String>,
}

impl AuthoritativeSource {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        (!self.source_id.trim().is_empty())
            .then_some(())
            .ok_or(AuthoritativeDataError::MissingSource)
    }
}

/// One exact provider-reported quantity and its explicit unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderReportedQuantity {
    pub unit: ProviderUnit,
    pub value: FixedDecimal,
}

impl ProviderReportedQuantity {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        self.unit.validate()
    }
}

/// Immutable authoritative capacity evidence for one provider constraint.
///
/// Values are optional because providers expose different shapes. The gateway
/// stores the facts as received; it never derives one value from another.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderQuotaSnapshot {
    pub snapshot_id: String,
    pub provider_account_id: String,
    pub constraint_id: String,
    pub unit: ProviderUnit,
    pub allowance: Option<FixedDecimal>,
    pub consumed: Option<FixedDecimal>,
    pub remaining: Option<FixedDecimal>,
    pub reset_at_unix: Option<i64>,
    pub observed_at_unix: i64,
    pub fresh_until_unix: i64,
    pub source: AuthoritativeSource,
}

impl ProviderQuotaSnapshot {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        non_empty_identifiers(&[
            &self.snapshot_id,
            &self.provider_account_id,
            &self.constraint_id,
        ])?;
        self.unit.validate()?;
        self.source.validate()?;
        if self.allowance.is_none() && self.consumed.is_none() && self.remaining.is_none() {
            return Err(AuthoritativeDataError::MissingReportedValue);
        }
        (self.fresh_until_unix >= self.observed_at_unix)
            .then_some(())
            .ok_or(AuthoritativeDataError::InvalidFreshness)
    }
}

/// Immutable billing evidence reconciled to one gateway attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderBillingRecord {
    pub record_id: String,
    pub attempt_id: AttemptId,
    pub provider_account_id: String,
    pub provider_request_id: Option<String>,
    pub billed_units: Vec<ProviderReportedQuantity>,
    pub charge: Option<ProviderReportedQuantity>,
    pub observed_at_unix: i64,
    pub fresh_until_unix: i64,
    pub source: AuthoritativeSource,
}

impl ProviderBillingRecord {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        non_empty_identifiers(&[&self.record_id, &self.provider_account_id])?;
        self.source.validate()?;
        if self.billed_units.is_empty() && self.charge.is_none() {
            return Err(AuthoritativeDataError::MissingReportedValue);
        }
        for quantity in &self.billed_units {
            quantity.validate()?;
        }
        if let Some(charge) = &self.charge {
            charge.validate()?;
            if charge.unit.kind != ProviderUnitKind::Currency {
                return Err(AuthoritativeDataError::ChargeMustUseCurrency);
            }
        }
        (self.fresh_until_unix >= self.observed_at_unix)
            .then_some(())
            .ok_or(AuthoritativeDataError::InvalidFreshness)
    }
}

/// Provider-supplied billing facts from a single inference response.
///
/// This is intentionally narrower than [`ProviderBillingRecord`]: the
/// application supplies the gateway attempt identity and the local observation
/// timestamp, while the provider adapter supplies only values explicitly
/// present in an upstream response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderBillingEvidence {
    pub provider_request_id: Option<String>,
    pub billed_units: Vec<ProviderReportedQuantity>,
    pub charge: Option<ProviderReportedQuantity>,
    pub source: AuthoritativeSource,
}

impl ProviderBillingEvidence {
    pub fn validate(&self) -> Result<(), AuthoritativeDataError> {
        self.source.validate()?;
        if self.billed_units.is_empty() && self.charge.is_none() {
            return Err(AuthoritativeDataError::MissingReportedValue);
        }
        for quantity in &self.billed_units {
            quantity.validate()?;
        }
        if let Some(charge) = &self.charge {
            charge.validate()?;
            if charge.unit.kind != ProviderUnitKind::Currency {
                return Err(AuthoritativeDataError::ChargeMustUseCurrency);
            }
        }
        Ok(())
    }
}

/// Optional facts supplied by one provider inference response.
///
/// The gateway carries this evidence alongside the client response but never
/// derives values from it. Quota observations retain their provider account
/// identity so the application can reject cross-account contamination.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderAuthoritativeEvidence {
    pub quota_snapshots: Vec<ProviderQuotaSnapshot>,
    pub billing: Option<ProviderBillingEvidence>,
}

impl ProviderAuthoritativeEvidence {
    pub fn validate_for_account(
        &self,
        provider_account_id: &str,
    ) -> Result<(), AuthoritativeDataError> {
        for snapshot in &self.quota_snapshots {
            snapshot.validate()?;
            if snapshot.provider_account_id != provider_account_id {
                return Err(AuthoritativeDataError::MismatchedProviderAccount);
            }
        }
        if let Some(billing) = &self.billing {
            billing.validate()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.quota_snapshots.is_empty() && self.billing.is_none()
    }
}

fn non_empty_identifiers(identifiers: &[&str]) -> Result<(), AuthoritativeDataError> {
    identifiers
        .iter()
        .all(|identifier| !identifier.trim().is_empty())
        .then_some(())
        .ok_or(AuthoritativeDataError::MissingIdentifier)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthoritativeDataError {
    #[error("authoritative provider data requires a non-empty identifier")]
    MissingIdentifier,
    #[error("authoritative provider data requires a source identifier")]
    MissingSource,
    #[error("provider-reported unit metadata is invalid")]
    InvalidUnit,
    #[error("authoritative provider data requires at least one reported value")]
    MissingReportedValue,
    #[error("authoritative provider data has an invalid freshness window")]
    InvalidFreshness,
    #[error("a provider-reported charge must use an explicit currency unit")]
    ChargeMustUseCurrency,
    #[error("provider-authoritative evidence belongs to a different provider account")]
    MismatchedProviderAccount,
}

/// A normalized reference to an environment variable that holds a credential.
///
/// Credential values are not valid configuration data. Keeping this as a
/// validated domain type ensures route snapshots and repository reads cannot
/// reintroduce literal secrets after they pass a persistence boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String", into = "String")]
pub struct CredentialReference(String);

impl CredentialReference {
    /// Parses the only permitted credential reference form: `env:NAME`.
    pub fn parse(value: &str) -> Result<Self, CredentialReferenceError> {
        let Some(name) = value.strip_prefix("env:") else {
            return Err(CredentialReferenceError::InvalidFormat);
        };
        let mut characters = name.chars();
        let Some(first) = characters.next() else {
            return Err(CredentialReferenceError::InvalidFormat);
        };
        if !(first == '_' || first.is_ascii_alphabetic())
            || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
        {
            return Err(CredentialReferenceError::InvalidEnvironmentName);
        }
        Ok(Self(format!("env:{name}")))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn environment_name(&self) -> &str {
        self.0.strip_prefix("env:").unwrap_or_default()
    }
}

impl TryFrom<String> for CredentialReference {
    type Error = CredentialReferenceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<CredentialReference> for String {
    fn from(reference: CredentialReference) -> Self {
        reference.0
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CredentialReferenceError {
    #[error("credential reference must use the explicit env:NAME format")]
    InvalidFormat,
    #[error("credential reference must use env:NAME with a valid environment variable name")]
    InvalidEnvironmentName,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteCandidate {
    pub route_id: String,
    pub logical_model: String,
    pub provider_account_id: String,
    pub provider: String,
    pub credential_ref: CredentialReference,
    pub base_url: String,
    pub upstream_model: String,
    pub priority: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    QuotaExhausted,
    CreditExhausted,
    SpendLimitExceeded,
    RateLimited,
    ConcurrencyLimited,
    AuthenticationFailed,
    ModelUnavailable,
    ProviderUnavailable,
    Unknown,
}

impl ErrorCategory {
    #[must_use]
    pub const fn blocks_account(self) -> bool {
        matches!(
            self,
            Self::QuotaExhausted
                | Self::CreditExhausted
                | Self::SpendLimitExceeded
                | Self::AuthenticationFailed
        )
    }

    #[must_use]
    pub const fn opens_route(self) -> bool {
        matches!(
            self,
            Self::RateLimited
                | Self::ConcurrencyLimited
                | Self::ModelUnavailable
                | Self::ProviderUnavailable
                | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderFailure {
    pub category: ErrorCategory,
    pub message: String,
    pub status: Option<u16>,
    pub retry_at_unix: Option<i64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("requested logical model `{0}` is not configured")]
    UnknownModel(String),
    #[error("fallback graph contains a cycle at `{0}`")]
    FallbackCycle(String),
    #[error("no eligible route exists for `{0}`")]
    NoEligibleRoute(String),
}

/// Expands a model and its ordered fallback graph into a deterministic route plan.
///
/// # Errors
///
/// Returns an error when the requested model is not configured, no route is
/// eligible, or the configured fallback graph contains a cycle.
pub fn candidate_plan(
    requested_model: &str,
    routes: &[RouteCandidate],
    fallbacks: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<RouteCandidate>, DomainError> {
    fn visit(
        model: &str,
        routes: &HashMap<&str, Vec<&RouteCandidate>>,
        fallbacks: &BTreeMap<String, Vec<String>>,
        visiting: &mut HashSet<String>,
        visited: &mut HashSet<String>,
        output: &mut Vec<RouteCandidate>,
    ) -> Result<(), DomainError> {
        if !visiting.insert(model.to_owned()) {
            return Err(DomainError::FallbackCycle(model.to_owned()));
        }
        if !visited.insert(model.to_owned()) {
            visiting.remove(model);
            return Ok(());
        }
        if let Some(model_routes) = routes.get(model) {
            output.extend(model_routes.iter().map(|route| (*route).clone()));
        }
        if let Some(next_models) = fallbacks.get(model) {
            for next in next_models {
                visit(next, routes, fallbacks, visiting, visited, output)?;
            }
        }
        visiting.remove(model);
        Ok(())
    }

    let mut routes_by_model: HashMap<&str, Vec<&RouteCandidate>> = HashMap::new();
    for route in routes {
        routes_by_model
            .entry(&route.logical_model)
            .or_default()
            .push(route);
    }
    for entries in routes_by_model.values_mut() {
        entries.sort_by_key(|route| route.priority);
    }
    if !routes_by_model.contains_key(requested_model) {
        return Err(DomainError::UnknownModel(requested_model.to_owned()));
    }

    let mut output = Vec::new();
    visit(
        requested_model,
        &routes_by_model,
        fallbacks,
        &mut HashSet::new(),
        &mut HashSet::new(),
        &mut output,
    )?;
    if output.is_empty() {
        Err(DomainError::NoEligibleRoute(requested_model.to_owned()))
    } else {
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(model: &str, id: &str, priority: u32) -> RouteCandidate {
        RouteCandidate {
            route_id: id.into(),
            logical_model: model.into(),
            provider_account_id: "account".into(),
            provider: "test".into(),
            credential_ref: CredentialReference::parse("env:KEY").unwrap(),
            base_url: "https://example.test".into(),
            upstream_model: model.into(),
            priority,
        }
    }

    #[test]
    fn expands_routes_then_fallbacks() {
        let mut fallbacks = BTreeMap::new();
        fallbacks.insert("primary".into(), vec!["backup".into()]);
        let plan = candidate_plan(
            "primary",
            &[
                route("primary", "a", 2),
                route("primary", "b", 1),
                route("backup", "c", 1),
            ],
            &fallbacks,
        )
        .unwrap();
        assert_eq!(
            plan.iter()
                .map(|item| item.route_id.as_str())
                .collect::<Vec<_>>(),
            ["b", "a", "c"]
        );
    }

    #[test]
    fn rejects_fallback_cycles() {
        let mut fallbacks = BTreeMap::new();
        fallbacks.insert("primary".into(), vec!["backup".into()]);
        fallbacks.insert("backup".into(), vec!["primary".into()]);
        assert_eq!(
            candidate_plan(
                "primary",
                &[route("primary", "a", 1), route("backup", "b", 1)],
                &fallbacks
            ),
            Err(DomainError::FallbackCycle("primary".into()))
        );
    }

    #[test]
    fn credential_references_accept_only_normalized_environment_references() {
        let reference = CredentialReference::parse("env:PROVIDER_API_KEY").unwrap();
        assert_eq!(reference.as_str(), "env:PROVIDER_API_KEY");
        assert_eq!(reference.environment_name(), "PROVIDER_API_KEY");
        for invalid in [
            "PROVIDER_API_KEY",
            "sk-live-literal",
            "env:",
            "env:1INVALID",
        ] {
            assert!(CredentialReference::parse(invalid).is_err(), "{invalid}");
        }
    }
}
