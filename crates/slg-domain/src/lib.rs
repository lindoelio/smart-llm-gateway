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
