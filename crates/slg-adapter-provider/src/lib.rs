//! Generic OpenAI-compatible provider connector with conservative error mapping.

use async_trait::async_trait;
use reqwest::StatusCode;
use slg_domain::{ErrorCategory, InferenceRequest, ProviderFailure, RouteCandidate};
use slg_ports::InferenceExecutor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct OpenAiCompatibleExecutor {
    client: reqwest::Client,
}

impl Default for OpenAiCompatibleExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiCompatibleExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl InferenceExecutor for OpenAiCompatibleExecutor {
    async fn execute(
        &self,
        route: &RouteCandidate,
        request: &InferenceRequest,
        credential: &str,
    ) -> Result<serde_json::Value, ProviderFailure> {
        if request.stream {
            return Err(ProviderFailure {
                category: ErrorCategory::Unknown,
                message: "streaming is not yet enabled for the initial OpenAI-compatible slice"
                    .into(),
                status: None,
                retry_at_unix: None,
            });
        }
        let url = format!(
            "{}/v1/chat/completions",
            route.base_url.trim_end_matches('/')
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(credential)
            .json(&slg_adapter_upstream_openai::encode_chat_completion(
                request,
                &route.upstream_model,
            ))
            .send()
            .await
            .map_err(|_| ProviderFailure {
                category: ErrorCategory::ProviderUnavailable,
                message: client_message(ErrorCategory::ProviderUnavailable).into(),
                status: None,
                retry_at_unix: None,
            })?;
        let status = response.status();
        let body = response.json::<serde_json::Value>().await.unwrap_or_else(|_| serde_json::json!({"error": {"message": "provider returned an invalid JSON response"}}));
        if status.is_success() {
            return Ok(body);
        }
        Err(classify(status, &body))
    }
}

fn classify(status: StatusCode, body: &serde_json::Value) -> ProviderFailure {
    let code = body
        .pointer("/error/code")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let category = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        ErrorCategory::AuthenticationFailed
    } else if code.contains("insufficient_quota") || code.contains("quota_exceeded") {
        ErrorCategory::QuotaExhausted
    } else if code.contains("credit") {
        ErrorCategory::CreditExhausted
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        ErrorCategory::RateLimited
    } else if status == StatusCode::NOT_FOUND || code.contains("model") {
        ErrorCategory::ModelUnavailable
    } else if status.is_server_error() {
        ErrorCategory::ProviderUnavailable
    } else {
        ErrorCategory::Unknown
    };
    let retry_at_unix = matches!(
        category,
        ErrorCategory::RateLimited
            | ErrorCategory::ConcurrencyLimited
            | ErrorCategory::ProviderUnavailable
            | ErrorCategory::Unknown
    )
    .then(|| {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
        )
        .unwrap_or(i64::MAX - 60)
            + 60
    });
    ProviderFailure {
        category,
        message: client_message(category).into(),
        status: Some(status.as_u16()),
        retry_at_unix,
    }
}

const fn client_message(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::QuotaExhausted
        | ErrorCategory::CreditExhausted
        | ErrorCategory::SpendLimitExceeded => "provider capacity is currently unavailable",
        ErrorCategory::RateLimited | ErrorCategory::ConcurrencyLimited => {
            "provider is temporarily rate limited"
        }
        ErrorCategory::AuthenticationFailed => "provider credential is unavailable",
        ErrorCategory::ModelUnavailable => "requested upstream model is unavailable",
        ErrorCategory::ProviderUnavailable | ErrorCategory::Unknown => {
            "provider request could not be completed"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_429_is_rate_limited_not_quota() {
        assert_eq!(
            classify(
                StatusCode::TOO_MANY_REQUESTS,
                &serde_json::json!({"error": {"message": "slow down"}})
            )
            .category,
            ErrorCategory::RateLimited
        );
    }

    #[test]
    fn classification_never_exposes_raw_upstream_error_content() {
        let raw_message = "provider diagnostic with a secret-like value: sk-live-not-for-client";
        let failure = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            &serde_json::json!({"error": {"message": raw_message}}),
        );
        assert_eq!(failure.category, ErrorCategory::ProviderUnavailable);
        assert_eq!(failure.message, "provider request could not be completed");
        assert!(!failure.message.contains(raw_message));
    }
}
