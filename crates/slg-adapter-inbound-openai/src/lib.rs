//! The initial OpenAI-compatible public HTTP surface.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;
use serde_json::json;
use slg_application::{ApplicationError, Gateway};
use slg_domain::{ChatMessage, InferenceRequest};
use slg_ports::{ConfigurationRepository, InferenceExecutor, SecretResolver};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    stream: bool,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
}

pub fn router<C, E, S>(gateway: Arc<Gateway<C, E, S>>) -> Router
where
    C: ConfigurationRepository + 'static,
    E: InferenceExecutor + 'static,
    S: SecretResolver + 'static,
{
    Router::new()
        .route("/v1/chat/completions", post(chat_completion::<C, E, S>))
        .route(
            "/healthz",
            axum::routing::get(|| async { StatusCode::NO_CONTENT }),
        )
        .with_state(gateway)
}

async fn chat_completion<C, E, S>(
    State(gateway): State<Arc<Gateway<C, E, S>>>,
    headers: HeaderMap,
    Json(input): Json<ChatCompletionRequest>,
) -> Response
where
    C: ConfigurationRepository + 'static,
    E: InferenceExecutor + 'static,
    S: SecretResolver + 'static,
{
    let Some(key) = bearer(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "Missing Bearer gateway key",
        );
    };
    let request = InferenceRequest {
        request_id: Uuid::new_v4(),
        model: input.model,
        messages: input.messages,
        stream: input.stream,
        temperature: input.temperature,
        max_tokens: input.max_tokens,
    };
    application_error_response(gateway.infer(key, request).await)
}

fn application_error_response(result: Result<serde_json::Value, ApplicationError>) -> Response {
    match result {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(ApplicationError::Unauthorized) => error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "Invalid gateway key",
        ),
        Err(ApplicationError::Configuration(message)) => {
            error(StatusCode::BAD_REQUEST, "configuration_error", &message)
        }
        Err(ApplicationError::StreamingUnsupported) => error(
            StatusCode::BAD_REQUEST,
            "unsupported_feature",
            "Streaming is not supported by this gateway version",
        ),
        Err(ApplicationError::UpstreamUnavailable) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "No upstream route is currently available",
        ),
        Err(ApplicationError::Upstream(message)) => error(
            StatusCode::BAD_GATEWAY,
            "upstream_error",
            sanitized_upstream_message(&message),
        ),
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn sanitized_upstream_message(_: &str) -> &'static str {
    "The configured upstream provider could not complete the request"
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"message": message, "type": code, "code": code}})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use slg_application::ApplicationError;

    use super::{application_error_response, sanitized_upstream_message};

    #[test]
    fn upstream_error_message_is_never_reflected_to_the_client() {
        let raw = "provider diagnostic: sk-live-credential-must-not-leak";
        let sanitized = sanitized_upstream_message(raw);
        assert_eq!(
            sanitized,
            "The configured upstream provider could not complete the request"
        );
        assert!(!sanitized.contains(raw));
    }

    #[test]
    fn unavailable_routes_return_a_normalized_503_response() {
        let response = application_error_response(Err(ApplicationError::UpstreamUnavailable));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn streaming_rejection_is_not_a_configuration_error() {
        let response = application_error_response(Err(ApplicationError::StreamingUnsupported));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
