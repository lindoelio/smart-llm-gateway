//! The initial OpenAI-compatible public HTTP surface.

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use slg_application::{ApplicationError, Gateway, InferenceResponse};
use slg_domain::{ChatMessage, InferenceRequest};
use slg_ports::{
    ConfigurationRepository, InferenceExecutor, InferenceStream, InferenceStreamEvent,
    SecretResolver,
};
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

fn application_error_response(result: Result<InferenceResponse, ApplicationError>) -> Response {
    match result {
        Ok(InferenceResponse::Complete(response)) => {
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(InferenceResponse::Streaming(body)) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no")
            .body(Body::from_stream(public_sse_stream(body)))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Err(ApplicationError::Unauthorized) => error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "Invalid gateway key",
        ),
        Err(ApplicationError::Configuration(message)) => {
            error(StatusCode::BAD_REQUEST, "configuration_error", &message)
        }
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

fn public_sse_stream(
    stream: InferenceStream,
) -> impl futures_util::Stream<Item = Result<Bytes, std::convert::Infallible>> {
    stream.map(|event| {
        let bytes = match event {
            InferenceStreamEvent::Frame(frame) => {
                let mut encoded = b"data: ".to_vec();
                encoded.extend_from_slice(&serde_json::to_vec(&frame).unwrap_or_else(|_| b"{}".to_vec()));
                encoded.extend_from_slice(b"\n\n");
                encoded
            }
            InferenceStreamEvent::Completed => b"data: [DONE]\n\n".to_vec(),
            InferenceStreamEvent::Failed(_) => b"data: {\"error\":{\"message\":\"The configured upstream provider could not complete the stream\",\"type\":\"upstream_error\",\"code\":\"upstream_error\"}}\n\n".to_vec(),
        };
        Ok(bytes.into())
    })
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
    use axum::{body::to_bytes, http::StatusCode};
    use bytes::Bytes;
    use futures_util::stream;
    use slg_adapter_upstream_openai::decode_chat_completion_stream;
    use slg_application::{ApplicationError, InferenceResponse};
    use slg_domain::{ErrorCategory, ProviderFailure};
    use slg_ports::{InferenceExecution, InferenceStreamEvent};

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
    fn streaming_response_uses_unbuffered_sse_headers() {
        let InferenceExecution::Streaming { body } =
            InferenceExecution::streaming(Box::pin(stream::iter([
                InferenceStreamEvent::Completed,
            ])))
        else {
            unreachable!();
        };
        let response = application_error_response(Ok(InferenceResponse::Streaming(body)));
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        assert_eq!(response.headers()["cache-control"], "no-cache");
        assert_eq!(response.headers()["x-accel-buffering"], "no");
    }

    #[tokio::test]
    async fn post_commit_failure_emits_one_safe_openai_error_event() {
        let raw = "provider diagnostic sk-live-must-not-leak";
        let response =
            application_error_response(Ok(InferenceResponse::Streaming(Box::pin(stream::iter([
                InferenceStreamEvent::Failed(ProviderFailure {
                    category: ErrorCategory::ProviderUnavailable,
                    message: raw.into(),
                    status: Some(500),
                    retry_at_unix: None,
                }),
            ])))));
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert_eq!(text.matches("data: ").count(), 1);
        assert!(text.contains("upstream_error"));
        assert!(!text.contains(raw));
        assert!(!text.contains("sk-live"));
    }

    #[tokio::test]
    async fn emitted_sse_contains_only_recursively_allowlisted_chunk_fields() {
        let source = stream::iter([
            Ok(Bytes::from_static(
                br#"data: {"id":"chunk-1","object":"chat.completion.chunk","created":42,"model":"safe-model","diagnostic":"Bearer sk-live-root","choices":[{"index":0,"diagnostic":"sk-live-choice","delta":{"content":"safe content","diagnostic":"sk-live-delta","tool_calls":[{"index":0,"id":"call-1","type":"function","diagnostic":"sk-live-tool","function":{"name":"lookup","arguments":"{\"city\":\"Sao Paulo\"}","diagnostic":"sk-live-function"}}]},"finish_reason":null}]}"#,
            )),
            Ok(Bytes::from_static(b"\n\ndata: [DONE]\n\n")),
        ]);
        let response = application_error_response(Ok(InferenceResponse::Streaming(
            decode_chat_completion_stream(Box::pin(source)),
        )));

        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("safe content"));
        assert!(text.contains("call-1"));
        assert!(text.contains("lookup"));
        assert!(text.contains(r#"{\"city\":\"Sao Paulo\"}"#));
        assert!(text.contains("data: [DONE]"));
        assert!(!text.contains("diagnostic"));
        assert!(!text.contains("sk-live"));
        assert!(!text.contains("Bearer"));
    }
}
