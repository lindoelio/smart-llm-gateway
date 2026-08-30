//! OpenAI-compatible upstream codec.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::Stream;
use serde_json::{Value, json};
use slg_domain::{ErrorCategory, InferenceRequest, ProviderFailure};
use slg_ports::{InferenceStream, InferenceStreamEvent, OpenAiChatCompletionChunk};

/// Maximum bytes retained while waiting for one complete SSE frame.
pub const MAX_SSE_FRAME_BYTES: usize = 256 * 1024;

pub type OpenAiByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ()>> + Send + 'static>>;

struct OpenAiSseDecoder {
    source: OpenAiByteStream,
    buffer: Vec<u8>,
    terminal: bool,
}

impl OpenAiSseDecoder {
    fn new(source: OpenAiByteStream) -> Self {
        Self {
            source,
            buffer: Vec::new(),
            terminal: false,
        }
    }

    fn fail(&mut self, category: ErrorCategory) -> Poll<Option<InferenceStreamEvent>> {
        self.buffer.clear();
        self.terminal = true;
        Poll::Ready(Some(InferenceStreamEvent::Failed(sanitized_failure(
            category,
        ))))
    }
}

impl Stream for OpenAiSseDecoder {
    type Item = InferenceStreamEvent;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.terminal {
                return Poll::Ready(None);
            }
            if let Some((frame_end, consumed)) = complete_frame(&self.buffer) {
                let frame = self.buffer[..frame_end].to_vec();
                self.buffer.drain(..consumed);
                match decode_frame(&frame) {
                    Ok(None) => continue,
                    Ok(Some(event @ InferenceStreamEvent::Frame(_))) => {
                        return Poll::Ready(Some(event));
                    }
                    Ok(Some(
                        event @ (InferenceStreamEvent::Completed | InferenceStreamEvent::Failed(_)),
                    )) => {
                        self.buffer.clear();
                        self.terminal = true;
                        return Poll::Ready(Some(event));
                    }
                    Err(category) => return self.fail(category),
                }
            }
            if self.buffer.len() > MAX_SSE_FRAME_BYTES {
                return self.fail(ErrorCategory::ProviderUnavailable);
            }
            match self.source.as_mut().poll_next(context) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.len() > MAX_SSE_FRAME_BYTES.saturating_sub(self.buffer.len()) {
                        return self.fail(ErrorCategory::ProviderUnavailable);
                    }
                    self.buffer.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(())) | None) => {
                    return self.fail(ErrorCategory::ProviderUnavailable);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Incrementally decodes one OpenAI-compatible SSE response.
///
/// Only complete JSON object frames cross the port. `[DONE]` is the sole clean
/// completion marker; malformed frames, top-level errors, transport failures,
/// and EOF before `[DONE]` are sanitized terminal failures.
#[must_use]
pub fn decode_chat_completion_stream(source: OpenAiByteStream) -> InferenceStream {
    Box::pin(OpenAiSseDecoder::new(source))
}

fn complete_frame(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut line_start = 0;
    for (index, byte) in buffer.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let mut line = &buffer[line_start..index];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len().saturating_sub(1)];
        }
        if line.is_empty() {
            return Some((line_start, index + 1));
        }
        line_start = index + 1;
    }
    None
}

fn decode_frame(frame: &[u8]) -> Result<Option<InferenceStreamEvent>, ErrorCategory> {
    let text = std::str::from_utf8(frame).map_err(|_| ErrorCategory::ProviderUnavailable)?;
    let mut data = Vec::new();
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    let data = data.join("\n");
    if data.trim() == "[DONE]" {
        return Ok(Some(InferenceStreamEvent::Completed));
    }
    let value: Value =
        serde_json::from_str(&data).map_err(|_| ErrorCategory::ProviderUnavailable)?;
    let object = value
        .as_object()
        .ok_or(ErrorCategory::ProviderUnavailable)?;
    if let Some(error) = object.get("error") {
        return Ok(Some(InferenceStreamEvent::Failed(sanitized_openai_error(
            error,
        ))));
    }
    let chunk = serde_json::from_value::<OpenAiChatCompletionChunk>(value)
        .map_err(|_| ErrorCategory::ProviderUnavailable)?;
    Ok(Some(InferenceStreamEvent::Frame(Box::new(chunk))))
}

fn sanitized_openai_error(error: &Value) -> ProviderFailure {
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let category = if code.contains("insufficient_quota") || code.contains("quota_exceeded") {
        ErrorCategory::QuotaExhausted
    } else if code.contains("credit") {
        ErrorCategory::CreditExhausted
    } else if code.contains("rate") {
        ErrorCategory::RateLimited
    } else if code.contains("model") {
        ErrorCategory::ModelUnavailable
    } else if code.contains("auth") || code.contains("key") {
        ErrorCategory::AuthenticationFailed
    } else {
        ErrorCategory::ProviderUnavailable
    };
    sanitized_failure(category)
}

fn sanitized_failure(category: ErrorCategory) -> ProviderFailure {
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
    let message = match category {
        ErrorCategory::QuotaExhausted
        | ErrorCategory::CreditExhausted
        | ErrorCategory::SpendLimitExceeded => "provider capacity is currently unavailable",
        ErrorCategory::RateLimited | ErrorCategory::ConcurrencyLimited => {
            "provider is temporarily rate limited"
        }
        ErrorCategory::AuthenticationFailed => "provider credential is unavailable",
        ErrorCategory::ModelUnavailable => "requested upstream model is unavailable",
        ErrorCategory::ProviderUnavailable | ErrorCategory::Unknown => {
            "provider stream could not be completed"
        }
    };
    ProviderFailure {
        category,
        message: message.into(),
        status: None,
        retry_at_unix,
    }
}

#[must_use]
pub fn encode_chat_completion(request: &InferenceRequest, upstream_model: &str) -> Value {
    let messages = request
        .messages
        .iter()
        .map(|message| json!({"role": message.role, "content": message.content}))
        .collect::<Vec<_>>();
    let mut payload =
        json!({"model": upstream_model, "messages": messages, "stream": request.stream});
    if let Some(temperature) = request.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        payload["max_tokens"] = json!(max_tokens);
    }
    payload
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::{StreamExt, stream};
    use tokio::sync::Notify;

    use super::*;

    fn decode(chunks: Vec<Result<&'static [u8], ()>>) -> InferenceStream {
        decode_chat_completion_stream(Box::pin(stream::iter(
            chunks
                .into_iter()
                .map(|chunk| chunk.map(Bytes::from_static)),
        )))
    }

    #[tokio::test]
    async fn fragmented_top_level_error_is_consumed_and_redacted() {
        let raw = "sk-live-provider-diagnostic-must-not-leak";
        let mut events = decode(vec![
            Ok(b"data: {\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"sk-live-provider-"),
            Ok(b"diagnostic-must-not-leak\"}}\n\n"),
        ]);
        let Some(InferenceStreamEvent::Failed(failure)) = events.next().await else {
            panic!("expected sanitized failure");
        };
        assert_eq!(failure.category, ErrorCategory::RateLimited);
        assert!(!failure.message.contains(raw));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn emits_first_complete_frame_without_waiting_for_done() {
        let release_done = Arc::new(Notify::new());
        let source_release = release_done.clone();
        let source = stream::unfold((0_u8, source_release), |(index, release)| async move {
            match index {
                0 => Some((
                    Ok(Bytes::from_static(
                        b"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n",
                    )),
                    (1, release),
                )),
                1 => {
                    release.notified().await;
                    Some((Ok(Bytes::from_static(b"data: [DONE]\n\n")), (2, release)))
                }
                _ => None,
            }
        });
        let mut events = decode_chat_completion_stream(Box::pin(source));
        let Some(InferenceStreamEvent::Frame(frame)) =
            tokio::time::timeout(Duration::from_millis(100), events.next())
                .await
                .expect("decoder waited for the terminal marker")
        else {
            panic!("expected the first frame incrementally");
        };
        assert_eq!(frame.choices[0].delta.content.as_deref(), Some("one"));
        release_done.notify_one();
        assert_eq!(events.next().await, Some(InferenceStreamEvent::Completed));
    }

    #[tokio::test]
    async fn canonical_chunk_recursively_drops_unknown_secret_like_fields() {
        let mut events = decode(vec![
            Ok(br#"data: {"id":"chunk-1","object":"chat.completion.chunk","created":42,"model":"safe-model","diagnostic":"Bearer sk-live-root","choices":[{"index":0,"diagnostic":"sk-live-choice","delta":{"role":"assistant","content":"safe content","diagnostic":"sk-live-delta","tool_calls":[{"index":0,"id":"call-1","type":"function","diagnostic":"sk-live-tool","function":{"name":"lookup","arguments":"{\"city\":\"Sao Paulo\"}","diagnostic":"sk-live-function"}}]},"logprobs":{"content":[{"token":"safe","logprob":-0.5,"bytes":[115,97,102,101],"top_logprobs":[{"token":"safe","logprob":-0.5,"bytes":[115],"diagnostic":"sk-live-top"}],"diagnostic":"sk-live-token"}],"diagnostic":"sk-live-logprobs"},"finish_reason":null}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3,"diagnostic":"sk-live-usage"}}"#),
            Ok(b"\n\n"),
        ]);

        let Some(InferenceStreamEvent::Frame(chunk)) = events.next().await else {
            panic!("expected a canonical frame");
        };
        let encoded = serde_json::to_string(&chunk).unwrap();
        assert!(!encoded.contains("diagnostic"));
        assert!(!encoded.contains("sk-live"));
        assert!(!encoded.contains("Bearer"));
        assert!(encoded.contains("safe content"));
        assert!(encoded.contains("call-1"));
        assert!(encoded.contains("lookup"));
        assert!(encoded.contains(r#"{\"city\":\"Sao Paulo\"}"#));
        assert!(encoded.contains("top_logprobs"));
    }

    #[tokio::test]
    async fn done_is_clean_and_early_eof_is_failed() {
        let complete = decode(vec![Ok(b"data: [DONE]\n\n")])
            .collect::<Vec<_>>()
            .await;
        assert_eq!(complete, [InferenceStreamEvent::Completed]);

        let early_eof = decode(vec![Ok(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        )])
        .collect::<Vec<_>>()
        .await;
        assert!(matches!(
            early_eof.as_slice(),
            [
                InferenceStreamEvent::Frame(_),
                InferenceStreamEvent::Failed(_)
            ]
        ));
    }

    #[tokio::test]
    async fn invalid_or_oversized_frames_fail_without_emission() {
        let invalid = decode(vec![Ok(b"data: {not-json}\n\n")])
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            invalid.as_slice(),
            [InferenceStreamEvent::Failed(_)]
        ));

        let oversized = vec![b'x'; MAX_SSE_FRAME_BYTES + 1];
        let events =
            decode_chat_completion_stream(Box::pin(stream::iter([Ok(Bytes::from(oversized))])))
                .collect::<Vec<_>>()
                .await;
        assert!(matches!(
            events.as_slice(),
            [InferenceStreamEvent::Failed(_)]
        ));
    }
}
