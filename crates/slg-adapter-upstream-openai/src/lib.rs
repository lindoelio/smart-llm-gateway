//! OpenAI-compatible upstream codec.

use serde_json::{Value, json};
use slg_domain::InferenceRequest;

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
