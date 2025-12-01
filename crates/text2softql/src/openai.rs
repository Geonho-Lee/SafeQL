use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Error during chat completion. hint = {hint}")]
pub struct ChatError {
    pub hint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub max_tokens: u32,
    pub temperature: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,
}

impl ChatRequest {
    pub fn new(model: String, message: String) -> Self {
        Self {
            model,
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: message,
            }],
            max_tokens: 500,
            temperature: 0.0,
            stop: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ChatResponse {
    pub choices: Vec<ChatChoice>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct ChatChoice {
    pub message: ChatMessage,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: i32,
    pub completion_tokens: i32,
    pub total_tokens: i32,
}

impl ChatResponse {
    pub fn try_pop_softql(self) -> Result<String, ChatError> {
        self.choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or(ChatError {
                hint: "no response choices".to_string(),
            })
    }
}