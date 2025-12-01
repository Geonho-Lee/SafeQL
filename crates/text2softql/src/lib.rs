pub mod openai;
pub mod prompt;

use crate::openai::{ChatError, ChatRequest, ChatResponse};
use reqwest::blocking::Client;
use std::time::Duration;

pub struct Text2SoftQLOptions {
    pub base_url: String,
    pub model_name: String,
    pub api_key: String,
}

pub fn text2softql(
    schema: String,
    context: String,
    query: String,
    opt: Text2SoftQLOptions,
) -> Result<ChatResponse, ChatError> {
    let url = format!("{}/chat/completions", opt.base_url);
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| ChatError {
            hint: e.to_string(),
        })?;

    let prompt = prompt::generate_text2softql_prompt(&schema, &context, &query);

    let mut request = ChatRequest::new(opt.model_name, prompt);
    request.stop = Some(vec!["\n\n".to_string()]); // 원하는 stop 조건 설정 가능

    let resp = client
        .post(url)
        .header("Authorization", format!("Bearer {}", opt.api_key))
        .json(&request)
        .send()
        .map_err(|e| ChatError {
            hint: e.to_string(),
        })?;

    resp.json::<ChatResponse>().map_err(|e| ChatError {
        hint: e.to_string(),
    })
}
