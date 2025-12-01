use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;
use reqwest::blocking::Client;
use std::time::Duration;

#[derive(Debug, Error)]
#[error(
    "\
Error happens at embedding.
INFORMATION: hint = {hint}"
)]
pub struct EmbeddingError {
    pub hint: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EmbeddingData {
    pub object: String,
    pub embedding: Vec<f32>,
    pub index: i32,
}

#[derive(Debug, Serialize, Clone)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: String,
    pub encoding_format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

impl EmbeddingRequest {
    pub fn new(model: String, input: String, encoding_format: String) -> Self {
        Self {
            model,
            input,
            encoding_format,
            dimensions: None,
            user: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: Usage,
}

impl EmbeddingResponse {
    pub fn try_pop_embedding(mut self) -> Result<Vec<f32>, EmbeddingError> {
        match self.data.pop() {
            Some(d) => Ok(d.embedding),
            None => Err(EmbeddingError {
                hint: "no embedding from service".to_string(),
            }),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Usage {
    pub prompt_tokens: i32,
    pub total_tokens: i32,
}


pub struct OpenAIOptions {
    pub base_url: String,
    pub api_key: String,
}

pub fn openai_embedding(
    input: String,
    model: String,
    opt: OpenAIOptions,
) -> Result<EmbeddingResponse, EmbeddingError> {
    let url = format!("{}/embeddings", opt.base_url);
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| EmbeddingError {
            hint: e.to_string(),
        })?;

    let request: EmbeddingRequest = EmbeddingRequest::new(model.to_string(), input, "float".to_string());
    let resp = client
        .post(url)
        .header("Authorization", format!("Bearer {}", opt.api_key))
        .json(&request)
        .send()
        .map_err(|e| EmbeddingError {
            hint: e.to_string(),
        })?;

    match resp.json::<EmbeddingResponse>() {
        Ok(c) => Ok(c),
        Err(e) => Err(EmbeddingError {
            hint: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::openai::EmbeddingData;
    use crate::openai::Usage;

    use super::openai_embedding;
    use super::EmbeddingResponse;
    use super::OpenAIOptions;
    use httpmock::Method::POST;
    use httpmock::MockServer;

    fn mock_server(resp: EmbeddingResponse) -> MockServer {
        let server = MockServer::start();
        let data = serde_json::to_string(&resp).unwrap();
        let _ = server.mock(|when, then| {
            when.method(POST).path("/embeddings");
            then.status(200)
                .header("content-type", "text/html; charset=UTF-8")
                .body(data);
        });
        server
    }

    #[test]
    fn test_openai_embedding_successful() {
        let embedding = vec![1.0, 2.0, 3.0];
        let resp = EmbeddingResponse {
            object: "mock-object".to_string(),
            data: vec![EmbeddingData {
                object: "mock-object".to_string(),
                embedding: embedding.clone(),
                index: 0,
            }],
            model: "mock-model".to_string(),
            usage: Usage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        };
        let server = mock_server(resp);

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_ok());
        let real_embedding = real_resp.unwrap().try_pop_embedding();
        assert!(real_embedding.is_ok());
    }

    #[test]
    fn test_openai_embedding_empty_embedding() {
        let resp = EmbeddingResponse {
            object: "mock-object".to_string(),
            data: vec![],
            model: "mock-model".to_string(),
            usage: Usage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        };
        let server = mock_server(resp);

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_ok());
        let real_embedding = real_resp.unwrap().try_pop_embedding();
        assert!(real_embedding.is_err());
    }

    #[test]
    fn test_openai_embedding_error() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(POST).path("/embeddings");
            then.status(502)
                .header("content-type", "text/html; charset=UTF-8")
                .body("502 Bad Gateway");
        });

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_err());
    }
}
