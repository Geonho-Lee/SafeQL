pub mod openai;
pub mod fast;

use crate::openai::EmbeddingError as OpenAIError;
use crate::fast::{FastEmbedError, parse_embedding_model};
use fastembed::{EmbeddingModel, TextEmbedding};
use thiserror::Error;
use std::path::PathBuf;

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("Embed initialization error: {0}")]
    Init(String),
    #[error("Unsupported model: {0}")]
    UnsupportedModel(String),
}

/// Available embedding backends with configuration
pub enum BackendOptions {
    /// OpenAI embeddings: supply API base URL, key, and model name
    OpenAI { 
        base_url: String, 
        api_key: String, 
        model: String 
    },
    /// FastEmbed embeddings: supply model variant, optional cache directory, download progress flag, and GPU device ID
    FastEmbed { 
        model: String, 
        cache_dir: Option<PathBuf>, 
        show_download_progress: bool,
        gpu_device_id: i32,  // -1 for CPU, 0-3 for GPU
    },
}

/// Unified error type for both backends
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error(transparent)]
    OpenAI(#[from] OpenAIError),
    #[error(transparent)]
    FastEmbed(#[from] FastEmbedError),
}

/// Perform embedding for a single input using the specified backend
pub fn embed(input: String, backend: BackendOptions) -> Result<Vec<f32>, EmbeddingError> {
    match backend {
        BackendOptions::OpenAI { base_url, api_key, model } => {
            let opt = openai::OpenAIOptions { base_url, api_key };
            let resp = openai::openai_embedding(input, model, opt)?;
            resp.try_pop_embedding().map_err(EmbeddingError::from)
        }
        BackendOptions::FastEmbed { model, cache_dir, show_download_progress, gpu_device_id } => {
            let model: EmbeddingModel = parse_embedding_model(&model)
                .map_err(EmbeddingError::FastEmbed)?;
            // [TODO] - it occurs corruped double free error when using GPU embedding currently
            let mut client = if gpu_device_id >= 0 {
                // GPU 사용
                // fast::FastEmbedClient::new_with_gpu(
                //     model, 
                //     cache_dir, 
                //     show_download_progress, 
                //     gpu_device_id
                // )?
                // CPU 사용
                fast::FastEmbedClient::new(
                    model, 
                    cache_dir, 
                    show_download_progress
                )?
            } else {
                // CPU 사용
                fast::FastEmbedClient::new(
                    model, 
                    cache_dir, 
                    show_download_progress
                )?
            };
            
            client.embed(&input).map_err(EmbeddingError::from)
        }
    }
}

/// Perform batch embedding using the specified backend
pub fn embed_batch(inputs: Vec<String>, backend: BackendOptions) -> Result<Vec<Vec<f32>>, EmbeddingError> {
    match backend {
        BackendOptions::OpenAI { base_url, api_key, model } => {
            // OpenAI batch embedding would need to be implemented in openai module
            // For now, fall back to sequential embedding
            let mut results = Vec::with_capacity(inputs.len());
            for input in inputs {
                let opt = openai::OpenAIOptions { 
                    base_url: base_url.clone(), 
                    api_key: api_key.clone() 
                };
                let resp = openai::openai_embedding(input, model.clone(), opt)?;
                results.push(resp.try_pop_embedding()?);
            }
            Ok(results)
        }
        BackendOptions::FastEmbed { model, cache_dir, show_download_progress, gpu_device_id } => {
            let model: EmbeddingModel = parse_embedding_model(&model)
                .map_err(EmbeddingError::FastEmbed)?;
            
            let mut client = if gpu_device_id >= 0 {
                // GPU 사용
                fast::FastEmbedClient::new_with_gpu(
                    model, 
                    cache_dir, 
                    show_download_progress, 
                    gpu_device_id
                )?
            } else {
                // CPU 사용
                fast::FastEmbedClient::new(
                    model, 
                    cache_dir, 
                    show_download_progress
                )?
            };
            
            let text_refs: Vec<&str> = inputs.iter().map(|s| s.as_str()).collect();
            client.embed_batch(text_refs).map_err(EmbeddingError::from)
        }
    }
}

/// 모델 이름(String)으로부터 (모델 코드, 차원) 정보를 반환합니다.
pub fn get_model_info_by_name(model_name: String) -> Result<(String, usize), EmbedError> {
    if let Ok(model) = parse_embedding_model(&model_name) {
        let info = TextEmbedding::get_model_info(&model)
            .map_err(|e| EmbedError::Init(e.to_string()))?;
        return Ok((info.model_code.clone(), info.dim));
    }

    let openai_models = [
        ("text-embedding-ada-002", 1536),
        ("text-embedding-3-small", 1536),
        ("text-embedding-3-large", 3072),
    ];

    openai_models
        .iter()
        .find(|(name, _)| *name == model_name)
        .map(|(name, dim)| (name.to_string(), *dim))
        .ok_or_else(|| EmbedError::UnsupportedModel(model_name))
}