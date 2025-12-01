use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use ort::execution_providers::{CUDAExecutionProvider, CPUExecutionProvider};
use thiserror::Error;
use std::path::PathBuf;

/// Errors from fastembed-rs
#[derive(Debug, Error)]
pub enum FastEmbedError {
    #[error("FastEmbed initialization error: {0}")]
    Init(String),
    #[error("Unsupported model: {0}")]
    UnsupportedModel(String),
    #[error("FastEmbed embedding error: {0}")]
    Embed(String),
}

/// Client wrapper around fastembed TextEmbedding
pub struct FastEmbedClient {
    inner: TextEmbedding,
}

impl FastEmbedClient {
    /// Create a new client with GPU support
    pub fn new_with_gpu(
        model: EmbeddingModel,
        cache_dir: Option<PathBuf>,
        show_download_progress: bool,
        gpu_device_id: i32,
    ) -> Result<Self, FastEmbedError> {
        // GPU 디바이스 선택
        std::env::set_var("CUDA_VISIBLE_DEVICES", gpu_device_id.to_string());
        
        let mut opts = InitOptions::new(model);
        if let Some(dir) = cache_dir {
            opts = opts.with_cache_dir(dir);
        }
        
        // Execution providers 생성
        let providers = vec![
            CUDAExecutionProvider::default()
                .with_device_id(gpu_device_id)
                .build(),
            CPUExecutionProvider::default()
                .build(),
        ];
        
        opts = opts
            .with_show_download_progress(show_download_progress)
            .with_execution_providers(providers);
        
        let inner = TextEmbedding::try_new(opts)
            .map_err(|e| FastEmbedError::Init(e.to_string()))?;
        
        Ok(FastEmbedClient { inner })
    }

    /// Create a new client with CPU only
    pub fn new(
        model: EmbeddingModel,
        cache_dir: Option<PathBuf>,
        show_download_progress: bool,
    ) -> Result<Self, FastEmbedError> {
        let mut opts = InitOptions::new(model);
        if let Some(dir) = cache_dir {
            opts = opts.with_cache_dir(dir);
        }
        opts = opts.with_show_download_progress(show_download_progress);
        let inner = TextEmbedding::try_new(opts)
            .map_err(|e| FastEmbedError::Init(e.to_string()))?;
        Ok(FastEmbedClient { inner })
    }

    /// Embed a single piece of text
    pub fn embed(&mut self, text: &str) -> Result<Vec<f32>, FastEmbedError> {
        let docs = vec![text];
        let mut embeddings = self.inner.embed(docs, None)
            .map_err(|e| FastEmbedError::Embed(e.to_string()))?;
        embeddings.pop().ok_or_else(|| FastEmbedError::Embed("no embedding returned".to_string()))
    }

    /// Embed multiple texts in batch
    pub fn embed_batch(&mut self, texts: Vec<&str>) -> Result<Vec<Vec<f32>>, FastEmbedError> {
        let batch_size = texts.len();
        self.inner.embed(texts, Some(batch_size))
            .map_err(|e| FastEmbedError::Embed(e.to_string()))
    }
}

/// 문자열로부터 `EmbeddingModel` enum 을 찾아 반환합니다.
pub fn parse_embedding_model(model_name: &str) -> Result<EmbeddingModel, FastEmbedError> {
    TextEmbedding::list_supported_models()
        .into_iter()
        .find(|info| info.model_code == model_name)
        .map(|info| info.model)
        .ok_or_else(|| FastEmbedError::UnsupportedModel(model_name.to_string()))
}