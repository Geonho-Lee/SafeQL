use embedding::openai::OpenAIOptions;
use embedding::BackendOptions;
use text2softql::Text2SoftQLOptions;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;
use crate::error::*;


fn parse(target: &'static GucSetting<Option<&'static CStr>>, name: &'static str) -> String {
    let value = match target.get() {
        Some(s) => s,
        None => bad_guc_literal(name, "should not be `NULL`"),
    };
    match value.to_str() {
        Ok(s) => s.to_string(),
        Err(_e) => bad_guc_literal(name, "should be a valid UTF-8 string"),
    }
}

pub fn embedding_backend_options() -> BackendOptions {
    let backend = parse(&EMBEDDING_BACKEND, "vectors.embedding_backend");
    let model_name = parse(&EMBEDDING_MODEL_NAME, "vectors.embedding_model_name");
    match backend.as_str() {
        "fastembed" => {
            let gpu_device_id = EMBEDDING_GPU_DEVICE_ID.get();
            BackendOptions::FastEmbed {
                model: model_name,
                cache_dir: None,
                show_download_progress: false,
                gpu_device_id,  // GPU ID 추가
            }
        }
        _ => {
            BackendOptions::OpenAI {
                base_url: parse(&OPENAI_BASE_URL, "vectors.openai_base_url"),
                api_key: parse(&OPENAI_API_KEY, "vectors.openai_api_key"),
                model: model_name,
            }
        }
    }
}

pub fn openai_embedding_options() -> OpenAIOptions {
    let base_url = parse(&OPENAI_BASE_URL, "vectors.openai_base_url");
    let api_key = parse(&OPENAI_API_KEY, "vectors.openai_api_key");
    OpenAIOptions { base_url, api_key }
}

pub fn text2softql_options() -> Text2SoftQLOptions {
    let base_url = parse(&TEXT_TO_SOFTQL_MODEL_URL, "vectors.text2softql_model_url");
    let model_name = parse(&TEXT_TO_SOFTQL_MODEL_NAME, "vectors.text2softql_model_name");
    let api_key = parse(&OPENAI_API_KEY, "vectors.openai_api_key");
    Text2SoftQLOptions { base_url, model_name, api_key }
}

static OPENAI_API_KEY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

static OPENAI_BASE_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"https://api.openai.com/v1"));

static EMBEDDING_BACKEND: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"fastembed"));

static EMBEDDING_MODEL_NAME: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"Xenova/bge-base-en-v1.5"));

static EMBEDDING_GPU_DEVICE_ID: GucSetting<i32> =
    GucSetting::<i32>::new(0);  // -1 = CPU, 0~3 = GPU ID

pub static VECTOR_EMBEDDING_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(512);

pub static ENABLE_SEARCH_CACHE: GucSetting<bool> =
    GucSetting::<bool>::new(true);  // 기본값: true (캐시 활성화)

static TEXT_TO_SOFTQL_MODEL_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"https://api.openai.com/v1"));

static TEXT_TO_SOFTQL_MODEL_NAME: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"gpt-4o"));


pub unsafe fn init() {
    GucRegistry::define_string_guc(
        "vectors.openai_api_key",
        "The API key of OpenAI.",
        "",
        &OPENAI_API_KEY,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.openai_base_url",
        "The base url of OpenAI or compatible server.",
        "",
        &OPENAI_BASE_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.embedding_backend",
        "The model backend for embedding.",
        "",
        &EMBEDDING_BACKEND,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.embedding_model_name",
        "The model name for embedding.",
        "",
        &EMBEDDING_MODEL_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.embedding_gpu_device_id",
        "GPU device ID for embedding (-1 for CPU, 0-3 for GPU).",
        "Set to -1 to use CPU, or 0-3 to use specific GPU device.",
        &EMBEDDING_GPU_DEVICE_ID,
        -1,      // min value
        3,       // max value
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.vector_embedding_batch_size",
        "Batch size for vector embedding generation",
        "Controls how many texts are embedded in a single GPU batch. Higher values improve GPU utilization but require more memory. Default is 4096.",
        &VECTOR_EMBEDDING_BATCH_SIZE,
        256,        // min value
        32768,      // max value
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.enable_search_cache",
        "Enable or disable search caching.",
        "When enabled, search results are cached for improved performance.",
        &ENABLE_SEARCH_CACHE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.text2softql_model_url",
        "The url for the text2softql model.",
        "",
        &TEXT_TO_SOFTQL_MODEL_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.text2softql_model_name",
        "The model name for text2softql.",
        "",
        &TEXT_TO_SOFTQL_MODEL_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
}