pub mod model;
pub mod executing;
pub mod internal;
pub mod planning;
pub mod parser;

pub unsafe fn init() {
    unsafe {
        planning::init();
        internal::init();
        executing::init();
        model::init();
        parser::init();
        #[cfg(feature = "pg14")]
        pgrx::pg_sys::EmitWarningsOnPlaceholders(c"vectors".as_ptr());
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::MarkGUCPrefixReserved(c"vectors".as_ptr());
        pgrx::pg_sys::MarkGUCPrefixReserved(c"safeql".as_ptr());
    }
    
    // ONNX Runtime 라이브러리 경로 설정
    const ORT_LIB: &str = "/opt/onnxruntime/lib/libonnxruntime.so";
    std::env::set_var("ORT_DYLIB_PATH", ORT_LIB);
    
    // CUDA 라이브러리 경로 추가 (12.3으로 변경)
    let cuda_lib_paths = "/usr/local/cuda-12.3/lib64:/usr/local/cuda/lib64:/usr/lib/x86_64-linux-gnu:/opt/onnxruntime/lib";
    let current = std::env::var("LD_LIBRARY_PATH").unwrap_or_default();
    let new_path = if current.is_empty() {
        cuda_lib_paths.to_string()
    } else {
        format!("{}:{}", cuda_lib_paths, current)
    };
    std::env::set_var("LD_LIBRARY_PATH", new_path);
}