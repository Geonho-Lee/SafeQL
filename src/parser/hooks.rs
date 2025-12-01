use std::ffi::c_void;

static mut PREV_POST_PARSE_ANALYZE_START: pgrx::pg_sys::post_parse_analyze_hook_type = None;


#[pgrx::pg_guard]
unsafe extern "C" fn chat_post_parse_analyze_start(
    _pstate: *mut pgrx::pg_sys::ParseState,
    _query: *mut pgrx::pg_sys::Query,
    _jstate: *mut pgrx::pg_sys::JumbleState,
) {
    unsafe {
        if let Some(prev_post_parse_analyze_start) = PREV_POST_PARSE_ANALYZE_START {
            prev_post_parse_analyze_start(_pstate, _query, _jstate);
        }
        super::chat_analyze::convert_chat_walker(_query as *mut pgrx::pg_sys::Node, _pstate as *mut c_void);
    }
}

pub unsafe fn init() {
    unsafe {
        PREV_POST_PARSE_ANALYZE_START = pgrx::pg_sys::post_parse_analyze_hook;
        pgrx::pg_sys::post_parse_analyze_hook = Some(chat_post_parse_analyze_start);
    }
}