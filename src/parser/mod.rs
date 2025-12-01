mod chat_analyze;
mod hooks;

pub unsafe fn init() {
    unsafe {
        hooks::init();
    }
}
