use std::os::raw::c_void;
use pgrx::pg_sys;

/// RawStmt 를 deep-copy
pub unsafe fn copy_node<T>(ptr: *mut T) -> *mut T {
    // PostgreSQL의 copyObject는 Raw/Parse 트리도 deep-copy 가능
    unsafe {
        pg_sys::copyObjectImpl(ptr as *mut c_void) as *mut T
    }
}