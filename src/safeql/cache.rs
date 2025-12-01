use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::ffi::CString;
use crate::gucs::model::ENABLE_SEARCH_CACHE;

/// 캐시 엔트리 구조체 (C-compatible)
#[repr(C, align(8))]
#[derive(Debug, Clone)]
struct CacheEntry {
    key_hash: u64,
    query_type_hash: u64,
    created_at: u64,
    last_accessed: u64,
    data_len: usize,
    // 가변 길이 데이터: key_str + query_type_str + data_str이 뒤에 따라옴
}

/// 공유 메모리 캐시 헤더
#[repr(C, align(8))]
struct CacheHeader {
    magic: u32,           // 매직 넘버로 유효성 검사
    max_entries: u32,     // 최대 엔트리 수
    current_entries: u32, // 현재 엔트리 수
    next_cleanup_time: u64, // 다음 정리 시간
    lock_tranche_id: i32, // LWLock tranche ID
    // LWLock은 바로 이 구조체 뒤에 위치
}

const CACHE_MAGIC: u32 = 0x56454354; // "VECT"
const MAX_CACHE_ENTRIES: u32 = 100000;
const CACHE_CLEANUP_INTERVAL: u64 = 3600; // 1시간마다 정리
const ENTRY_SLOT_SIZE: usize = 7200; // 각 엔트리 슬롯의 고정 크기
const MAX_DATA_SIZE: usize = ENTRY_SLOT_SIZE - size_of::<CacheEntry>() - 128; // 안전 마진

// LWLock tranche ID (전역으로 할당받음)
static mut CACHE_LOCK_TRANCHE_ID: i32 = 0;

/// RAII 스타일 락 가드 - 제대로 구현
struct CacheLockGuard {
    lock: *mut pgrx::pg_sys::LWLock,
}

impl CacheLockGuard {
    /// 공유(읽기) 락 획득
    unsafe fn acquire_shared() -> Option<Self> {
        let lock = unsafe { SharedMemoryCache::get_cache_lock() };
        if lock.is_null() {
            // pgrx::notice!("Failed to get cache lock pointer");
            return None;
        }
        
        unsafe {
            pgrx::pg_sys::LWLockAcquire(lock, pgrx::pg_sys::LWLockMode::LW_SHARED);
        }
        Some(Self { lock })
    }
    
    /// 배타(쓰기) 락 획득
    unsafe fn acquire_exclusive() -> Option<Self> {
        let lock = unsafe { SharedMemoryCache::get_cache_lock() };
        if lock.is_null() {
            // pgrx::notice!("Failed to get cache lock pointer");
            return None;
        }
        
        unsafe {
            pgrx::pg_sys::LWLockAcquire(lock, pgrx::pg_sys::LWLockMode::LW_EXCLUSIVE);
        }
        Some(Self { lock })
    }
}

impl Drop for CacheLockGuard {
    fn drop(&mut self) {
        if !self.lock.is_null() {
            unsafe {
                pgrx::pg_sys::LWLockRelease(self.lock);
            }
        }
    }
}

/// 공유 메모리 캐시 관리자
struct SharedMemoryCache {
    _shmem_size: usize,
}

impl SharedMemoryCache {
    fn new() -> Self {
        let shmem_size = size_of::<CacheHeader>() + 
                        size_of::<pgrx::pg_sys::LWLock>() +
                        (ENTRY_SLOT_SIZE * MAX_CACHE_ENTRIES as usize);
        
        Self { _shmem_size: shmem_size }
    }
    
    /// 캐시 LWLock 포인터 가져오기
    unsafe fn get_cache_lock() -> *mut pgrx::pg_sys::LWLock {
        let header = unsafe { Self::get_cache_header_ptr() };
        if header.is_null() {
            return std::ptr::null_mut();
        }
        
        // 헤더 바로 뒤에 LWLock이 위치
        unsafe {
            (header as *mut u8).add(size_of::<CacheHeader>()) as *mut pgrx::pg_sys::LWLock
        }
    }
    
    /// 캐시 헤더 포인터만 가져오기 (락 없이)
    unsafe fn get_cache_header_ptr() -> *mut CacheHeader {
        let shmem_name = CString::new("pg_vector_similarity_cache").unwrap();
        let mut found = false;
        
        let cache_size = size_of::<CacheHeader>() + 
                        size_of::<pgrx::pg_sys::LWLock>() + 
                        (ENTRY_SLOT_SIZE * MAX_CACHE_ENTRIES as usize);
        
        let shmem = unsafe {
            pgrx::pg_sys::ShmemInitStruct(
                shmem_name.as_ptr(),
                cache_size,
                &mut found as *mut bool
            )
        };
        
        if shmem.is_null() {
            return std::ptr::null_mut();
        }
        
        shmem as *mut CacheHeader
    }
    
    /// 엔트리 시작 포인터 계산
    unsafe fn get_entries_start(header: *mut CacheHeader) -> *mut u8 {
        unsafe {
            (header as *mut u8)
                .add(size_of::<CacheHeader>())
                .add(size_of::<pgrx::pg_sys::LWLock>())
        }
    }
    
    /// 캐시에서 항목 검색
    fn get(&self, cache_key: &str) -> Option<String> {
        // 캐시가 비활성화되어 있으면 항상 miss
        if !ENABLE_SEARCH_CACHE.get() {
            return None;
        }
        
        unsafe {
            // 공유 락 획득
            let _guard = CacheLockGuard::acquire_shared()?;
            
            let header = Self::get_cache_header_ptr();
            if header.is_null() || (*header).magic != CACHE_MAGIC {
                // pgrx::notice!("Invalid cache header");
                return None;
            }
            
            let key_hash = Self::hash_string(cache_key);
            let entries_start = Self::get_entries_start(header);
            
            // 엔트리 검색
            let current_entries = (*header).current_entries;
            for i in 0..current_entries {
                let entry_offset = i as usize * ENTRY_SLOT_SIZE;
                let entry = entries_start.add(entry_offset) as *mut CacheEntry;
                
                let entry_key_hash = (*entry).key_hash;
                if entry_key_hash == key_hash {
                    // 데이터 크기 검증
                    let data_len = (*entry).data_len;
                    if data_len > MAX_DATA_SIZE {
                        // pgrx::notice!("Warning: Invalid data length in cache entry: {}", data_len);
                        continue;
                    }
                    
                    let data_ptr = (entry as *mut u8).add(size_of::<CacheEntry>());
                    let data_slice = std::slice::from_raw_parts(data_ptr, data_len);
                    
                    return String::from_utf8(data_slice.to_vec()).ok();
                }
            }
            
            None
        }
    }
    
    /// 캐시에 항목 저장
    fn set(&self, cache_key: &str, query_type: &str, data: &str) -> Result<(), &'static str> {
        // 캐시가 비활성화되어 있으면 저장하지 않음
        if !ENABLE_SEARCH_CACHE.get() {
            return Ok(()); // 에러는 반환하지 않음
        }
        
        // 데이터 크기 검증 (락 획득 전에 먼저 체크)
        if data.len() > MAX_DATA_SIZE {
            pgrx::log!("Data too large for cache: {} bytes (max: {})", data.len(), MAX_DATA_SIZE);
            pgrx::log!("Cache key: {}", cache_key);
            pgrx::log!("Query type: {}", query_type);
            return Err("Data too large for cache");
        }
        
        unsafe {
            // 배타 락 획득
            let _guard = CacheLockGuard::acquire_exclusive()
                .ok_or("Failed to acquire exclusive lock")?;
            
            let header = Self::get_cache_header_ptr();
            if header.is_null() || (*header).magic != CACHE_MAGIC {
                return Err("Invalid cache header");
            }
            
            // 공간이 부족하면 정리
            if (*header).current_entries >= (*header).max_entries {
                pgrx::log!("Cache full, cleaning up old entries");
                self.cleanup_old_entries_unlocked(header)?;
                
                // 정리 후에도 공간이 없으면 실패
                if (*header).current_entries >= (*header).max_entries {
                    return Err("Cache full after cleanup");
                }
            }
            
            let key_hash = Self::hash_string(cache_key);
            let query_type_hash = Self::hash_string(query_type);
            let entries_start = Self::get_entries_start(header);
            
            // 새 엔트리 추가
            let current_entries = (*header).current_entries;
            let entry_offset = current_entries as usize * ENTRY_SLOT_SIZE;
            let entry = entries_start.add(entry_offset) as *mut CacheEntry;
            
            let now = current_timestamp();
            (*entry).key_hash = key_hash;
            (*entry).query_type_hash = query_type_hash;
            (*entry).created_at = now;
            (*entry).last_accessed = now;
            (*entry).data_len = data.len();
            
            // 데이터 복사 - 크기 재검증
            if data.len() > MAX_DATA_SIZE {
                return Err("Data size validation failed");
            }
            
            let data_ptr = (entry as *mut u8).add(size_of::<CacheEntry>());
            std::ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
            
            (*header).current_entries += 1;
            
            // let new_count = (*header).current_entries;
            // pgrx::notice!("Cached entry added: key_hash={:x}, size={} bytes, total_entries={}", 
            //           key_hash, data.len(), new_count);
            
            Ok(())
        }
    }
    
    /// 오래된 엔트리 정리 (이미 락이 잡힌 상태)
    unsafe fn cleanup_old_entries_unlocked(&self, header: *mut CacheHeader) -> Result<(), &'static str> {
        if header.is_null() || unsafe { (*header).magic } != CACHE_MAGIC {
            return Err("Invalid cache header");
        }
        
        let now = current_timestamp();
        let cutoff_time = now.saturating_sub(7 * 24 * 3600); // 7일 이전
        
        let entries_start = unsafe { Self::get_entries_start(header) };
        let mut write_index = 0;
        let initial_count = unsafe { (*header).current_entries };
        
        // 유효한 엔트리들을 앞으로 이동
        for read_index in 0..initial_count as usize {
            let read_entry = unsafe {
                entries_start.add(read_index * ENTRY_SLOT_SIZE) as *mut CacheEntry
            };
            
            let last_accessed = unsafe { (*read_entry).last_accessed };
            if last_accessed >= cutoff_time {
                if read_index != write_index {
                    let write_entry = unsafe {
                        entries_start.add(write_index * ENTRY_SLOT_SIZE) as *mut CacheEntry
                    };
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            read_entry as *const u8, 
                            write_entry as *mut u8, 
                            ENTRY_SLOT_SIZE
                        );
                    }
                }
                write_index += 1;
            }
        }
        
        // let removed = initial_count - write_index as u32;
        unsafe {
            (*header).current_entries = write_index as u32;
            (*header).next_cleanup_time = now + CACHE_CLEANUP_INTERVAL;
        }
        
        // pgrx::notice!("Cache cleanup: removed {} old entries, {} remaining", removed, write_index);
        
        Ok(())
    }
    
    /// 오래된 엔트리 정리 (공개 인터페이스)
    fn _cleanup_old_entries(&self) -> Result<(), &'static str> {
        unsafe {
            let _guard = CacheLockGuard::acquire_exclusive()
                .ok_or("Failed to acquire exclusive lock")?;
            
            let header = Self::get_cache_header_ptr();
            if header.is_null() {
                return Err("Failed to get cache header");
            }
            
            self.cleanup_old_entries_unlocked(header)
        }
    }
    
    /// 문자열 해시
    fn hash_string(s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }
    
    /// 캐시 통계 수집
    fn _get_stats(&self) -> Vec<(String, i64, Option<f64>)> {
        let _guard = match unsafe { CacheLockGuard::acquire_shared() } {
            Some(g) => g,
            None => return vec![],
        };
        
        let header = unsafe { Self::get_cache_header_ptr() };
        if header.is_null() || unsafe { (*header).magic } != CACHE_MAGIC {
            return vec![];
        }
        
        let mut stats = std::collections::HashMap::new();
        let entries_start = unsafe { Self::get_entries_start(header) };
        let now = current_timestamp();
        
        let current_entries = unsafe { (*header).current_entries };
        for i in 0..current_entries as usize {
            let entry = unsafe {
                entries_start.add(i * ENTRY_SLOT_SIZE) as *mut CacheEntry
            };
            let query_type_hash = unsafe { (*entry).query_type_hash };
            let created_at = unsafe { (*entry).created_at };
            
            let query_type = format!("type_{}", query_type_hash);
            let age_hours = (now.saturating_sub(created_at)) as f64 / 3600.0;
            
            stats.entry(query_type)
                .and_modify(|(count, total_age): &mut (i64, f64)| {
                    *count += 1;
                    *total_age += age_hours;
                })
                .or_insert((1, age_hours));
        }
        
        stats.into_iter()
            .map(|(query_type, (count, total_age))| {
                let avg_age = if count > 0 { Some(total_age / count as f64) } else { None };
                (query_type, count, avg_age)
            })
            .collect()
    }
    
    /// 캐시 클리어
    fn _clear(&self, days_old: Option<i32>) -> i64 {
        let _guard = match unsafe { CacheLockGuard::acquire_exclusive() } {
            Some(g) => g,
            None => {
                // pgrx::notice!("Failed to acquire lock for cache clear");
                return 0;
            }
        };
        
        let header = unsafe { Self::get_cache_header_ptr() };
        if header.is_null() || unsafe { (*header).magic } != CACHE_MAGIC {
            return 0;
        }
        
        let days = days_old.unwrap_or(0);
        
        if days <= 0 {
            let count = unsafe { (*header).current_entries } as i64;
            unsafe {
                (*header).current_entries = 0;
            }
            // pgrx::notice!("Cache cleared: {} entries removed", count);
            return count;
        }
        
        let now = current_timestamp();
        let cutoff_time = now.saturating_sub(days as u64 * 24 * 3600);
        
        let entries_start = unsafe { Self::get_entries_start(header) };
        let mut write_index = 0;
        let initial_count = unsafe { (*header).current_entries };
        
        for read_index in 0..initial_count as usize {
            let read_entry = unsafe {
                entries_start.add(read_index * ENTRY_SLOT_SIZE) as *mut CacheEntry
            };
            
            let last_accessed = unsafe { (*read_entry).last_accessed };
            if last_accessed >= cutoff_time {
                if read_index != write_index {
                    let write_entry = unsafe {
                        entries_start.add(write_index * ENTRY_SLOT_SIZE) as *mut CacheEntry
                    };
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            read_entry as *const u8, 
                            write_entry as *mut u8, 
                            ENTRY_SLOT_SIZE
                        );
                    }
                }
                write_index += 1;
            }
        }
        
        let removed = initial_count - write_index as u32;
        unsafe {
            (*header).current_entries = write_index as u32;
        }
        
        // pgrx::notice!("Cache cleared: {} entries older than {} days removed", removed, days);
        
        removed as i64
    }
}

/// 전역 캐시 인스턴스
static SHARED_CACHE: once_cell::sync::Lazy<SharedMemoryCache> = 
    once_cell::sync::Lazy::new(|| SharedMemoryCache::new());

/// 현재 시간을 Unix timestamp로 반환
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

/// 캐시 키를 생성하는 함수
pub fn generate_cache_key(query_type: &str, params: &[&str]) -> String {
    let mut hasher = DefaultHasher::new();
    query_type.hash(&mut hasher);
    for param in params {
        param.hash(&mut hasher);
    }
    format!("{}_{:016x}", query_type, hasher.finish())
}

/// 캐시에서 결과를 가져오는 함수
pub fn get_cached_result(cache_key: &str) -> Option<String> {
    SHARED_CACHE.get(cache_key)
}

/// 결과를 캐시에 저장하는 함수
pub fn store_cached_result(cache_key: &str, query_type: &str, result_json: &str) -> Result<(), &'static str> {
    SHARED_CACHE.set(cache_key, query_type, result_json)
}

/// 캐시 통계 조회
pub fn _get_cache_stats() -> Vec<(String, i64, Option<f64>)> {
    SHARED_CACHE._get_stats()
}

/// 캐시 정리
pub fn _cleanup_cache() -> Result<(), &'static str> {
    SHARED_CACHE._cleanup_old_entries()
}

/// 캐시 클리어
pub fn _clear_cache(days_old: Option<i32>) -> i64 {
    SHARED_CACHE._clear(days_old)
}

/// 초기화 함수
pub unsafe fn init() {
    register_shmem_startup_hook();
}

fn register_shmem_startup_hook() {
    static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
    static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;
    
    unsafe extern "C" fn shmem_request_hook() {
        unsafe {
            if let Some(prev_hook) = PREV_SHMEM_REQUEST_HOOK {
                prev_hook();
            }
        }
        
        let cache_size = size_of::<CacheHeader>() + 
                        size_of::<pgrx::pg_sys::LWLock>() +
                        (ENTRY_SLOT_SIZE * MAX_CACHE_ENTRIES as usize);
        
        unsafe {
            pgrx::pg_sys::RequestAddinShmemSpace(cache_size);
        }
        
        // pgrx::notice!("Requested shared memory space: {} bytes", cache_size);
    }
    
    unsafe extern "C" fn shmem_startup_hook() {
        unsafe {
            if let Some(prev_hook) = PREV_SHMEM_STARTUP_HOOK {
                prev_hook();
            }
        }
        
        initialize_shared_cache();
    }
    
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pgrx::pg_sys::shmem_request_hook;
        pgrx::pg_sys::shmem_request_hook = Some(shmem_request_hook);
        
        PREV_SHMEM_STARTUP_HOOK = pgrx::pg_sys::shmem_startup_hook;
        pgrx::pg_sys::shmem_startup_hook = Some(shmem_startup_hook);
    }
}

/// 공유 캐시 초기화
fn initialize_shared_cache() {
    // tranche ID 할당
    if unsafe { CACHE_LOCK_TRANCHE_ID } == 0 {
        unsafe {
            CACHE_LOCK_TRANCHE_ID = pgrx::pg_sys::LWLockNewTrancheId();
        }
        // pgrx::notice!("Allocated LWLock tranche ID: {}", unsafe { CACHE_LOCK_TRANCHE_ID });
    }
    
    let shmem_name = CString::new("pg_vector_similarity_cache").unwrap();
    let cache_size = size_of::<CacheHeader>() + 
                    size_of::<pgrx::pg_sys::LWLock>() +
                    (ENTRY_SLOT_SIZE * MAX_CACHE_ENTRIES as usize);
    
    let mut found = false;
    let shmem = unsafe {
        pgrx::pg_sys::ShmemInitStruct(
            shmem_name.as_ptr(),
            cache_size,
            &mut found as *mut bool
        )
    };
    
    if shmem.is_null() {
        // pgrx::notice!("Failed to initialize shared memory for cache");
        return;
    }
    
    let header = shmem as *mut CacheHeader;
    
    // LWLock 초기化
    let lock = unsafe {
        (header as *mut u8).add(size_of::<CacheHeader>()) 
            as *mut pgrx::pg_sys::LWLock
    };
    unsafe {
        pgrx::pg_sys::LWLockInitialize(lock, CACHE_LOCK_TRANCHE_ID);
    }
    
    // 첫 번째 초기화인 경우
    if !found {
        unsafe {
            (*header).magic = CACHE_MAGIC;
            (*header).max_entries = MAX_CACHE_ENTRIES;
            (*header).current_entries = 0;
            (*header).next_cleanup_time = current_timestamp() + CACHE_CLEANUP_INTERVAL;
            (*header).lock_tranche_id = CACHE_LOCK_TRANCHE_ID;
        }
        
        // 모든 엔트리 영역을 0으로 초기화
        let entries_start = unsafe {
            (header as *mut u8)
                .add(size_of::<CacheHeader>())
                .add(size_of::<pgrx::pg_sys::LWLock>())
        };
        unsafe {
            std::ptr::write_bytes(
                entries_start, 
                0, 
                ENTRY_SLOT_SIZE * MAX_CACHE_ENTRIES as usize
            );
        }
        
        pgrx::log!(
            "Vector similarity cache initialized: {} entries, {} MB total", 
            MAX_CACHE_ENTRIES,
            cache_size / (1024 * 1024)
        );
    } else {
        let current_entries = unsafe { (*header).current_entries };
        pgrx::log!(
            "Vector similarity cache reattached: {} entries currently stored",
            current_entries
        );
    }
}