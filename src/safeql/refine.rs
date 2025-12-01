use std::ffi::{CString, CStr};
use std::os::raw::c_void;
use regex::Regex;
use pgrx::{memcx, pg_sys};
use pgrx::list::List;

use super::score::{
    list_tables_by_similarity, 
    list_columns_by_similarity, 
    find_all_joinable_tables,
    find_joinable_tables_for_column, 
    find_compatible_columns_for_operator,
    get_typecast_refinements_for_operator,
    find_compatible_arguments_for_function,
    get_typecast_refinements_for_function,
    find_similar_functions,
    find_tables_with_exact_column,
    find_similar_values_for_literal,
    JoinCondition,
    OperandPosition,
    ArgumentPosition,
    OperatorInfo,
    FunctionInfo,
};
use super::utils::copy_node;
use crate::gucs::parser::{
    TABLE_REFINEMENT_WEIGHT,
    COLUMN_REFINEMENT_WEIGHT,
    TABLE_FOR_COLUMN_WEIGHT,
    COLUMN_TABLE_REFERENCE_WEIGHT,
    JOIN_REFINEMENT_WEIGHT,
    OPERAND_REFINEMENT_WEIGHT,
    TYPECAST_REFINEMENT_WEIGHT,
    ARGUMENT_REFINEMENT_WEIGHT,
    FUNCTION_NAME_REFINEMENT_WEIGHT,
    COLUMN_AMBIGUITY_REFINEMENT_WEIGHT,
    VALUE_REFINEMENT_WEIGHT,
};

/* ------------------------------------------------
컬럼 오퍼랜드 정보
------------------------------------------------ */
#[derive(Debug, Clone)]
pub struct ColumnOperand {
    pub table_name: Option<String>,
    pub column_name: String,
}

/* ------------------------------------------------
FROM절의 모든 테이블에 대해 refinement 수행 (EmptyResult용)
------------------------------------------------ */
pub fn generate_table_refinements_for_all_from_tables_raw(
    orig: *mut pg_sys::RawStmt,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    // FROM절의 모든 테이블 추출
    let from_tables = extract_all_tables_from_raw(orig);
    
    if from_tables.is_empty() {
        return out;
    }
    
    let weight = TABLE_REFINEMENT_WEIGHT.get() as f32;
    
    // 각 테이블에 대해 유사한 테이블로 교체 시도
    for table_info in from_tables {
        let target_table = &table_info.table_name;
        
        // 유사한 테이블 후보들 찾기
        let candidates = list_tables_by_similarity(target_table);
        
        for (fq, distance) in candidates {
            let candidate_table = if let Some((_, rel)) = split_schema_rel(&fq) {
                rel
            } else {
                fq.clone()
            };
            
            // 자기 자신은 스킵
            if candidate_table.to_ascii_lowercase() == target_table.to_ascii_lowercase() {
                continue;
            }
            
            // GUC 가중치를 적용하여 priority 계산
            let additional_priority = ((distance * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            if let Some((_, rel)) = split_schema_rel(&fq) {
                let cloned = unsafe { copy_node(orig) };
                unsafe {
                    // 테이블 교체 (alias 고려)
                    replace_table_intelligently(cloned, target_table, &rel);
                }
                out.push((cumulative_priority, cloned));
                
                // pgrx::notice!("SafeQL: Generated table refinement for FROM clause: {} -> {} (distance: {})", 
                //     target_table, rel, distance);
            }
        }
    }
    
    // pgrx::notice!("SafeQL: Generated {} table refinements for all FROM clause tables", out.len());
    
    out
}

/* ------------------------------------------------
CASE 1) FROM Rel -> FROM Rel' - 테이블 refinement 수행
------------------------------------------------ */
pub fn generate_table_refinements_raw(
    orig: *mut pg_sys::RawStmt, 
    missing_rel: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    // 현재 FROM절의 모든 테이블명과 alias 추출
    let existing_tables = extract_all_tables_from_raw(orig);
    let existing_table_names: std::collections::HashSet<String> = existing_tables.iter()
        .flat_map(|t| {
            let mut names = vec![t.table_name.to_ascii_lowercase()];
            if let Some(ref alias) = t.alias {
                names.push(alias.to_ascii_lowercase());
            }
            names
        })
        .collect();

    // missing_rel이 alias인지 확인하고 실제 테이블명 찾기
    let target_table_name = unsafe { 
        find_actual_table_name(orig, missing_rel)
            .unwrap_or_else(|| missing_rel.to_string())
    };
    
    let candidates = list_tables_by_similarity(&target_table_name);
    let mut out = Vec::with_capacity(candidates.len());

    for (fq, distance) in candidates {
        let candidate_table = if let Some((_, rel)) = split_schema_rel(&fq) {
            rel
        } else {
            fq.clone()
        };
        
        // 현재 FROM절에 이미 있는 테이블이면 스킵
        if existing_table_names.contains(&candidate_table.to_ascii_lowercase()) {
            continue;
        }

        let weight = TABLE_REFINEMENT_WEIGHT.get() as f32;
        let additional_priority = ((distance * 100.0) * weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        if let Some((_schema, rel)) = split_schema_rel(&fq) {
            let cloned = unsafe { copy_node(orig) };
            unsafe {
                // missing_rel이 alias였다면 alias로 추가, 아니면 실제 테이블명으로 교체
                if missing_rel != target_table_name {
                    // missing_rel은 alias, target_table_name은 실제 테이블명
                    add_table_with_alias(cloned, &rel, Some(missing_rel));
                } else {
                    // missing_rel이 실제 테이블명
                    replace_table_intelligently(cloned, missing_rel, &rel);
                }
            }
            out.push((cumulative_priority, cloned));
        }
    }
    
    out
}

/// FROM절에 alias와 함께 테이블 추가
unsafe fn add_table_with_alias(
    raw: *mut pg_sys::RawStmt,
    table_name: &str,
    alias: Option<&str>,
) {
    unsafe {
        if let Some(select_stmt) = find_select_stmt(raw) {
            let table_cstr = CString::new(table_name).unwrap();
            let alias_cstr = alias.map(|a| CString::new(a).unwrap());
            
            let range_var = if let Some(ref alias_c) = alias_cstr {
                create_range_var_with_alias(&table_cstr, Some(alias_c))
            } else {
                create_range_var(&table_cstr)
            };
            
            add_table_to_from_list((*select_stmt).fromClause, range_var as *mut pg_sys::Node);
        }
    }
}

/// alias 지원하는 RangeVar 생성
unsafe fn create_range_var_with_alias(
    table_name: &CString, 
    alias: Option<&CString>
) -> *mut pg_sys::RangeVar {
    unsafe {
        let range_var = create_range_var(table_name);
        
        if let Some(alias_cstr) = alias {
            let alias_node = pg_sys::palloc0(size_of::<pg_sys::Alias>()) as *mut pg_sys::Alias;
            (*alias_node).type_ = pg_sys::NodeTag::T_Alias;
            (*alias_node).aliasname = pg_sys::pstrdup(alias_cstr.as_ptr());
            (*alias_node).colnames = std::ptr::null_mut();
            (*range_var).alias = alias_node;
        }
        
        range_var
    }
}


/* ------------------------------------------------
CASE 2) SELECT Att -> SELECT Att' - 칼럼 refinement 수행, table 있으면 그 안에서 refinement
------------------------------------------------ */
/// Column refinements 생성
pub fn generate_column_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    table_name: Option<&str>,
    missing_col: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    // table_name이 있으면 해당 테이블에서만, 없으면 전체에서 검색
    let candidates = if let Some(table) = table_name {
        // 특정 테이블이 지정된 경우: 실제 테이블명으로 변환 후 해당 테이블에서만 검색
        let actual_table_name = unsafe { 
            find_actual_table_name(orig, table)
                .unwrap_or_else(|| table.to_string())
        };
        list_columns_by_similarity(Some(&actual_table_name), missing_col, false)
    } else {
        // 테이블이 지정되지 않은 경우: 모든 테이블에서 검색
        list_columns_by_similarity(None, missing_col, false)
    };
    
    let mut out = Vec::with_capacity(candidates.len());
    
    for (col_name, candidate_table_name, distance) in candidates {
        // table_name이 지정된 경우 해당 테이블의 컬럼만 사용
        if let Some(specified_table) = table_name {
            let actual_table_name = unsafe { 
                find_actual_table_name(orig, specified_table)
                    .unwrap_or_else(|| specified_table.to_string())
            };
            
            // 후보 컬럼이 지정된 테이블에서 온 것이 아니면 스킵
            if candidate_table_name.to_ascii_lowercase() != actual_table_name.to_ascii_lowercase() {
                continue;
            }
        }
        
        // GUC 가중치를 적용하여 priority 계산
        let weight = COLUMN_REFINEMENT_WEIGHT.get() as f32;
        let additional_priority = ((distance * 100.0) * weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        let cloned = unsafe { copy_node(orig) };
        unsafe { replace_column_ref_inplace(cloned, missing_col, &col_name, table_name); }
        out.push((cumulative_priority, cloned));
    }
    
    out
}

#[derive(Debug)]
struct ReplaceColumnCtx {
    missing: String,
    new_col: CString,
    table_filter: Option<String>, // 특정 테이블의 컬럼만 교체
    replaced_any: bool,
}

unsafe fn replace_column_ref_inplace(
    raw: *mut pg_sys::RawStmt,
    missing: &str,
    new_col: &str,
    table_filter: Option<&str>,
) {
    let mut ctx = ReplaceColumnCtx {
        missing: missing.to_ascii_lowercase(),
        new_col: CString::new(new_col).unwrap(),
        table_filter: table_filter.map(|s| s.to_string()),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut ReplaceColumnCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(raw_replace_column_walker), ctx_ptr);
    }
    
    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL refined ColumnRef: {} -> {}", missing, new_col);
    // }
}

/// ColumnRef 교체 walker
unsafe extern "C" fn raw_replace_column_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut ReplaceColumnCtx);

        if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
            let col_ref = node as *mut pg_sys::ColumnRef;

            if !(*col_ref).fields.is_null() {
                let mut should_stop = false;
                
                memcx::current_context(|mcx| {
                    if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                        let field_count = fields.len();
                        if field_count > 0 {
                            // 마지막 field = 컬럼명
                            let last_idx = field_count - 1;
                            let mut table_name: Option<String> = None;

                            // table_filter가 있으면 첫 번째 field가 테이블명인지 확인
                            if ctx.table_filter.is_some() && field_count > 1 {
                                if let Some(first_field_ptr) = fields.get(0) {
                                    let first_field = *first_field_ptr as *mut pg_sys::Node;
                                    if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                        let str_node = first_field as *mut pg_sys::String;
                                        table_name = Some(
                                            CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned()
                                        );
                                    }
                                }
                            }

                            if let Some(last_field_ptr) = fields.get(last_idx) {
                                let last_field = *last_field_ptr as *mut pg_sys::Node;

                                if (*last_field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = last_field as *mut pg_sys::String;
                                    let cur_col = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();

                                    // 조건: (컬럼명이 맞고) + (table_filter 없거나, 일치하는 경우만)
                                    let column_match = cur_col.to_ascii_lowercase() == ctx.missing;
                                    let table_match = match (&ctx.table_filter, &table_name) {
                                        (Some(filter), Some(tbl)) => tbl.to_ascii_lowercase() == filter.to_ascii_lowercase(),
                                        (Some(_), None) => false, // 필터가 있는데 테이블명이 없는 경우 -> qualified reference가 아님
                                        (None, _) => true,        // 필터 없으면 무조건 허용 (unqualified reference)
                                    };

                                    if column_match && table_match {
                                        // 메모리 안전성을 위해 더 신중하게 처리
                                        if !(*str_node).sval.is_null() {
                                            let new_sval = pg_sys::pstrdup(ctx.new_col.as_ptr());
                                            if !new_sval.is_null() {
                                                (*str_node).sval = new_sval;
                                                ctx.replaced_any = true;
                                                should_stop = true;
                                                
                                                // pgrx::notice!(
                                                //     "Replaced column {} with {}{}",
                                                //     ctx.missing,
                                                //     ctx.new_col.to_string_lossy(),
                                                //     match table_name {
                                                //         Some(ref t) => format!(" (table={})", t),
                                                //         None => "".to_string(),
                                                //     }
                                                // );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
                
                if should_stop {
                    return false;
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(raw_replace_column_walker), ctx);
    }
}

/* ------------------------------------------------
CASE 3) FROM Rel -> FROM Rel' - 칼럼이 참조하는 테이블 refinement 수행
------------------------------------------------ */
/// Column 에러에서 테이블명을 바꿀 때 사용: FROM절과 모든 qualified column reference를 동시에 변경
pub fn generate_table_refinements_for_column_error_raw(
    orig: *mut pg_sys::RawStmt,
    old_table_name: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    // 현재 FROM절의 모든 테이블명 추출 (중복 방지용)
    let existing_tables = extract_all_tables_from_raw(orig);
    let existing_table_names: std::collections::HashSet<String> = existing_tables.iter()
        .map(|t| t.table_name.to_ascii_lowercase())
        .collect();

    // 먼저 old_table_name이 alias인지 실제 테이블명인지 확인
    let actual_table_name = unsafe { find_actual_table_name(orig, old_table_name) };
    
    // 실제 테이블명 기준으로 similarity 계산
    let target_table = actual_table_name.as_deref().unwrap_or(old_table_name);
    let candidates = list_tables_by_similarity(target_table);
    let mut out = Vec::with_capacity(candidates.len());

    for (fq, distance) in candidates {
        // 후보 테이블명 추출
        let candidate_table = if let Some((_, rel)) = split_schema_rel(&fq) {
            rel
        } else {
            fq.clone()
        };
        
        // 현재 FROM절에 이미 있는 테이블이면 스킵 (단, 교체 대상인 테이블은 제외)
        if existing_table_names.contains(&candidate_table.to_ascii_lowercase()) && 
           candidate_table.to_ascii_lowercase() != target_table.to_ascii_lowercase() {
            continue;
        }

        // GUC 가중치를 적용하여 priority 계산
        let weight = TABLE_FOR_COLUMN_WEIGHT.get() as f32;
        let additional_priority = ((distance * 100.0) * weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        let new_table_name = if let Some((_, rel)) = split_schema_rel(&fq) {
            rel
        } else {
            fq.clone()
        };
        
        let cloned = unsafe { copy_node(orig) };
        unsafe { 
            replace_table_intelligently(cloned, target_table, &new_table_name);
        }
        out.push((cumulative_priority, cloned));
    }
    
    out
}

/// old_table_name이 실제로 어떤 테이블을 가리키는지 찾기
unsafe fn find_actual_table_name(raw: *mut pg_sys::RawStmt, reference_name: &str) -> Option<String> {
    let mut result = None;
    let reference_lower = reference_name.to_ascii_lowercase();
    
    unsafe {
        safe_raw_expression_tree_walker(
            (*raw).stmt, 
            Some(find_table_walker), 
            &mut (reference_lower, &mut result) as *mut _ as *mut c_void
        );
    }
    
    result
}

unsafe extern "C" fn find_table_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let (reference_name, result) = &mut *(ctx as *mut (String, &mut Option<String>));

        if (*node).type_ == pg_sys::NodeTag::T_RangeVar {
            let rv = node as *mut pg_sys::RangeVar;
            
            let table_name = if !(*rv).relname.is_null() {
                CStr::from_ptr((*rv).relname).to_string_lossy().into_owned()
            } else { 
                return false;
            };

            // 1) 실제 테이블명과 매치되는 경우
            if table_name.to_ascii_lowercase() == *reference_name {
                **result = Some(table_name);
                return false; // 찾았으므로 중단
            }

            // 2) alias와 매치되는 경우
            if !(*rv).alias.is_null() {
                let alias = (*rv).alias as *mut pg_sys::Alias;
                if !(*alias).aliasname.is_null() {
                    let alias_name = CStr::from_ptr((*alias).aliasname).to_string_lossy().into_owned();
                    if alias_name.to_ascii_lowercase() == *reference_name {
                        **result = Some(table_name); // 실제 테이블명 반환
                        return false; // 찾았으므로 중단
                    }
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(find_table_walker), ctx);
    }
}

/// 테이블과 해당 alias를 표현하는 구조체
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_name: String,
    pub alias: Option<String>,
}

impl TableInfo {
    pub fn new(table_name: String, alias: Option<String>) -> Self {
        Self { table_name, alias }
    }
    
    /// 참조할 때 사용할 이름 반환 (alias가 있으면 alias, 없으면 table_name)
    pub fn get_reference_name(&self) -> &str {
        self.alias.as_ref().unwrap_or(&self.table_name)
    }
}

/// FROM절에서 모든 테이블명과 alias 추출
pub fn extract_all_tables_from_raw(raw: *mut pg_sys::RawStmt) -> Vec<TableInfo> {
    let mut tables = Vec::new();
    let tables_ptr = &mut tables as *mut Vec<TableInfo> as *mut c_void;
    
    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(extract_tables_with_alias_walker), tables_ptr);
    }
    
    tables
}

/// 테이블과 alias를 함께 추출하는 walker 함수
unsafe extern "C" fn extract_tables_with_alias_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    let tables = context as *mut Vec<TableInfo>;

    unsafe {
        match (*node).type_ {
            pg_sys::NodeTag::T_RangeVar => {
                let range_var = node as *mut pg_sys::RangeVar;
                let table_name = CStr::from_ptr((*range_var).relname)
                    .to_string_lossy()
                    .to_string();
                
                let alias = if (*range_var).alias.is_null() {
                    None
                } else {
                    let alias_name = CStr::from_ptr((*(*range_var).alias).aliasname)
                        .to_string_lossy()
                        .to_string();
                    Some(alias_name)
                };
                
                (*tables).push(TableInfo::new(table_name, alias));
            }
            _ => {}
        }
    
        safe_raw_expression_tree_walker(node, Some(extract_tables_with_alias_walker), context)
    }
}

/// 테이블명이 특정되지 않은 경우 FROM절의 모든 테이블에 대해 refinement 생성
pub fn generate_table_refinements_for_all_tables_raw(
    orig: *mut pg_sys::RawStmt,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    // FROM절의 모든 테이블 추출
    let from_tables = extract_all_tables_from_raw(orig);
    let existing_table_names: std::collections::HashSet<String> = from_tables.iter()
        .map(|t| t.table_name.to_ascii_lowercase())
        .collect();
    
    let mut out = Vec::new();
    let weight = TABLE_FOR_COLUMN_WEIGHT.get() as f32; // 가중치 미리 가져오기
    
    // 각 테이블에 대해 refinement 생성
    for table_info in from_tables {
        let candidates = list_tables_by_similarity(&table_info.table_name);
        
        for (fq, distance) in candidates {
            // 후보 테이블명 추출
            let candidate_table = if let Some((_, rel)) = split_schema_rel(&fq) {
                rel
            } else {
                fq.clone()
            };
            
            // 현재 FROM절에 이미 있는 테이블이면 스킵 (단, 교체 대상인 테이블은 제외)
            if existing_table_names.contains(&candidate_table.to_ascii_lowercase()) && 
               candidate_table.to_ascii_lowercase() != table_info.table_name.to_ascii_lowercase() {
                continue;
            }

            // GUC 가중치를 적용하여 priority 계산
            let additional_priority = ((distance * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let new_table_name = if let Some((_, rel)) = split_schema_rel(&fq) {
                rel
            } else {
                fq.clone()
            };
                        
            let cloned = unsafe { copy_node(orig) };
            unsafe { 
                replace_table_intelligently(cloned, &table_info.table_name, &new_table_name);
            }
            out.push((cumulative_priority, cloned));
        }
    }
    
    out
}

#[derive(Debug)]
struct IntelligentTableReplaceCtx {
    old_reference: String,    // 에러 메시지에서 온 reference (alias일 수도 있고 테이블명일 수도 있음)
    new_table: CString,       // 새로운 실제 테이블명
    actual_old_table: String, // 실제 기존 테이블명
    replaced_any: bool,
}

/// 지능적 테이블 교체: alias와 실제 테이블명을 구분하여 처리
unsafe fn replace_table_intelligently(
    raw: *mut pg_sys::RawStmt,
    old_reference: &str,  // 에러에서 온 reference (w 또는 world)
    new_table: &str,      // 새로운 테이블명 (world_info)
) {
    // 1단계: old_reference가 실제로 어떤 테이블을 가리키는지 확인
    let actual_old_table = unsafe {
        find_actual_table_name(raw, old_reference)
            .unwrap_or_else(|| old_reference.to_string())
    };

    let mut ctx = IntelligentTableReplaceCtx {
        old_reference: old_reference.to_ascii_lowercase(),
        new_table: CString::new(new_table).unwrap(),
        actual_old_table: actual_old_table.to_ascii_lowercase(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut IntelligentTableReplaceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(intelligent_table_replace_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL intelligently refined table: {} -> {}", 
    //         &ctx.actual_old_table, new_table);
    // }
}

unsafe extern "C" fn intelligent_table_replace_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut IntelligentTableReplaceCtx);

        match (*node).type_ {
            // FROM절의 테이블명 교체
            pg_sys::NodeTag::T_RangeVar => {
                let rv = node as *mut pg_sys::RangeVar;
                
                let cur_rel = if !(*rv).relname.is_null() {
                    CStr::from_ptr((*rv).relname).to_string_lossy().into_owned()
                } else { 
                    String::new() 
                };
                
                // 실제 테이블명이 일치하면 교체
                if cur_rel.to_ascii_lowercase() == ctx.actual_old_table {
                    (*rv).relname = pg_sys::pstrdup(ctx.new_table.as_ptr());
                    ctx.replaced_any = true;
                    
                    // alias는 그대로 유지 (건드리지 않음)
                }
            },

            // SELECT절, WHERE절 등의 qualified column reference 교체
            pg_sys::NodeTag::T_ColumnRef => {
                let col_ref = node as *mut pg_sys::ColumnRef;
                
                if !(*col_ref).fields.is_null() {
                    memcx::current_context(|mcx| {
                        if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                            let field_count = fields.len();
                            
                            // qualified reference (table.column 형태)인 경우만 처리
                            if field_count >= 2 {
                                if let Some(first_field_ptr) = fields.get(0) {
                                    let first_field = *first_field_ptr as *mut pg_sys::Node;
                                    
                                    if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                        let str_node = first_field as *mut pg_sys::String;
                                        let qualifier = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                        
                                        // qualifier가 old_reference와 일치하면 교체
                                        if qualifier.to_ascii_lowercase() == ctx.old_reference {
                                            // old_reference가 실제 테이블명이었다면 새 테이블명으로 교체
                                            // old_reference가 alias였다면 alias는 그대로 유지
                                            let new_qualifier = if ctx.old_reference == ctx.actual_old_table {
                                                // 실제 테이블명으로 참조된 경우 -> 새 테이블명으로 교체
                                                ctx.new_table.to_string_lossy().into_owned()
                                            } else {
                                                // alias로 참조된 경우 -> alias 그대로 유지
                                                qualifier.clone() // 변경하지 않음
                                            };
                                            
                                            if new_qualifier != qualifier {
                                                let new_qualifier_cstr = CString::new(new_qualifier).unwrap();
                                                (*str_node).sval = pg_sys::pstrdup(new_qualifier_cstr.as_ptr());
                                                ctx.replaced_any = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            },

            _ => {}
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(intelligent_table_replace_walker), ctx);
    }
}

/* ------------------------------------------------
CASE 4) SELECT R.Att -> SELECT S.Att - 칼럼의 참조를 변경 시도
------------------------------------------------ */
/// qualified column reference의 테이블 참조 변경 refinement 생성 (R.a -> S.a, alias 고려)
pub fn generate_column_table_reference_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    current_table: Option<&str>,
    missing_col: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    // 현재 테이블이 특정되지 않은 경우 아무것도 하지 않음
    let current_table = match current_table {
        Some(table) => table,
        None => return out,
    };
    
    // FROM절의 모든 테이블과 alias 추출
    let from_tables = extract_all_tables_from_raw(orig);
    let weight = COLUMN_TABLE_REFERENCE_WEIGHT.get() as f32; // 가중치 미리 가져오기
    
    // 현재 테이블과 다른 테이블들에 대해 참조 변경 시도
    for table_info in from_tables {
        let reference_name = table_info.get_reference_name();
        
        // 현재 테이블과 동일한 경우 스킵
        if reference_name.to_ascii_lowercase() == current_table.to_ascii_lowercase() ||
           table_info.table_name.to_ascii_lowercase() == current_table.to_ascii_lowercase() {
            continue;
        }
        
        // 실제 테이블명으로 컬럼 검색
        let column_candidates = list_columns_by_similarity(Some(&table_info.table_name), missing_col, false);
        
        // 해당 테이블에 유사한 컬럼이 있으면 테이블 참조 변경
        if !column_candidates.is_empty() {
            if let Some((best_col, _table_name, distance)) = column_candidates.first() {
                // GUC 가중치를 적용하여 priority 계산
                let additional_priority = ((distance * 100.0) * weight) as i32;
                let cumulative_priority = base_priority + additional_priority;
                let cloned = unsafe { copy_node(orig) };
                
                unsafe { 
                    replace_column_table_reference(
                        cloned, 
                        current_table, 
                        reference_name,
                        missing_col, 
                        best_col
                    );
                }
                out.push((cumulative_priority, cloned));
            }
        }
    }
    
    out
}

#[derive(Debug)]
struct ColumnTableReferenceCtx {
    old_table: String,
    new_table: String,
    old_column: String,
    new_column: String,
    replaced_any: bool,
}

/// qualified column reference에서 테이블 참조 변경 (R.a -> S.b)
unsafe fn replace_column_table_reference(
    raw: *mut pg_sys::RawStmt,
    old_table: &str,
    new_table: &str,
    old_column: &str,
    new_column: &str,
) {
    let mut ctx = ColumnTableReferenceCtx {
        old_table: old_table.to_ascii_lowercase(),
        new_table: new_table.to_string(),
        old_column: old_column.to_ascii_lowercase(),
        new_column: new_column.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut ColumnTableReferenceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(column_table_reference_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL refined column table reference: {}.{} -> {}.{}", 
    //         old_table, old_column, new_table, new_column);
    // }
}

unsafe extern "C" fn column_table_reference_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut ColumnTableReferenceCtx);

        if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
            let col_ref = node as *mut pg_sys::ColumnRef;

            if !(*col_ref).fields.is_null() {
                let mut should_stop = false;
                
                memcx::current_context(|mcx| {
                    if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                        let field_count = fields.len();
                        
                        // qualified reference (table.column 형태)인 경우만 처리
                        if field_count >= 2 {
                            let mut table_match = false;
                            let mut column_match = false;
                            
                            // 첫 번째 field (테이블명) 확인
                            if let Some(first_field_ptr) = fields.get(0) {
                                let first_field = *first_field_ptr as *mut pg_sys::Node;
                                if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = first_field as *mut pg_sys::String;
                                    let table_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                    
                                    if table_name.to_ascii_lowercase() == ctx.old_table {
                                        table_match = true;
                                    }
                                }
                            }
                            
                            // 마지막 field (컬럼명) 확인
                            let last_idx = field_count - 1;
                            if let Some(last_field_ptr) = fields.get(last_idx) {
                                let last_field = *last_field_ptr as *mut pg_sys::Node;
                                if (*last_field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = last_field as *mut pg_sys::String;
                                    let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                    
                                    if column_name.to_ascii_lowercase() == ctx.old_column {
                                        column_match = true;
                                    }
                                }
                            }
                            
                            // 테이블과 컬럼이 모두 일치하면 교체
                            if table_match && column_match {
                                // 테이블명 교체
                                if let Some(first_field_ptr) = fields.get(0) {
                                    let first_field = *first_field_ptr as *mut pg_sys::Node;
                                    if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                        let str_node = first_field as *mut pg_sys::String;
                                        let new_table_cstr = CString::new(ctx.new_table.as_str()).unwrap();
                                        (*str_node).sval = pg_sys::pstrdup(new_table_cstr.as_ptr());
                                    }
                                }
                                
                                // 컬럼명 교체
                                if let Some(last_field_ptr) = fields.get(last_idx) {
                                    let last_field = *last_field_ptr as *mut pg_sys::Node;
                                    if (*last_field).type_ == pg_sys::NodeTag::T_String {
                                        let str_node = last_field as *mut pg_sys::String;
                                        let new_column_cstr = CString::new(ctx.new_column.as_str()).unwrap();
                                        (*str_node).sval = pg_sys::pstrdup(new_column_cstr.as_ptr());
                                    }
                                }
                                
                                ctx.replaced_any = true;
                                should_stop = true;
                            }
                        }
                    }
                });
                
                if should_stop {
                    return false; // 즉시 중단
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(column_table_reference_walker), ctx);
    }
}

/* ------------------------------------------------
CASE 5) FROM R -> FROM R JOIN S - 새로운 테이블 조인 추가
------------------------------------------------ */
/* ------------------------------------------------
FROM절의 모든 테이블에 대해 1-hop JOIN refinement 수행
------------------------------------------------ */
pub fn generate_one_hop_join_refinements_for_all_tables_raw(
    orig: *mut pg_sys::RawStmt,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    // FROM절의 모든 테이블 추출
    let from_tables = extract_all_tables_from_raw(orig);
    let existing_table_names: Vec<String> = from_tables.iter()
        .map(|t| t.table_name.clone())
        .collect();
    
    if existing_table_names.is_empty() {
        return out;
    }
    
    let existing_table_set: std::collections::HashSet<String> = existing_table_names.iter()
        .map(|t| t.to_ascii_lowercase())
        .collect();
    
    let weight = JOIN_REFINEMENT_WEIGHT.get() as f32;
    
    // 컬럼 검색 없이 모든 PK-FK 관계로 연결 가능한 테이블들 찾기
    let joinable_tables = find_all_joinable_tables(&existing_table_names);
    
    for (join_table, _distance, join_conditions) in joinable_tables {
        // 이미 FROM절에 있는 테이블이면 스킵
        if existing_table_set.contains(&join_table.to_ascii_lowercase()) {
            continue;
        }
        
        // 각 JOIN 조건에 대해 refinement 생성
        for join_condition in join_conditions {
            let additional_priority = ((1.0 * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            unsafe {
                // FROM절의 테이블 정보를 고려하여 JOIN 조건 조정
                let existing_tables = extract_all_tables_from_raw(cloned);
                let adjusted_condition = adjust_join_condition_for_existing_alias(&join_condition, &existing_tables);
                
                // 테이블 추가 및 WHERE 조건 추가
                add_table_and_where_condition(cloned, &join_table, &adjusted_condition);
            }
            out.push((cumulative_priority, cloned));
            
            // pgrx::notice!("SafeQL: Generated 1-hop JOIN refinement with table {} on condition {:?} (weight: {})", 
            //     join_table, join_condition, weight); 
        }
    }
    
    // pgrx::notice!("SafeQL: Generated {} 1-hop JOIN refinements for FROM clause tables", out.len());
    
    out
}

pub fn generate_join_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    missing_col: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    // 현재 FROM절의 모든 테이블 추출
    let from_tables = extract_all_tables_from_raw(orig);
    let existing_table_names: Vec<String> = from_tables.iter()
        .map(|t| t.table_name.clone())
        .collect();
    
    let weight = JOIN_REFINEMENT_WEIGHT.get() as f32; // 가중치 미리 가져오기
    
    if existing_table_names.is_empty() {
        // FROM절이 비어있는 경우: 테이블을 직접 추가
        let column_candidates = list_columns_by_similarity(None, missing_col, false);
        
        for (_col_name, table_name, distance) in column_candidates {
            // GUC 가중치를 적용하여 priority 계산
            let additional_priority = (((distance + 1.0) * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            unsafe {
                add_table_to_empty_from_clause(cloned, &table_name);
            }
            out.push((cumulative_priority, cloned));
        }
        
        return out;
    }
    
    // PK-FK 관계로 JOIN 가능한 테이블들 찾기 (기존 테이블들과 중복되지 않는 것만)
    let joinable_tables = find_joinable_tables_for_column(&existing_table_names, missing_col);
    let existing_table_set: std::collections::HashSet<String> = existing_table_names.iter()
        .map(|t| t.to_ascii_lowercase())
        .collect();
    
    for (join_table, column_distance, join_conditions) in joinable_tables {
        // 이미 FROM절에 있는 테이블이면 스킵
        if existing_table_set.contains(&join_table.to_ascii_lowercase()) {
            // pgrx::notice!("SafeQL: Skipping JOIN with table '{}' - already exists in FROM clause", join_table);
            continue;
        }
        
        // 각 JOIN 조건에 대해 별도의 refinement 생성
        for join_condition in join_conditions {
            // GUC 가중치를 적용하여 priority 계산
            let additional_priority = (((column_distance + 1.0) * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            unsafe {
                add_table_and_where_condition(cloned, &join_table, &join_condition);
            }            
            out.push((cumulative_priority, cloned));
            // pgrx::notice!("SafeQL added JOIN with table {} on condition {:?} (weight: {})", 
            //     join_table, join_condition, weight);
        }
    }
    
    out
}

/// FROM절에 테이블 추가 + WHERE절에 JOIN 조건 추가
unsafe fn add_table_and_where_condition(
    raw: *mut pg_sys::RawStmt,
    table_name: &str,
    join_condition: &JoinCondition,
) {
    unsafe {
        if let Some(select_stmt) = find_select_stmt(raw) {
            // 1. 현재 FROM절의 테이블과 alias 정보 추출
            let existing_tables = extract_all_tables_from_raw(raw);
            
            // 2. JoinCondition의 left_table을 alias를 고려해서 수정
            let adjusted_condition = adjust_join_condition_for_existing_alias(join_condition, &existing_tables);
            
            // 3. FROM절에 새 테이블 추가 (alias 없이)
            let table_cstr = CString::new(table_name).unwrap();
            let range_var = create_range_var(&table_cstr);
            add_table_to_from_list((*select_stmt).fromClause, range_var as *mut pg_sys::Node);
            
            // 4. WHERE절에 조정된 JOIN 조건 추가
            let join_cond_expr = create_join_condition_expr(&adjusted_condition);
            add_condition_to_where_clause(select_stmt, join_cond_expr);
        }
    }
}

/// JoinCondition의 left_table을 기존 테이블의 alias에 맞게 조정
fn adjust_join_condition_for_existing_alias(
    condition: &JoinCondition, 
    existing_tables: &[TableInfo],
) -> JoinCondition {
    let mut adjusted = condition.clone();
    
    // left_table이 기존 테이블 중 하나이고 alias가 있으면 alias로 변경
    for table_info in existing_tables {
        if table_info.table_name.to_ascii_lowercase() == condition.left_table.to_ascii_lowercase() {
            if let Some(ref alias) = table_info.alias {
                adjusted.left_table = alias.clone();
                // pgrx::notice!("SafeQL: Adjusted left table reference from {} to {} (using existing alias)", 
                //     condition.left_table, alias);
            }
            break;
        }
    }
    
    // right_table은 새로 추가되는 테이블이므로 그대로 유지
    
    adjusted
}

/// 빈 FROM절에 테이블만 추가 (WHERE 조건 없음)
unsafe fn add_table_to_empty_from_clause(
    raw: *mut pg_sys::RawStmt,
    table_name: &str,
) {
    unsafe {
        if let Some(select_stmt) = find_select_stmt(raw) {
            let table_cstr = CString::new(table_name).unwrap();
            let range_var = create_range_var(&table_cstr);
            let new_from_list = create_single_item_list(range_var as *mut pg_sys::Node);
            (*select_stmt).fromClause = new_from_list;
        }
    }
}

/* ------------------------------------------------
CASE) Operand column refinement - 오퍼레이터 오류 refinement 수행
------------------------------------------------ */
pub fn generate_operand_column_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    left_operand: &Option<ColumnOperand>,
    right_operand: &Option<ColumnOperand>,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let operand_weight = OPERAND_REFINEMENT_WEIGHT.get() as f32;

    // FROM절의 테이블 정보 미리 추출
    let from_tables = extract_all_tables_from_raw(orig);

    // Left operand 교체
    if let Some(left_op) = left_operand.as_ref() {
        let column_refinements = unsafe {
            let actual_table = if left_op.table_name.is_some() {
                find_actual_table_name(orig, left_op.table_name.as_ref().unwrap())
                    .unwrap_or_else(|| left_op.table_name.as_ref().unwrap().clone())
            } else {
                String::new()
            };
            
            find_compatible_columns_for_operator(
                if actual_table.is_empty() { None } else { Some(&actual_table) },
                &left_op.column_name,
                "",
                OperandPosition::Left
            )
        };
        
        for (col_name, table_name, distance) in column_refinements {
            let additional_priority = ((distance * 100.0) * operand_weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            
            // FROM절에서 해당 테이블의 참조명(alias 또는 테이블명) 찾기
            let reference_name = find_table_reference_name(&from_tables, &table_name);
            
            unsafe {
                replace_column_in_all_operands(cloned, left_op, &reference_name, &col_name);
            }

            // let sql = deparse_raw_stmt(cloned);
            // pgrx::notice!("SafeQL: Generated operand column refinement:\n{}", sql);

            // // print sql with priority
            // pgrx::notice!("SafeQL: Refinement priority: {}", cumulative_priority);
            out.push((cumulative_priority, cloned));
        }
    }
    
    // Right operand 교체
    if let Some(right_op) = right_operand.as_ref() {
        let column_refinements = unsafe {
            let actual_table = if right_op.table_name.is_some() {
                find_actual_table_name(orig, right_op.table_name.as_ref().unwrap())
                    .unwrap_or_else(|| right_op.table_name.as_ref().unwrap().clone())
            } else {
                String::new()
            };
            
            find_compatible_columns_for_operator(
                if actual_table.is_empty() { None } else { Some(&actual_table) },
                &right_op.column_name,
                "",
                OperandPosition::Right
            )
        };
        
        for (col_name, table_name, distance) in column_refinements {
            let additional_priority = ((distance * 100.0) * operand_weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            
            // FROM절에서 해당 테이블의 참조명(alias 또는 테이블명) 찾기
            let reference_name = find_table_reference_name(&from_tables, &table_name);
            
            unsafe {
                replace_column_in_all_operands(cloned, right_op, &reference_name, &col_name);
            }
            out.push((cumulative_priority, cloned));
        }
    }
    
    out
}

/// FROM절에서 테이블의 참조명(alias가 있으면 alias, 없으면 테이블명) 찾기
fn find_table_reference_name(from_tables: &[TableInfo], table_name: &str) -> String {
    for table_info in from_tables {
        if table_info.table_name.to_ascii_lowercase() == table_name.to_ascii_lowercase() {
            return table_info.get_reference_name().to_string();
        }
    }
    // 찾지 못한 경우 원래 테이블명 반환
    table_name.to_string()
}

unsafe fn replace_column_in_all_operands(
    raw: *mut pg_sys::RawStmt,
    old_operand: &ColumnOperand,
    new_table: &str,
    new_column: &str,
) {
    let mut ctx = AllOperandReplaceCtx {
        old_operand: old_operand.clone(),
        new_table: new_table.to_string(),
        new_column: new_column.to_string(),
        replaced_count: 0,
    };
    let ctx_ptr = &mut ctx as *mut AllOperandReplaceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(all_operand_replace_walker), ctx_ptr);
    }
    
    // if ctx.replaced_count > 0 {
    //     pgrx::notice!("SafeQL replaced {} operand(s): {}.{} -> {}.{}", 
    //         ctx.replaced_count,
    //         old_operand.table_name.as_deref().unwrap_or(""),
    //         old_operand.column_name,
    //         new_table,
    //         new_column
    //     );
    // }
}

#[derive(Debug)]
struct AllOperandReplaceCtx {
    old_operand: ColumnOperand,
    new_table: String,
    new_column: String,
    replaced_count: usize,
}

unsafe extern "C" fn all_operand_replace_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut AllOperandReplaceCtx);

        if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
            let col_ref = node as *mut pg_sys::ColumnRef;
            
            if !(*col_ref).fields.is_null() {
                let matched = memcx::current_context(|mcx| {
                    if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                        let field_count = fields.len();
                        
                        if field_count >= 2 {
                            if let (Some(first_ptr), Some(last_ptr)) = (fields.get(0), fields.get(field_count - 1)) {
                                let first_field = *first_ptr as *mut pg_sys::Node;
                                let last_field = *last_ptr as *mut pg_sys::Node;
                                
                                if (*first_field).type_ == pg_sys::NodeTag::T_String &&
                                   (*last_field).type_ == pg_sys::NodeTag::T_String {
                                    let first_str = first_field as *mut pg_sys::String;
                                    let last_str = last_field as *mut pg_sys::String;
                                    
                                    let table_name = CStr::from_ptr((*first_str).sval).to_string_lossy().into_owned();
                                    let column_name = CStr::from_ptr((*last_str).sval).to_string_lossy().into_owned();
                                    
                                    if let Some(ref old_table) = ctx.old_operand.table_name {
                                        if table_name.to_ascii_lowercase() == old_table.to_ascii_lowercase() &&
                                           column_name.to_ascii_lowercase() == ctx.old_operand.column_name.to_ascii_lowercase() {
                                            return true;
                                        }
                                    }
                                }
                            }
                        } 
                        else if field_count == 1 && ctx.old_operand.table_name.is_none() {
                            if let Some(field_ptr) = fields.get(0) {
                                let field = *field_ptr as *mut pg_sys::Node;
                                if (*field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = field as *mut pg_sys::String;
                                    let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                    
                                    if column_name.to_ascii_lowercase() == ctx.old_operand.column_name.to_ascii_lowercase() {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    false
                });
                
                if matched {
                    if ctx.old_operand.table_name.is_some() {
                        let table_str = create_string_node(&ctx.new_table);
                        let column_str = create_string_node(&ctx.new_column);
                        
                        let table_cell = pg_sys::ListCell { ptr_value: table_str as *mut c_void };
                        let column_cell = pg_sys::ListCell { ptr_value: column_str as *mut c_void };
                        
                        (*col_ref).fields = pg_sys::list_make2_impl(
                            pg_sys::NodeTag::T_List,
                            table_cell,
                            column_cell
                        );
                    } else {
                        let column_str = create_string_node(&ctx.new_column);
                        let column_cell = pg_sys::ListCell { ptr_value: column_str as *mut c_void };
                        (*col_ref).fields = pg_sys::list_make1_impl(pg_sys::NodeTag::T_List, column_cell);
                    }
                    
                    ctx.replaced_count += 1;
                }
            }
        }

        let ctx_ptr = ctx as *mut AllOperandReplaceCtx as *mut c_void;
        
        safe_raw_expression_tree_walker(node, Some(all_operand_replace_walker), ctx_ptr)
    }
}

/* ------------------------------------------------
Operand typecast refinement (수정됨)
------------------------------------------------ */
pub fn generate_operand_typecast_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    left_operand: &Option<ColumnOperand>,
    right_operand: &Option<ColumnOperand>,
    operator_info: &OperatorInfo,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let typecast_weight = TYPECAST_REFINEMENT_WEIGHT.get() as f32;

    let current_left_type = operator_info.left_type.as_deref();
    let current_right_type = operator_info.right_type.as_deref();
    
    let typecast_refinements = get_typecast_refinements_for_operator(
        &operator_info,
        current_left_type,
        current_right_type
    );
    
    for typecast_refinement in typecast_refinements {
        let additional_priority = ((1.0 * 100.0) * typecast_weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        let cloned = unsafe { copy_node(orig) };
        
        match typecast_refinement.cast_position {
            OperandPosition::Left => {
                unsafe {
                    apply_typecast_to_all_operands(
                        cloned,
                        left_operand,
                        &typecast_refinement.target_type,
                        OperandPosition::Left
                    );
                }
                out.push((cumulative_priority, cloned));
            },
            OperandPosition::Right => {
                unsafe {
                    apply_typecast_to_all_operands(
                        cloned,
                        right_operand,
                        &typecast_refinement.target_type,
                        OperandPosition::Right
                    );
                }
                out.push((cumulative_priority, cloned));
            }
        }
    }
    
    out
}

unsafe fn apply_typecast_to_all_operands(
    raw: *mut pg_sys::RawStmt,
    operand: &Option<ColumnOperand>,
    target_type: &str,
    cast_position: OperandPosition,
) {
    let mut ctx = AllOperandTypecastCtx {
        operand: operand.clone(),
        target_type: target_type.to_string(),
        cast_position,
        replaced_count: 0,
    };
    let ctx_ptr = &mut ctx as *mut AllOperandTypecastCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(all_operand_typecast_walker), ctx_ptr);
    }
}

#[derive(Debug)]
struct AllOperandTypecastCtx {
    operand: Option<ColumnOperand>,
    target_type: String,
    cast_position: OperandPosition,
    replaced_count: usize,
}

unsafe extern "C" fn all_operand_typecast_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut AllOperandTypecastCtx);

        if (*node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = node as *mut pg_sys::A_Expr;
            
            let target_operand_node = match ctx.cast_position {
                OperandPosition::Left => (*a_expr).lexpr,
                OperandPosition::Right => (*a_expr).rexpr,
            };
            
            if !target_operand_node.is_null() && 
               (*target_operand_node).type_ != pg_sys::NodeTag::T_TypeCast {
                
                if let Some(ref operand) = ctx.operand {
                    if (*target_operand_node).type_ == pg_sys::NodeTag::T_ColumnRef {
                        let col_ref = target_operand_node as *mut pg_sys::ColumnRef;
                        
                        if !(*col_ref).fields.is_null() {
                            let matches = memcx::current_context(|mcx| {
                                if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                                    let field_count = fields.len();
                                    
                                    if field_count >= 2 && operand.table_name.is_some() {
                                        if let (Some(first_ptr), Some(last_ptr)) = (fields.get(0), fields.get(field_count - 1)) {
                                            let first_field = *first_ptr as *mut pg_sys::Node;
                                            let last_field = *last_ptr as *mut pg_sys::Node;
                                            
                                            if (*first_field).type_ == pg_sys::NodeTag::T_String &&
                                               (*last_field).type_ == pg_sys::NodeTag::T_String {
                                                let first_str = first_field as *mut pg_sys::String;
                                                let last_str = last_field as *mut pg_sys::String;
                                                
                                                let table_name = CStr::from_ptr((*first_str).sval).to_string_lossy();
                                                let column_name = CStr::from_ptr((*last_str).sval).to_string_lossy();
                                                
                                                if let Some(ref op_table) = operand.table_name {
                                                    return table_name.to_ascii_lowercase() == op_table.to_ascii_lowercase() &&
                                                           column_name.to_ascii_lowercase() == operand.column_name.to_ascii_lowercase();
                                                }
                                            }
                                        }
                                    } else if field_count == 1 && operand.table_name.is_none() {
                                        if let Some(field_ptr) = fields.get(0) {
                                            let field = *field_ptr as *mut pg_sys::Node;
                                            if (*field).type_ == pg_sys::NodeTag::T_String {
                                                let str_node = field as *mut pg_sys::String;
                                                let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy();
                                                return column_name.to_ascii_lowercase() == operand.column_name.to_ascii_lowercase();
                                            }
                                        }
                                    }
                                }
                                false
                            });
                            
                            if matches {
                                let typecast_node = create_typecast_node(target_operand_node, &ctx.target_type);
                                
                                match ctx.cast_position {
                                    OperandPosition::Left => (*a_expr).lexpr = typecast_node,
                                    OperandPosition::Right => (*a_expr).rexpr = typecast_node,
                                }
                                
                                ctx.replaced_count += 1;
                            }
                        }
                    }
                }
            }
        }

        let ctx_ptr = ctx as *mut AllOperandTypecastCtx as *mut c_void;
        
        safe_raw_expression_tree_walker(node, Some(all_operand_typecast_walker), ctx_ptr)
    }
}

unsafe fn create_typecast_node(operand_node: *mut pg_sys::Node, target_type: &str) -> *mut pg_sys::Node {
    unsafe {
        let typecast = pg_sys::palloc0(size_of::<pg_sys::TypeCast>()) as *mut pg_sys::TypeCast;
        (*typecast).type_ = pg_sys::NodeTag::T_TypeCast;
        (*typecast).arg = operand_node;
        
        let type_name = pg_sys::palloc0(size_of::<pg_sys::TypeName>()) as *mut pg_sys::TypeName;
        (*type_name).type_ = pg_sys::NodeTag::T_TypeName;
        
        let type_str_node = create_string_node(target_type);
        let type_cell = pg_sys::ListCell { ptr_value: type_str_node as *mut c_void };
        (*type_name).names = pg_sys::list_make1_impl(pg_sys::NodeTag::T_List, type_cell);
        (*type_name).setof = false;
        (*type_name).pct_type = false;
        (*type_name).location = -1;
        
        (*typecast).typeName = type_name;
        (*typecast).location = -1;
        
        typecast as *mut pg_sys::Node
    }
}

/* ------------------------------------------------
WHERE절 헬퍼 함수들
------------------------------------------------ */
pub unsafe fn find_all_where_expressions(raw: *mut pg_sys::RawStmt) -> Vec<*mut pg_sys::Node> {
    let mut expressions = Vec::new();
    
    unsafe {
        if let Some(select_stmt) = find_select_stmt(raw) {
            if !(*select_stmt).whereClause.is_null() {
                collect_a_exprs((*select_stmt).whereClause, &mut expressions);
            }
        }
    }
    expressions
}

unsafe fn collect_a_exprs(node: *mut pg_sys::Node, expressions: &mut Vec<*mut pg_sys::Node>) {
    if node.is_null() {
        return;
    }
    
    unsafe {
        if (*node).type_ == pg_sys::NodeTag::T_A_Expr {
            expressions.push(node);
        }
        
        safe_raw_expression_tree_walker(node, Some(collect_a_exprs_walker), expressions as *mut _ as *mut c_void);
    }
}

unsafe extern "C" fn collect_a_exprs_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    unsafe { 
        collect_a_exprs(node, &mut *(ctx as *mut Vec<*mut pg_sys::Node>));
    }
    false
}

// pub unsafe fn extract_columns_from_where_clause(raw: *mut pg_sys::RawStmt) -> Vec<String> {
//     let mut columns = std::collections::HashSet::new();
    
//     unsafe {
//         if let Some(select_stmt) = find_select_stmt(raw) {
//             if !(*select_stmt).whereClause.is_null() {
//                 extract_columns_from_node((*select_stmt).whereClause, &mut columns);
//             }
//         }
//     }
    
//     columns.into_iter().collect()
// }

// unsafe fn extract_columns_from_node(node: *mut pg_sys::Node, columns: &mut std::collections::HashSet<String>) {
//     if node.is_null() {
//         return;
//     }
    
//     unsafe {
//         if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
//             let col_ref = node as *mut pg_sys::ColumnRef;
//             if !(*col_ref).fields.is_null() {
//                 memcx::current_context(|mcx| {
//                     if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
//                         if let Some(last_field_ptr) = fields.get(fields.len() - 1) {
//                             let last_field = *last_field_ptr as *mut pg_sys::Node;
//                             if (*last_field).type_ == pg_sys::NodeTag::T_String {
//                                 let str_node = last_field as *mut pg_sys::String;
//                                 let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
//                                 columns.insert(column_name);
//                             }
//                         }
//                     }
//                 });
//             }
//         }
        
//         safe_raw_expression_tree_walker(node, Some(extract_columns_walker), columns as *mut _ as *mut c_void);
//     }
// }

// unsafe extern "C" fn extract_columns_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
//     unsafe {
//         extract_columns_from_node(node, &mut *(ctx as *mut std::collections::HashSet<String>));
//     }
//     false
// }

pub unsafe fn extract_operator_info_from_expr(expr_node: *mut pg_sys::Node) -> OperatorInfo {
    unsafe {
        if expr_node.is_null() || (*expr_node).type_ != pg_sys::NodeTag::T_A_Expr {
            return OperatorInfo {
                operator_name: String::new(),
                left_type: None,
                right_type: None,
            };
        }
        
        let a_expr = expr_node as *mut pg_sys::A_Expr;
        let operator_name = if !(*a_expr).name.is_null() {
            memcx::current_context(|mcx| {
                if let Some(op_names) = List::<*mut c_void>::downcast_ptr_in_memcx((*a_expr).name, mcx) {
                    if let Some(op_name_ptr) = op_names.get(0) {
                        let op_name_node = *op_name_ptr as *mut pg_sys::Node;
                        if (*op_name_node).type_ == pg_sys::NodeTag::T_String {
                            let str_node = op_name_node as *mut pg_sys::String;
                            return CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                        }
                    }
                }
                String::new()
            })
        } else {
            String::new()
        };

        OperatorInfo {
            operator_name,
            left_type: None,
            right_type: None,
        }
    }
}


/* ------------------------------------------------
CASE 7) Function Argument Column Refinement - 함수 argument 교체
------------------------------------------------ */
pub fn generate_function_argument_column_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    argument_operands: &[Option<ColumnOperand>],
    error_pos: i32,
    function_name: &str,
    _arg_types: &[String],
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let argument_weight = ARGUMENT_REFINEMENT_WEIGHT.get() as f32;

    // 각 argument 위치에 대해 교체 시도
    unsafe {
        for (arg_index, argument_operand) in argument_operands.iter().enumerate() {
            if let Some(arg_op) = argument_operand {
                let argument_position = ArgumentPosition { index: arg_index };
                
                let column_refinements = if arg_op.table_name.is_some() {
                    let actual_table = find_actual_table_name(orig, arg_op.table_name.as_ref().unwrap())
                        .unwrap_or_else(|| arg_op.table_name.as_ref().unwrap().clone());
                    
                    find_compatible_arguments_for_function(
                        Some(&actual_table), 
                        &arg_op.column_name, 
                        function_name,
                        &argument_position
                    )
                } else {
                    find_compatible_arguments_for_function(
                        None, 
                        &arg_op.column_name, 
                        function_name,
                        &argument_position
                    )
                };
                
                for (col_name, table_name, distance) in column_refinements {
                    let additional_priority = ((distance * 100.0) * argument_weight) as i32;
                    let cumulative_priority = base_priority + additional_priority;
                    
                    let cloned = copy_node(orig);
                    replace_function_argument_at_position(
                        cloned, 
                        error_pos, 
                        arg_index,
                        arg_op.table_name.as_deref(),
                        &arg_op.column_name, 
                        &table_name, 
                        &col_name
                    );
                    out.push((cumulative_priority, cloned));
                    
                    // pgrx::notice!("SafeQL: Generated function argument refinement {} -> {} (table: {}, arg_index: {})", 
                    //     arg_op.column_name, col_name, table_name, arg_index);
                }
            }
        }
    }
    out
}


/// 특정 위치의 함수 argument 교체
unsafe fn replace_function_argument_at_position(
    raw: *mut pg_sys::RawStmt,
    error_pos: i32,
    arg_index: usize,
    old_table: Option<&str>,
    old_column: &str,
    new_table: &str,
    new_column: &str,
) {
    let mut ctx = FunctionArgumentReplaceCtx {
        error_pos,
        arg_index,
        old_table: old_table.map(|s| s.to_ascii_lowercase()),
        old_column: old_column.to_ascii_lowercase(),
        new_table: new_table.to_string(),
        new_column: new_column.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut FunctionArgumentReplaceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(function_argument_replace_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     let table_str = match old_table {
    //         Some(table) => format!("{}.{}", table, old_column),
    //         None => old_column.to_string(),
    //     };
    //     pgrx::notice!("SafeQL function argument refinement: {} -> {}.{} at position {} (arg_index: {})", 
    //         table_str, new_table, new_column, error_pos, arg_index);
    // }
}

#[derive(Debug)]
struct FunctionArgumentReplaceCtx {
    error_pos: i32,
    arg_index: usize,
    old_table: Option<String>,
    old_column: String,
    new_table: String,
    new_column: String,
    replaced_any: bool,
}

unsafe extern "C" fn function_argument_replace_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut FunctionArgumentReplaceCtx);

        if (*node).type_ == pg_sys::NodeTag::T_FuncCall {
            let func_call = node as *mut pg_sys::FuncCall;
            
            // location 확인 (에러 위치 근처에서만 교체)
            if (*func_call).location >= 0 && (*func_call).location + 1 == ctx.error_pos {
                if !(*func_call).args.is_null() {
                    let mut should_stop = false;
                    
                    memcx::current_context(|mcx| {
                        if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*func_call).args, mcx) {
                            if ctx.arg_index < args.len() {
                                if let Some(arg_ptr) = args.get(ctx.arg_index) {
                                    let arg_node = *arg_ptr as *mut pg_sys::Node;
                                    
                                    // ColumnRef인 경우에만 처리
                                    if (*arg_node).type_ == pg_sys::NodeTag::T_ColumnRef {
                                        let col_ref = arg_node as *mut pg_sys::ColumnRef;
                                        
                                        if !(*col_ref).fields.is_null() {
                                            if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                                                let field_count = fields.len();
                                                
                                                let (table_match, column_match) = if field_count >= 2 {
                                                    // qualified reference
                                                    let mut table_match = false;
                                                    let mut column_match = false;
                                                    
                                                    // 테이블명 확인
                                                    if let Some(first_field_ptr) = fields.get(0) {
                                                        let first_field = *first_field_ptr as *mut pg_sys::Node;
                                                        if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                                            let str_node = first_field as *mut pg_sys::String;
                                                            let table_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                                            
                                                            if let Some(ref old_table) = ctx.old_table {
                                                                if table_name.to_ascii_lowercase() == *old_table {
                                                                    table_match = true;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    
                                                    // 컬럼명 확인
                                                    let last_idx = field_count - 1;
                                                    if let Some(last_field_ptr) = fields.get(last_idx) {
                                                        let last_field = *last_field_ptr as *mut pg_sys::Node;
                                                        if (*last_field).type_ == pg_sys::NodeTag::T_String {
                                                            let str_node = last_field as *mut pg_sys::String;
                                                            let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                                            
                                                            if column_name.to_ascii_lowercase() == ctx.old_column {
                                                                column_match = true;
                                                            }
                                                        }
                                                    }
                                                    
                                                    (table_match, column_match)
                                                } else if field_count == 1 {
                                                    // unqualified reference
                                                    let mut column_match = false;
                                                    
                                                    if let Some(field_ptr) = fields.get(0) {
                                                        let field = *field_ptr as *mut pg_sys::Node;
                                                        if (*field).type_ == pg_sys::NodeTag::T_String {
                                                            let str_node = field as *mut pg_sys::String;
                                                            let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                                            
                                                            if column_name.to_ascii_lowercase() == ctx.old_column && ctx.old_table.is_none() {
                                                                column_match = true;
                                                            }
                                                        }
                                                    }
                                                    
                                                    (ctx.old_table.is_none(), column_match)
                                                } else {
                                                    (false, false)
                                                };
                                                
                                                // 테이블과 컬럼이 모두 일치하면 교체
                                                if table_match && column_match {
                                                    // 새로운 qualified column reference 생성 시 기존 table qualifier 사용
                                                    let table_qualifier = if let Some(first_field_ptr) = fields.get(0) {
                                                        let first_field = *first_field_ptr as *mut pg_sys::Node;
                                                        if (*first_field).type_ == pg_sys::NodeTag::T_String {
                                                            let str_node = first_field as *mut pg_sys::String;
                                                            CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned()
                                                        } else {
                                                            ctx.new_table.clone() // fallback
                                                        }
                                                    } else {
                                                        ctx.new_table.clone() // fallback
                                                    };
                                                    
                                                    let new_col_ref = create_qualified_column_ref(&table_qualifier, &ctx.new_column);
                                                    
                                                    // args 리스트에서 해당 인덱스의 argument 교체
                                                    let raw_list = (*func_call).args as *mut pg_sys::List;
                                                    let elements_ptr = (*raw_list).elements;
                                                    if !elements_ptr.is_null() && ctx.arg_index < (*raw_list).length as usize {
                                                        let cell_ptr = elements_ptr.add(ctx.arg_index);
                                                        (*cell_ptr).ptr_value = new_col_ref as *mut c_void;
                                                    }
                                                    
                                                    // 새 리스트 생성 및 교체 로직...
                                                    ctx.replaced_any = true;
                                                    should_stop = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                    
                    if should_stop {
                        return false;
                    }
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(function_argument_replace_walker), ctx);
    }
}

/// qualified column reference 생성 (table.column)
unsafe fn create_qualified_column_ref(table_name: &str, column_name: &str) -> *mut pg_sys::Node {
    unsafe {
        let col_ref = pg_sys::palloc0(size_of::<pg_sys::ColumnRef>()) as *mut pg_sys::ColumnRef;
        (*col_ref).type_ = pg_sys::NodeTag::T_ColumnRef;
        
        let table_str = create_string_node(table_name);
        let column_str = create_string_node(column_name);
        
        let table_cell = pg_sys::ListCell { ptr_value: table_str as *mut c_void };
        let column_cell = pg_sys::ListCell { ptr_value: column_str as *mut c_void };
        
        (*col_ref).fields = pg_sys::list_make2_impl(
            pg_sys::NodeTag::T_List,
            table_cell,
            column_cell
        );
        (*col_ref).location = -1;
        
        col_ref as *mut pg_sys::Node
    }
}




/* ------------------------------------------------
CASE 8) Function Typecast Refinement - 함수 argument 타입 캐스팅
------------------------------------------------ */
/// 함수 타입 캐스팅 에러에 대한 refinement 생성
pub fn generate_function_typecast_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    function_info: &FunctionInfo,
    error_pos: i32,
    _function_name: &str,
    arg_types: &[String],
    _error_message: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let typecast_weight = TYPECAST_REFINEMENT_WEIGHT.get() as f32;

    unsafe {
        // 타입 캐스팅 refinement 시도
        let typecast_refinements = get_typecast_refinements_for_function(
            &function_info,
            arg_types
        );
        
        for typecast_refinement in typecast_refinements {
            // 타입 캐스팅 가중치: 1.0 * 100 * TYPECAST_REFINEMENT_WEIGHT
            let additional_priority = ((1.0 * 100.0) * typecast_weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = copy_node(orig);
            
            // 해당 argument 위치에 타입 캐스팅 적용
            apply_typecast_to_function_argument_at_position(
                cloned,
                error_pos,
                &typecast_refinement.target_type,
                typecast_refinement.cast_position.index
            );
            out.push((cumulative_priority, cloned));
            
            // pgrx::notice!("SafeQL: Generated function typecast refinement at arg_index {} to type {}", 
            //     typecast_refinement.cast_position.index, typecast_refinement.target_type);
        }
    }
    out
}

/// 특정 위치의 함수 argument에 타입 캐스팅 적용
unsafe fn apply_typecast_to_function_argument_at_position(
    raw: *mut pg_sys::RawStmt,
    error_pos: i32,
    target_type: &str,
    arg_index: usize,
) {
    let mut ctx = FunctionTypecastCtx {
        error_pos,
        target_type: target_type.to_string(),
        arg_index,
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut FunctionTypecastCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(function_typecast_walker), ctx_ptr);
    }
}

#[derive(Debug)]
struct FunctionTypecastCtx {
    error_pos: i32,
    target_type: String,
    arg_index: usize,
    replaced_any: bool,
}

unsafe extern "C" fn function_typecast_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut FunctionTypecastCtx);

        if (*node).type_ == pg_sys::NodeTag::T_FuncCall {
            let func_call = node as *mut pg_sys::FuncCall;
            
            // location 확인 (에러 위치 근처에서만 변경)
            if (*func_call).location >= 0 && (*func_call).location + 1 == ctx.error_pos {
                if !(*func_call).args.is_null() {
                    let mut should_stop = false;
                    
                    memcx::current_context(|mcx| {
                        if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*func_call).args, mcx) {
                            if ctx.arg_index < args.len() {
                                if let Some(arg_ptr) = args.get(ctx.arg_index) {
                                    let arg_node = *arg_ptr as *mut pg_sys::Node;
                                    
                                    if (*arg_node).type_ != pg_sys::NodeTag::T_TypeCast && !arg_node.is_null() {
                                        // TypeCast 노드로 감싸기
                                        let typecast_node = create_typecast_node(arg_node, &ctx.target_type);
                                        
                                        // args 리스트에서 해당 인덱스의 argument를 TypeCast 노드로 교체
                                        let raw_list = (*func_call).args as *mut pg_sys::List;
                                        let elements_ptr = (*raw_list).elements;
                                        if !elements_ptr.is_null() && ctx.arg_index < (*raw_list).length as usize {
                                            let cell_ptr = elements_ptr.add(ctx.arg_index);
                                            (*cell_ptr).ptr_value = typecast_node as *mut c_void;
                                        }
                                        
                                        ctx.replaced_any = true;
                                        should_stop = true;
                                    }
                                }
                            }
                        }
                    });
                    
                    if should_stop {
                        return false; // 변경 완료, 더 이상 탐색 불필요
                    }
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(function_typecast_walker), ctx);
    }
}


/* ------------------------------------------------
CASE) Function does not exist - function argument/typecast/name refinement 수행
------------------------------------------------ */

// generate_function_argument_refinements_raw 함수 뒤에 추가
/// 함수명 에러에 대한 refinement 생성 (CASE 6-4)
pub fn generate_function_name_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    error_pos: i32,
    function_name: &str,
    arg_types: &[String],
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let function_name_weight = FUNCTION_NAME_REFINEMENT_WEIGHT.get() as f32;
    
    let similar_functions = find_similar_functions(function_name, arg_types.len());
    
    for (similar_func_name, _similar_arg_types, _return_type, distance) in &similar_functions {
        let additional_priority = ((distance * 100.0) * function_name_weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        let cloned = unsafe { copy_node(orig) };
        unsafe {
            replace_function_name_at_position(
                cloned,
                error_pos,
                function_name,
                similar_func_name
            );
        }
        out.push((cumulative_priority, cloned));
        
        // pgrx::notice!("SafeQL: Generated function name refinement {} -> {} (distance: {})", 
        //     function_name, similar_func_name, distance);
    }

    out
}

// function_typecast_walker 함수 뒤에 추가
/// 특정 위치의 함수명 교체
unsafe fn replace_function_name_at_position(
    raw: *mut pg_sys::RawStmt,
    error_pos: i32,
    old_function_name: &str,
    new_function_name: &str,
) {
    let mut ctx = FunctionNameReplaceCtx {
        error_pos,
        old_function_name: old_function_name.to_ascii_lowercase(),
        new_function_name: new_function_name.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut FunctionNameReplaceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(function_name_replace_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL function name refinement: {} -> {} at position {}", 
    //         old_function_name, new_function_name, error_pos);
    // }
}

#[derive(Debug)]
struct FunctionNameReplaceCtx {
    error_pos: i32,
    old_function_name: String,
    new_function_name: String,
    replaced_any: bool,
}

unsafe extern "C" fn function_name_replace_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut FunctionNameReplaceCtx);

        if (*node).type_ == pg_sys::NodeTag::T_FuncCall {
            let func_call = node as *mut pg_sys::FuncCall;
            
            if (*func_call).location >= 0 && (*func_call).location + 1 == ctx.error_pos {
                if !(*func_call).funcname.is_null() {
                    let mut should_stop = false;
                    
                    memcx::current_context(|mcx| {
                        if let Some(func_names) = List::<*mut c_void>::downcast_ptr_in_memcx((*func_call).funcname, mcx) {
                            if let Some(last_name_ptr) = func_names.get(func_names.len() - 1) {
                                let last_name_node = *last_name_ptr as *mut pg_sys::Node;
                                if (*last_name_node).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = last_name_node as *mut pg_sys::String;
                                    let current_name = CStr::from_ptr((*str_node).sval)
                                        .to_string_lossy()
                                        .into_owned();
                                    
                                    if current_name.to_ascii_lowercase() == ctx.old_function_name {
                                        let new_name_cstr = CString::new(ctx.new_function_name.as_str()).unwrap();
                                        (*str_node).sval = pg_sys::pstrdup(new_name_cstr.as_ptr());
                                        ctx.replaced_any = true;
                                        should_stop = true;
                                    }
                                }
                            }
                        }
                    });
                    
                    if should_stop {
                        return false;
                    }
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(function_name_replace_walker), ctx);
    }
}


/* ------------------------------------------------
CASE 11) Invalid Text Representation - argument format 변환
------------------------------------------------ */
pub fn generate_argument_format_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    error_message: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    unsafe {
        // 1. 에러 메시지에서 문제가 되는 format string 추출
        let problematic_format = extract_format_from_error(error_message);
        
        if let Some(bad_format) = problematic_format {
            // pgrx::notice!("SafeQL: Detected problematic format string: '{}'", bad_format);
            
            // 2. 모든 literal을 순회하면서 매칭되는 것 찾기
            let all_literals = find_all_string_literals(orig);
            
            for literal_node in all_literals {
                if let Some(literal_value) = extract_literal_string_value(literal_node) {
                    let trimmed = literal_value.trim().trim_matches(|c| c == '\'' || c == '"');
                    
                    // 에러 메시지의 format과 매칭되는 literal 찾음
                    if trimmed.to_lowercase().contains(&bad_format) || trimmed == bad_format {
                        // strftime format -> date_part field 변환 시도
                        if let Some(transformed) = strftime_to_datepart(trimmed) {
                            let additional_priority = 0;
                            let cumulative_priority = base_priority + additional_priority;
                            
                            let cloned = copy_node(orig);
                            replace_literal_by_value(cloned, trimmed, &transformed);
                            out.push((cumulative_priority, cloned));
                            
                            // pgrx::notice!("SafeQL: Generated format refinement '{}' -> '{}'", 
                            //     trimmed, transformed);
                        }
                    }
                }
            }
        } 
    }
    
    out
}

/// 에러 메시지에서 문제가 되는 format string 추출
/// 예: "unit \"%y\" not recognized for type timestamp" -> "%y"
fn extract_format_from_error(error_message: &str) -> Option<String> {
    // Pattern 1: unit "%Y" not recognized
    let re1 = Regex::new(r#"unit\s+"([^"]+)"\s+not\s+recognized"#).unwrap();
    if let Some(cap) = re1.captures(error_message) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    // Pattern 2: invalid format string "%Y"
    let re2 = Regex::new(r#"invalid\s+format\s+(?:string\s+)?"([^"]+)""#).unwrap();
    if let Some(cap) = re2.captures(error_message) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    None
}

/// AST에서 모든 string literal 찾기
unsafe fn find_all_string_literals(
    raw: *mut pg_sys::RawStmt,
) -> Vec<*mut pg_sys::Node> {
    let mut literals = Vec::new();
    let ctx_ptr = &mut literals as *mut Vec<*mut pg_sys::Node> as *mut c_void;
    
    unsafe {
        safe_raw_expression_tree_walker(
            (*raw).stmt,
            Some(find_all_literals_walker),
            ctx_ptr
        );
    }
    
    literals
}

unsafe extern "C" fn find_all_literals_walker(
    node: *mut pg_sys::Node,
    ctx: *mut c_void
) -> bool {
    if node.is_null() {
        return false;
    }
    
    unsafe {
        let literals = &mut *(ctx as *mut Vec<*mut pg_sys::Node>);
        
        if (*node).type_ == pg_sys::NodeTag::T_A_Const {
            let a_const = node as *mut pg_sys::A_Const;
            let value_node = &(*a_const).val.node as *const pg_sys::Node as *mut pg_sys::Node;
            
            // string literal만 수집
            if !value_node.is_null() && (*value_node).type_ == pg_sys::NodeTag::T_String {
                literals.push(node);
            }
        }
        
        safe_raw_expression_tree_walker(node, Some(find_all_literals_walker), ctx)
    }
}

/// 값으로 literal 찾아서 교체
unsafe fn replace_literal_by_value(
    raw: *mut pg_sys::RawStmt,
    old_value: &str,
    new_value: &str,
) {
    let mut ctx = LiteralReplaceByValueCtx {
        old_value: old_value.to_string(),
        new_value: new_value.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut LiteralReplaceByValueCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(literal_replace_by_value_walker), ctx_ptr);
    }
}

#[derive(Debug)]
struct LiteralReplaceByValueCtx {
    old_value: String,
    new_value: String,
    replaced_any: bool,
}

unsafe extern "C" fn literal_replace_by_value_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut LiteralReplaceByValueCtx);

        if (*node).type_ == pg_sys::NodeTag::T_A_Const {
            let a_const = node as *mut pg_sys::A_Const;
            let value_node = &(*a_const).val.node as *const pg_sys::Node as *mut pg_sys::Node;
            
            if !value_node.is_null() && (*value_node).type_ == pg_sys::NodeTag::T_String {
                let str_node = value_node as *mut pg_sys::String;
                let current_value = CStr::from_ptr((*str_node).sval)
                    .to_string_lossy()
                    .into_owned();
                
                let trimmed_current = current_value.trim().trim_matches(|c| c == '\'' || c == '"');
                let trimmed_old = ctx.old_value.trim().trim_matches(|c| c == '\'' || c == '"');
                
                if trimmed_current == trimmed_old {
                    let new_value_cstr = CString::new(ctx.new_value.as_str()).unwrap();
                    (*str_node).sval = pg_sys::pstrdup(new_value_cstr.as_ptr());
                    ctx.replaced_any = true;
                    // 첫 번째 매칭만 교체하고 중단
                    return false;
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(literal_replace_by_value_walker), ctx);
    }
}

/// strftime format -> date_part field 변환
fn strftime_to_datepart(format_str: &str) -> Option<String> {
    let format_map = [
        ("%Y", "year"),
        ("%y", "year"),
        ("%m", "month"),
        ("%d", "day"),
        ("%H", "hour"),
        ("%M", "minute"),
        ("%S", "second"),
        ("%w", "dow"),
        ("%j", "doy"),
        ("%U", "week"),
        ("%W", "week"),
        ("%c", "epoch"),
        ("%s", "epoch"),
        ("%z", "timezone"),
        ("%Z", "timezone_abbrev"),
    ];
    
    let trimmed = format_str.trim().trim_matches(|c| c == '\'' || c == '"');
    
    for (format, field) in format_map.iter() {
        if trimmed == *format {
            return Some(field.to_string());
        }
    }
    
    None
}

/* ------------------------------------------------
CASE 9) Column Reference Ambiguous - qualified reference로 변경
------------------------------------------------ */
/// Column ambiguity 에러에 대한 refinement 생성
/// FROM절의 테이블들 중에서 해당 컬럼을 가진 테이블을 찾아 qualified reference로 변경
pub fn generate_column_ambiguity_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    ambiguous_col: &str,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    
    // FROM절의 모든 테이블과 alias 추출
    let from_tables = extract_all_tables_from_raw(orig);
    if from_tables.is_empty() {
        return out;
    }
    
    let weight = COLUMN_AMBIGUITY_REFINEMENT_WEIGHT.get() as f32;
    
    // 모든 테이블명 추출 (실제 테이블명만, alias는 제외)
    let table_names: Vec<String> = from_tables.iter()
        .map(|t| t.table_name.clone())
        .collect();
    
    // 한 번에 해당 컬럼을 가진 테이블들 찾기
    let matching_table_names = find_tables_with_exact_column(&table_names, ambiguous_col);
    
    if matching_table_names.is_empty() {
        // pgrx::notice!("SafeQL: No tables found with exact column '{}' in FROM clause", ambiguous_col);
        return out;
    }
    
    // 매치된 테이블들에 대해서만 refinement 생성
    for table_info in from_tables.iter() {
        // 실제 테이블명이 매치된 목록에 있는지 확인
        if matching_table_names.contains(&table_info.table_name) {
            // distance는 1.0으로 고정
            let additional_priority = ((1.0 * 100.0) * weight) as i32;
            let cumulative_priority = base_priority + additional_priority;
            
            let cloned = unsafe { copy_node(orig) };
            
            // 참조할 때 사용할 이름 결정 (alias가 있으면 alias, 없으면 테이블명)
            let reference_name = table_info.get_reference_name();
            
            unsafe { 
                qualify_ambiguous_column_reference(cloned, ambiguous_col, reference_name, ambiguous_col);
            }
            out.push((cumulative_priority, cloned));
            
            // pgrx::notice!("SafeQL: Generated ambiguity refinement {} -> {}.{} (using {})", 
            //     ambiguous_col, reference_name, ambiguous_col, 
            //     if table_info.alias.is_some() { "alias" } else { "table name" });
        }
    }
    
    // pgrx::notice!("SafeQL: Generated {} column ambiguity refinements for '{}' from {} matching tables", 
    //     out.len(), ambiguous_col, matching_table_names.len());
    
    out
}

#[derive(Debug)]
struct AmbiguousColumnQualifyCtx {
    ambiguous_col: String,
    table_qualifier: String,
    new_column_name: String,
    replaced_any: bool,
}

/// ambiguous unqualified column reference를 qualified reference로 변경
unsafe fn qualify_ambiguous_column_reference(
    raw: *mut pg_sys::RawStmt,
    ambiguous_col: &str,
    table_qualifier: &str,
    new_column_name: &str,
) {
    let mut ctx = AmbiguousColumnQualifyCtx {
        ambiguous_col: ambiguous_col.to_ascii_lowercase(),
        table_qualifier: table_qualifier.to_string(),
        new_column_name: new_column_name.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut AmbiguousColumnQualifyCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(ambiguous_column_qualify_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL qualified ambiguous column: {} -> {}.{}", 
    //         ambiguous_col, table_qualifier, new_column_name);
    // }
}

unsafe extern "C" fn ambiguous_column_qualify_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut AmbiguousColumnQualifyCtx);

        if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
            let col_ref = node as *mut pg_sys::ColumnRef;

            if !(*col_ref).fields.is_null() {
                let mut should_stop = false;
                
                memcx::current_context(|mcx| {
                    if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                        let field_count = fields.len();
                        
                        // unqualified reference (단일 필드)만 처리
                        if field_count == 1 {
                            if let Some(field_ptr) = fields.get(0) {
                                let field = *field_ptr as *mut pg_sys::Node;
                                if (*field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = field as *mut pg_sys::String;
                                    let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                    
                                    // ambiguous column과 일치하면 qualified reference로 변경
                                    if column_name.to_ascii_lowercase() == ctx.ambiguous_col {
                                        // 새로운 qualified fields 리스트 생성
                                        let table_str = create_string_node(&ctx.table_qualifier);
                                        let column_str = create_string_node(&ctx.new_column_name);
                                        
                                        let table_cell = pg_sys::ListCell { ptr_value: table_str as *mut c_void };
                                        let column_cell = pg_sys::ListCell { ptr_value: column_str as *mut c_void };
                                        
                                        (*col_ref).fields = pg_sys::list_make2_impl(
                                            pg_sys::NodeTag::T_List,
                                            table_cell,
                                            column_cell
                                        );
                                        
                                        ctx.replaced_any = true;
                                        should_stop = true;
                                        
                                        // pgrx::notice!(
                                        //     "Qualified ambiguous column {} to {}.{}",
                                        //     ctx.ambiguous_col,
                                        //     ctx.table_qualifier,
                                        //     ctx.new_column_name
                                        // );
                                    }
                                }
                            }
                        }
                    }
                });
                
                if should_stop {
                    return false;
                }
            }
        }
    }

    unsafe {
        return safe_raw_expression_tree_walker(node, Some(ambiguous_column_qualify_walker), ctx);
    }
}


/* ------------------------------------------------
CASE 10) Value Refinement - literal 값을 실제 DB 값으로 교체
         모든 value를 한번에 refinement
------------------------------------------------ */
pub fn generate_value_refinements_raw(
    orig: *mut pg_sys::RawStmt,
    base_priority: i32
) -> Vec<(i32, *mut pg_sys::RawStmt)> {
    let mut out = Vec::new();
    let weight = VALUE_REFINEMENT_WEIGHT.get() as f32;

    let cloned = unsafe { copy_node(orig) };
    
    // SQL에서 모든 equality expression들을 찾기 (R.a = 'literal' 형태)
    let equality_expressions = unsafe { find_all_equality_expressions(cloned) };
    
    if equality_expressions.is_empty() {
        return out;
    }
    
    // 모든 expression에 대한 refinement 정보를 수집
    let mut all_refinements = Vec::new();
    let mut total_distance = 0.0f32;
    // let mut refinement_count = 0;
    
    for eq_expr in &equality_expressions {
        unsafe {
            if let Some((similar_value, table_name, column_name, distance)) = generate_value_refinements_for_expression(*eq_expr, cloned) {
                all_refinements.push((*eq_expr, similar_value, table_name, column_name, distance));
                total_distance += distance;
                // refinement_count += 1;
            }
        }
    }
    
    // refinement할 것이 있으면 모두 적용한 새로운 쿼리 생성
    if !all_refinements.is_empty() {
        let additional_priority = ((total_distance * 100.0) * weight) as i32;
        let cumulative_priority = base_priority + additional_priority;
        
        unsafe {
            // 모든 refinement를 한번에 적용
            for (expr_node, new_value, _table_name, _column_name, _distance) in &all_refinements {
                replace_literal_value_in_expression(cloned, *expr_node, new_value);
                // pgrx::notice!("SafeQL: Refined value for {}.{}: -> '{}' (distance: {})", 
                //     table_name, column_name, new_value, distance);
            }
            
            out.push((cumulative_priority, cloned));
            // pgrx::notice!("SafeQL: Generated value refinement with {} changes (total distance: {})", 
            //     refinement_count, total_distance);
        }
    }
    
    out
}



/// Value refinement를 위한 helper 함수들
unsafe fn find_all_equality_expressions(raw: *mut pg_sys::RawStmt) -> Vec<*mut pg_sys::Node> {
    let mut expressions = Vec::new();
    let expressions_ptr = &mut expressions as *mut Vec<*mut pg_sys::Node> as *mut c_void;
    
    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(find_equality_expressions_walker), expressions_ptr);
    }
    
    expressions
}

unsafe extern "C" fn find_equality_expressions_walker(
    node: *mut pg_sys::Node,
    ctx: *mut c_void
) -> bool {
    if node.is_null() {
        return false;
    }
    
    unsafe {
        let expressions = ctx as *mut Vec<*mut pg_sys::Node>;
        
        if (*node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = node as *mut pg_sys::A_Expr;
            
            if (*a_expr).kind == pg_sys::A_Expr_Kind::AEXPR_OP && !(*a_expr).name.is_null() {
                memcx::current_context(|mcx| {
                    if let Some(op_names) = List::<*mut c_void>::downcast_ptr_in_memcx((*a_expr).name, mcx) {
                        if let Some(op_name_ptr) = op_names.get(0) {
                            let op_name_node = *op_name_ptr as *mut pg_sys::Node;
                            if (*op_name_node).type_ == pg_sys::NodeTag::T_String {
                                let str_node = op_name_node as *mut pg_sys::String;
                                let op_name = CStr::from_ptr((*str_node).sval).to_string_lossy();
                                
                                if op_name == "=" {
                                    (*expressions).push(node);
                                }
                            }
                        }
                    }
                });
            }
        }
        
        safe_raw_expression_tree_walker(node, Some(find_equality_expressions_walker), ctx)
    }
}

unsafe fn generate_value_refinements_for_expression(
    expr_node: *mut pg_sys::Node,
    orig: *mut pg_sys::RawStmt
) -> Option<(String, String, String, f32)> {
    if expr_node.is_null() {
        return None;
    }
    
    unsafe {
        if (*expr_node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = expr_node as *mut pg_sys::A_Expr;
            
            let (column_ref, literal_value) = if is_column_ref((*a_expr).lexpr) && is_literal_value((*a_expr).rexpr) {
                ((*a_expr).lexpr, (*a_expr).rexpr)
            } else if is_literal_value((*a_expr).lexpr) && is_column_ref((*a_expr).rexpr) {
                ((*a_expr).rexpr, (*a_expr).lexpr)
            } else {
                return None;
            };
            
            if let Some((table_name, column_name)) = extract_table_column_from_ref(column_ref, orig) {
                if let Some(literal_str) = extract_literal_string_value(literal_value) {
                    // find_similar_values_for_literal가 하나만 리턴한다고 가정
                    if let Some((similar_value, _, _, distance)) = find_similar_values_for_literal(&table_name, &column_name, &literal_str).first() {
                        return Some((similar_value.clone(), table_name, column_name, *distance));
                    }
                }
            }
        }
    }
    
    None
}

unsafe fn is_column_ref(node: *mut pg_sys::Node) -> bool {
    unsafe {
        !node.is_null() && (*node).type_ == pg_sys::NodeTag::T_ColumnRef
    }
}

unsafe fn is_literal_value(node: *mut pg_sys::Node) -> bool {
    unsafe {
        !node.is_null() && (*node).type_ == pg_sys::NodeTag::T_A_Const
    }
}


unsafe fn extract_table_column_from_ref(
    column_ref: *mut pg_sys::Node,
    orig: *mut pg_sys::RawStmt
) -> Option<(String, String)> {
    if column_ref.is_null() {
        return None;
    }
    
    unsafe {
        let col_ref = column_ref as *mut pg_sys::ColumnRef;
        
        if !(*col_ref).fields.is_null() {
            return memcx::current_context(|mcx| {
                if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                    let field_count = fields.len();
                    
                    if field_count >= 2 {
                        // qualified reference (table.column)
                        if let (Some(first_ptr), Some(last_ptr)) = (fields.get(0), fields.get(field_count - 1)) {
                            let first_field = *first_ptr as *mut pg_sys::Node;
                            let last_field = *last_ptr as *mut pg_sys::Node;
                            
                            if (*first_field).type_ == pg_sys::NodeTag::T_String &&
                               (*last_field).type_ == pg_sys::NodeTag::T_String {
                                let first_str = first_field as *mut pg_sys::String;
                                let last_str = last_field as *mut pg_sys::String;
                                
                                let table_ref = CStr::from_ptr((*first_str).sval).to_string_lossy().into_owned();
                                let column_name = CStr::from_ptr((*last_str).sval).to_string_lossy().into_owned();
                                
                                let actual_table = find_actual_table_name(orig, &table_ref)
                                    .unwrap_or(table_ref);
                                
                                return Some((actual_table, column_name));
                            }
                        }
                    } else if field_count == 1 {
                        // unqualified reference - FROM절에서 테이블 추정
                        if let Some(field_ptr) = fields.get(0) {
                            let field = *field_ptr as *mut pg_sys::Node;
                            if (*field).type_ == pg_sys::NodeTag::T_String {
                                let str_node = field as *mut pg_sys::String;
                                let column_name = CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned();
                                
                                let from_tables = extract_all_tables_from_raw(orig);
                                if let Some(first_table) = from_tables.first() {
                                    return Some((first_table.table_name.clone(), column_name));
                                }
                            }
                        }
                    }
                }
                None
            });
        }
    }
    
    None
}

unsafe fn extract_literal_string_value(literal_node: *mut pg_sys::Node) -> Option<String> {
    if literal_node.is_null() {
        return None;
    }
    
    unsafe {
        if (*literal_node).type_ == pg_sys::NodeTag::T_A_Const {
            let a_const = literal_node as *mut pg_sys::A_Const;
            let value_node = &(*a_const).val.node as *const pg_sys::Node as *mut pg_sys::Node;
            
            if !value_node.is_null() && (*value_node).type_ == pg_sys::NodeTag::T_String {
                let str_node = value_node as *mut pg_sys::String;
                return Some(CStr::from_ptr((*str_node).sval).to_string_lossy().into_owned());
            }
        }
    }
    
    None
}

unsafe fn replace_literal_value_in_expression(
    raw: *mut pg_sys::RawStmt,
    target_expr: *mut pg_sys::Node,
    new_value: &str
) {
    let mut ctx = ValueReplaceCtx {
        target_expr,
        new_value: new_value.to_string(),
        replaced_any: false,
    };
    let ctx_ptr = &mut ctx as *mut ValueReplaceCtx as *mut c_void;

    unsafe {
        safe_raw_expression_tree_walker((*raw).stmt, Some(value_replace_walker), ctx_ptr);
    }

    // if ctx.replaced_any {
    //     pgrx::notice!("SafeQL replaced literal value with: {}", new_value);
    // }
}

#[derive(Debug)]
struct ValueReplaceCtx {
    target_expr: *mut pg_sys::Node,
    new_value: String,
    replaced_any: bool,
}

unsafe extern "C" fn value_replace_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    unsafe {
        let ctx_ref = &mut *(ctx as *mut ValueReplaceCtx);

        if node == ctx_ref.target_expr && (*node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = node as *mut pg_sys::A_Expr;
            
            if is_literal_value((*a_expr).rexpr) {
                let new_literal = create_string_literal(&ctx_ref.new_value);
                (*a_expr).rexpr = new_literal;
                ctx_ref.replaced_any = true;
                return false;
            } else if is_literal_value((*a_expr).lexpr) {
                let new_literal = create_string_literal(&ctx_ref.new_value);
                (*a_expr).lexpr = new_literal;
                ctx_ref.replaced_any = true;
                return false;
            }
        }
        
        safe_raw_expression_tree_walker(node, Some(value_replace_walker), ctx)
    }
}

unsafe fn create_string_literal(value: &str) -> *mut pg_sys::Node {
    unsafe {
        let a_const =
            pg_sys::palloc0(size_of::<pg_sys::A_Const>()) as *mut pg_sys::A_Const;
        (*a_const).type_ = pg_sys::NodeTag::T_A_Const;
    
        // CString으로 변환하여 null-terminated string 보장
        let c_str = CString::new(value).expect("CString conversion failed");
        
        // makeString은 Value* 반환
        let str_val = pg_sys::makeString(pg_sys::pstrdup(c_str.as_ptr()));
    
        // ac->val.sval = *makeString(...)
        (*a_const).val.sval = *str_val;
    
        // 필요시 null 여부나 다른 case 구분 가능
        (*a_const).location = -1;
    
        a_const as *mut pg_sys::Node
    }
}


/* ------------------------------------------------
-------------------- Helper 함수 -----------------
------------------------------------------------ */
/// SelectStmt 찾기
unsafe fn find_select_stmt(raw: *mut pg_sys::RawStmt) -> Option<*mut pg_sys::SelectStmt> {
    unsafe {
        if !raw.is_null() && !(*raw).stmt.is_null() {
            if (*(*raw).stmt).type_ == pg_sys::NodeTag::T_SelectStmt {
                return Some((*raw).stmt as *mut pg_sys::SelectStmt);
            }
        }
        None
    }
}

/// RangeVar 생성
unsafe fn create_range_var(table_name: &CString) -> *mut pg_sys::RangeVar {
    unsafe {
        pg_sys::makeRangeVar(
            std::ptr::null_mut(),        
            pg_sys::pstrdup(table_name.as_ptr()), 
            -1,                          
        )
    }
}

/// FROM 리스트에 새 테이블 추가
unsafe fn add_table_to_from_list(from_list: *mut pg_sys::List, table_node: *mut pg_sys::Node) {
    unsafe {
        // null 포인터 체크
        if table_node.is_null() {
            pgrx::error!("table_node가 null입니다");
        }

        // lappend는 from_list가 null이거나 비어있어도 안전하게 처리
        pg_sys::lappend(from_list, table_node as *mut c_void);
    }
}

/// WHERE절에 조건 추가
unsafe fn add_condition_to_where_clause(
    select_stmt: *mut pg_sys::SelectStmt,
    new_condition: *mut pg_sys::Node
) {
    unsafe {
        if (*select_stmt).whereClause.is_null() {
            (*select_stmt).whereClause = new_condition;
        } else {
            let existing_where = (*select_stmt).whereClause;
            let and_expr = create_and_expr(existing_where, new_condition);
            (*select_stmt).whereClause = and_expr;
        }
    }
}

/// JOIN 조건 표현식 생성 (table1.col1 = table2.col2)
unsafe fn create_join_condition_expr(condition: &JoinCondition) -> *mut pg_sys::Node {
    unsafe {
        let left_col_ref = create_column_ref(&condition.left_table, &condition.left_column);
        let right_col_ref = create_column_ref(&condition.right_table, &condition.right_column);
        create_equality_expr(left_col_ref, right_col_ref)
    }
}

/// 컬럼 참조 생성 (table.column)
unsafe fn create_column_ref(table_name: &str, column_name: &str) -> *mut pg_sys::Node {
    unsafe {
        let col_ref = pg_sys::palloc0(size_of::<pg_sys::ColumnRef>()) as *mut pg_sys::ColumnRef;
        (*col_ref).type_ = pg_sys::NodeTag::T_ColumnRef;
        
        let table_str = create_string_node(table_name);
        let column_str = create_string_node(column_name);
        
        let table_cell = pg_sys::ListCell { ptr_value: table_str as *mut c_void };
        let column_cell = pg_sys::ListCell { ptr_value: column_str as *mut c_void };
        
        (*col_ref).fields = pg_sys::list_make2_impl(
            pg_sys::NodeTag::T_List,
            table_cell,
            column_cell
        );
        (*col_ref).location = -1;
        
        col_ref as *mut pg_sys::Node
    }
}

/// String 노드 생성
unsafe fn create_string_node(value: &str) -> *mut pg_sys::Node {
    unsafe {
        let str_node = pg_sys::palloc0(size_of::<pg_sys::String>()) as *mut pg_sys::String;
        (*str_node).type_ = pg_sys::NodeTag::T_String;
        let cstr = CString::new(value).unwrap();
        (*str_node).sval = pg_sys::pstrdup(cstr.as_ptr());
        str_node as *mut pg_sys::Node
    }
}

/// 등식 표현 생성 (left = right)
unsafe fn create_equality_expr(left: *mut pg_sys::Node, right: *mut pg_sys::Node) -> *mut pg_sys::Node {
    unsafe {
        let expr = pg_sys::palloc0(size_of::<pg_sys::A_Expr>()) as *mut pg_sys::A_Expr;
        (*expr).type_ = pg_sys::NodeTag::T_A_Expr;
        (*expr).kind = pg_sys::A_Expr_Kind::AEXPR_OP;
        
        let eq_str = create_string_node("=");
        let eq_cell = pg_sys::ListCell { ptr_value: eq_str as *mut c_void };
        (*expr).name = pg_sys::list_make1_impl(pg_sys::NodeTag::T_List, eq_cell);
        (*expr).lexpr = left;
        (*expr).rexpr = right;
        (*expr).location = -1;
        
        expr as *mut pg_sys::Node
    }
}

/// AND 표현 생성 (left AND right)
unsafe fn create_and_expr(left: *mut pg_sys::Node, right: *mut pg_sys::Node) -> *mut pg_sys::Node {
    unsafe {
        let expr = pg_sys::palloc0(size_of::<pg_sys::BoolExpr>()) as *mut pg_sys::BoolExpr;
        (*expr).xpr.type_ = pg_sys::NodeTag::T_BoolExpr;
        (*expr).boolop = pg_sys::BoolExprType::AND_EXPR;
        let left_cell = pg_sys::ListCell { ptr_value: left as *mut c_void };
        let right_cell = pg_sys::ListCell { ptr_value: right as *mut c_void };
        (*expr).args = pg_sys::list_make2_impl(pg_sys::NodeTag::T_List, left_cell, right_cell); // 두 개 조건을 리스트로 묶음
        (*expr).location = -1;
    
        expr as *mut pg_sys::Node
    }
}

/// 단일 항목 List 생성
unsafe fn create_single_item_list(node: *mut pg_sys::Node) -> *mut pg_sys::List {
    unsafe {
        // 빈 리스트를 생성한 후 항목 추가
        let list = pg_sys::list_make1_impl(
            pg_sys::NodeTag::T_List,
            pg_sys::ListCell { ptr_value: node as *mut c_void }
        );
        list
    }
}

/// "unrecognized node type" 에러를 안전하게 처리하는 walker 호출
pub unsafe fn safe_raw_expression_tree_walker(
    node: *mut pg_sys::Node,
    walker: Option<unsafe extern "C" fn(*mut pg_sys::Node, *mut c_void) -> bool>,
    context: *mut c_void
) -> bool {
    use pgrx::pg_sys::panic::CaughtError;
    
    let result = pgrx::PgTryBuilder::new(|| {
        unsafe {
            pg_sys::raw_expression_tree_walker(node, walker, context)
        }
    })
    .catch_others(|e| {
        let error_msg = match e {
            CaughtError::PostgresError(ref err_report) | 
            CaughtError::ErrorReport(ref err_report) => {
                err_report.message().to_string()
            },
            CaughtError::RustPanic { ref ereport, .. } => {
                ereport.message().to_string()
            }
        };
        
        if error_msg.contains("unrecognized node type") {
            false // 이 에러만 무시하고 false 반환
        } else {
            // 다른 에러는 다시 throw하기 위해 panic
            match e {
                CaughtError::PostgresError(err_report) => {
                    panic!("PostgreSQL error: {}", err_report.message());
                },
                CaughtError::ErrorReport(err_report) => {
                    panic!("Error report: {}", err_report.message());
                },
                CaughtError::RustPanic { ereport, .. } => {
                    panic!("Rust panic: {}", ereport.message());
                }
            }
        }
    })
    .execute();
    
    return result;
}

/// "schema.table" → (Some(schema), table), "table" → (None, table)
fn split_schema_rel(fq: &str) -> Option<(Option<String>, String)> {
    let mut parts = fq.split('.').collect::<Vec<_>>();
    if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        Some((None, parts.remove(0).to_string()))
    } else {
        let rel = parts.pop().unwrap().to_string();
        let schema = parts.join("."); // 다중 스키마 경로는 일반적이진 않지만 안전하게 join
        Some((Some(schema), rel))
    }
}