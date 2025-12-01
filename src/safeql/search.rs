use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::ffi::{CString, CStr};
use std::os::raw::c_void;

use pgrx::spi::Spi;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::{memcx, pg_sys, prelude::*};
use pgrx::list::List;
use pgrx::nodes::node_to_string;
use regex::Regex;

use super::score::{
    extract_function_info_from_error,
    check_function_exists,
};

use super::refine::{
    safe_raw_expression_tree_walker,
    generate_table_refinements_for_all_from_tables_raw,
    generate_table_refinements_raw,
    generate_column_refinements_raw,
    generate_table_refinements_for_column_error_raw,
    generate_table_refinements_for_all_tables_raw,
    generate_column_table_reference_refinements_raw,
    generate_one_hop_join_refinements_for_all_tables_raw,
    generate_join_refinements_raw,
    generate_function_argument_column_refinements_raw,
    generate_function_typecast_refinements_raw,
    generate_function_name_refinements_raw,
    generate_argument_format_refinements_raw,
    generate_operand_column_refinements_raw,
    generate_operand_typecast_refinements_raw,
    generate_column_ambiguity_refinements_raw,
    generate_value_refinements_raw,
    find_all_where_expressions,
    extract_operator_info_from_expr,
    ColumnOperand,
};
use super::utils::copy_node;
use crate::softql::deparse_raw_stmt;
use crate::gucs::parser::{
    ENABLE_SAFEQL_REFINEMENT,
    ENABLE_TABLE_REFINEMENT,
    ENABLE_COLUMN_REFINEMENT,
    ENABLE_TABLE_FOR_COLUMN,
    ENABLE_COLUMN_TABLE_REFERENCE,
    ENABLE_JOIN_REFINEMENT,
    ENABLE_OPERAND_COLUMN_REFINEMENT,
    ENABLE_OPERAND_TABLE_FOR_COLUMN_REFINEMENT,
    ENABLE_OPERAND_COLUMN_TABLE_REFERENCE_REFINEMENT,
    ENABLE_OPERAND_TYPECAST_REFINEMENT,
    ENABLE_ARGUMENT_COLUMN_REFINEMENT,
    ENABLE_ARGUMENT_TYPECAST_REFINEMENT,
    ENABLE_FUNCTION_NAME_REFINEMENT,
    ENABLE_COLUMN_AMBIGUITY_REFINEMENT,
    ENABLE_VALUE_REFINEMENT,
    MAX_REFINEMENT_HOP,
    MAX_REFINEMENT_NUM,
};

#[derive(Debug, Clone)]
enum AnalyzeOutcome {
    Success(*mut pg_sys::Query),
    Failure { 
        code: Option<PgSqlErrorCode>, 
        message: String,
        cursor_pos: Option<i32>,
    },
}

enum ExecutionOutcome {
    Success,
    ExecutionError(String),
    EmptyResult,
    ArgumentFormatError { message: String },
}

/// Expression들에 대해 operand refinement를 수행하는 공통 함수
fn process_operand_refinements_for_expressions(
    cand_raw: *mut pg_sys::RawStmt,
    expressions: Vec<*mut pg_sys::Node>,
    current_prio: i32,
    pq: &mut BinaryHeap<Reverse<(i32, u64, *mut pg_sys::RawStmt, i32)>>,
    visited: &mut HashSet<String>,
    current_hop_count: i32,
    seq: &mut u64,
) -> bool {
    let mut refinements_added = false;
    
    for expr_node in expressions {
        unsafe {
            if let Some((left_operand, right_operand)) = extract_column_operands_from_expr(expr_node) {
                // 1. Column refinement
                if ENABLE_OPERAND_COLUMN_REFINEMENT.get() {
                    let operand_column_refinements = generate_operand_column_refinements_raw(
                        cand_raw,
                        &left_operand,
                        &right_operand,
                        current_prio
                    );
                    
                    for (new_prio, refined_raw) in operand_column_refinements {
                        push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                        refinements_added = true;
                    }
                }
                
                // 2. Left operand에 대한 table/reference refinements
                if let Some(ColumnOperand { table_name, column_name }) = &left_operand {
                    refinements_added |= process_operand_table_refinements(
                        cand_raw, table_name, column_name, current_prio,
                        pq, visited, current_hop_count, seq
                    );
                }
                
                // 3. Right operand에 대한 table/reference refinements
                if let Some(ColumnOperand { table_name, column_name }) = &right_operand {
                    refinements_added |= process_operand_table_refinements(
                        cand_raw, table_name, column_name, current_prio,
                        pq, visited, current_hop_count, seq
                    );
                }
                
                // 4. Typecast refinement
                if ENABLE_OPERAND_TYPECAST_REFINEMENT.get() {
                    let operator_info = extract_operator_info_from_expr(expr_node);
                    let operand_typecast_refinements = generate_operand_typecast_refinements_raw(
                        cand_raw,
                        &left_operand,
                        &right_operand,
                        &operator_info,
                        current_prio
                    );
                    
                    for (new_prio, refined_raw) in operand_typecast_refinements {
                        push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                        refinements_added = true;
                    }
                }
            }
        }
    }
    
    refinements_added
}

/// 단일 operand에 대한 table/reference refinements 처리
fn process_operand_table_refinements(
    cand_raw: *mut pg_sys::RawStmt,
    table_name: &Option<String>,
    column_name: &str,
    current_prio: i32,
    pq: &mut BinaryHeap<Reverse<(i32, u64, *mut pg_sys::RawStmt, i32)>>,
    visited: &mut HashSet<String>,
    current_hop_count: i32,
    seq: &mut u64,
) -> bool {
    let mut refinements_added = false;
    
    // Table-for-column refinement
    if ENABLE_OPERAND_TABLE_FOR_COLUMN_REFINEMENT.get() {
        if let Some(ref table_name_str) = table_name {
            let refinements = generate_table_refinements_for_column_error_raw(
                cand_raw,
                table_name_str,
                current_prio 
            );
            
            for (new_prio, refined_raw) in refinements {
                push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                refinements_added = true;
            }
        } else {
            let refinements = generate_table_refinements_for_all_tables_raw(
                cand_raw,
                current_prio
            );
            
            for (new_prio, refined_raw) in refinements {
                push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                refinements_added = true;
            }
        }
    }
    
    // Column table reference refinement
    if ENABLE_OPERAND_COLUMN_TABLE_REFERENCE_REFINEMENT.get() {
        if table_name.is_some() {
            let refinements = generate_column_table_reference_refinements_raw(
                cand_raw,
                table_name.as_deref(),
                column_name,
                current_prio
            );
            
            for (new_prio, refined_raw) in refinements {
                push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                refinements_added = true;
            }
        }
    }
    
    refinements_added
}

/// Column들에 대해 JOIN refinement를 수행하는 공통 함수
fn process_join_refinements_for_columns(
    cand_raw: *mut pg_sys::RawStmt,
    columns: Vec<String>,
    current_prio: i32,
    pq: &mut BinaryHeap<Reverse<(i32, u64, *mut pg_sys::RawStmt, i32)>>,
    visited: &mut HashSet<String>,
    current_hop_count: i32,
    seq: &mut u64,
) -> bool {
    let mut refinements_added = false;
    
    if ENABLE_JOIN_REFINEMENT.get() {
        for column_name in columns {
            let join_refinements = generate_join_refinements_raw(
                cand_raw,
                &column_name,
                current_prio
            );
            
            for (new_prio, refined_raw) in join_refinements {
                push_candidate(pq, visited, new_prio, current_hop_count + 1, seq, refined_raw);
                refinements_added = true;
            }
        }
    }
    
    refinements_added
}


/// 공통 refinement search 로직 - 성공한 RawStmt*를 반환
pub fn perform_refinement_search(
    sql: &str, 
    pstate: *mut pg_sys::ParseState
) -> *mut pg_sys::RawStmt {
    // SafeQL refinement가 전체적으로 비활성화되어 있으면 원본 반환
    if !ENABLE_SAFEQL_REFINEMENT.get() {
        pgrx::notice!("SafeQL: Refinement is globally disabled, performing standard analysis");
        let init_raw = rawstmt_from_sql(sql).unwrap_or_else(|e| {
            pgrx::error!("SQL parse error: {}", e);
        });
        
        match try_analyze_raw_once(sql, init_raw, pstate) {
            AnalyzeOutcome::Success(_) => return init_raw,
            AnalyzeOutcome::Failure { code, message, .. } => {
                raise_saved_error(code, message);
            }
        }
    }

    // 제한값 설정
    let max_hops = MAX_REFINEMENT_HOP.get();
    let max_searches = MAX_REFINEMENT_NUM.get();
    let mut search_count = 0;

    // 1) 초기 RawStmt*
    let init_raw = rawstmt_from_sql(sql).unwrap_or_else(|e| {
        pgrx::error!("SQL parse error: {}", e);  // pgrx::error! 매크로 사용
    });
    
    // 2) PQ: (prio, seq, RawStmt*, hop_count)
    let mut pq: BinaryHeap<Reverse<(i32, u64, *mut pg_sys::RawStmt, i32)>> = BinaryHeap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut seq: u64 = 0;

    let init_analyze_raw = unsafe { copy_node(init_raw) };
    push_candidate(&mut pq, &mut visited, 0, 0, &mut seq, init_analyze_raw);

    while let Some(Reverse((current_prio, _seq, cand_raw, current_hop_count))) = pq.pop() {
        search_count += 1;
        
        // 최대 탐색 횟수 체크
        if search_count > max_searches {
            pgrx::notice!("SafeQL: Reached maximum search limit ({}), stopping refinement", max_searches);
            break;
        }
        
        // 최대 hop 수 체크
        if current_hop_count > max_hops {
            pgrx::notice!("SafeQL: Candidate with hop count {} exceeds maximum ({}) - skipping", 
                         current_hop_count, max_hops);
            continue;
        }

        // RawStmt* 복사, 안그러면 RawStmt 에서 JoinExpr 등이 공유되어 변형 시 서로 꼬임
        let analyze_raw = unsafe { copy_node(cand_raw) };
        match try_analyze_raw_once(sql, analyze_raw, pstate) {
            AnalyzeOutcome::Success(_q) => {
                // 성공한 쿼리에 대해 실행 테스트 수행
                let execute_raw = unsafe { copy_node(cand_raw) };
                // print current priority
                // pgrx::notice!("SafeQL: Analyzed candidate with priority {}", current_prio);
                match try_execute_query(execute_raw, sql) {
                    ExecutionOutcome::Success => {
                        // 실행도 성공 - refined RawStmt 반환
                        unsafe {
                            let refined_sql = node_to_string(cand_raw as *mut pg_sys::Node)
                                .unwrap_or("<failed-to-serialize>").to_string();
                            pgrx::notice!("SafeQL successfully refined and validated SQL: {}", refined_sql);
                        }
                        return cand_raw;
                    },
                    ExecutionOutcome::ArgumentFormatError { message } => {
                        if ENABLE_FUNCTION_NAME_REFINEMENT.get() {
                            pgrx::notice!("SafeQL: Detected argument format error during execution: {}", message);
                            
                            let format_refinements = generate_argument_format_refinements_raw(
                                cand_raw,
                                &message,
                                current_prio
                            );
                            
                            for (new_prio, refined_raw) in format_refinements {
                                push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                            }
                            continue;
                        }
                    },
                    ExecutionOutcome::ExecutionError(exec_error) => {
                        // 실행 에러가 발생한 경우 다음 후보 시도
                        unsafe {
                            let refined_sql = node_to_string(cand_raw as *mut pg_sys::Node)
                                .unwrap_or("<failed-to-serialize>").to_string();
                            pgrx::notice!("SafeQL refined SQL failed execution test: {} - Error: {}", 
                                         refined_sql, exec_error);
                        }
                        continue;
                    },
                    ExecutionOutcome::EmptyResult => {
                        pgrx::notice!("SafeQL: Query returned empty result, attempting comprehensive refinements");
                        let mut refinements_added = false;
                        
                        // 1. Value refinement
                        if ENABLE_VALUE_REFINEMENT.get() {
                            let value_refinements = generate_value_refinements_raw(cand_raw, current_prio);
                            for (new_prio, refined_raw) in value_refinements {
                                push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                refinements_added = true;
                            }
                        }

                        // 2. FROM절의 모든 테이블에 대해 table refinement
                        if ENABLE_TABLE_REFINEMENT.get() {
                            let table_refinements = generate_table_refinements_for_all_from_tables_raw(
                                cand_raw,
                                current_prio
                            );
                            
                            for (new_prio, refined_raw) in table_refinements {
                                push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                refinements_added = true;
                            }
                        }
                        
                        // 3. WHERE절의 모든 표현식에 대해 operand refinement
                        let where_exprs = unsafe { find_all_where_expressions(cand_raw) };
                        if !where_exprs.is_empty() {
                            refinements_added |= process_operand_refinements_for_expressions(
                                cand_raw,
                                where_exprs,
                                current_prio,
                                &mut pq,
                                &mut visited,
                                current_hop_count,
                                &mut seq
                            );
                        }
                        
                        // 4. FROM절의 모든 테이블에 대해 1-hop JOIN refinement
                        if ENABLE_JOIN_REFINEMENT.get() {
                            let join_add_refinements = generate_one_hop_join_refinements_for_all_tables_raw(
                                cand_raw,
                                current_prio
                            );
                            
                            for (new_prio, refined_raw) in join_add_refinements {
                                push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                refinements_added = true;
                            }
                        }
                        
                        if refinements_added {
                            continue;
                        }
                        
                        // 모든 refinement 실패
                        continue;
                    }
                }
            },
            AnalyzeOutcome::Failure { code, message, cursor_pos } => {
                match code {
                    Some(PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE) => {
                        // CASE 1) FROM Rel -> FROM Rel' - 테이블 refinement 수행
                        if ENABLE_TABLE_REFINEMENT.get() {
                            if let Some(missing_rel) = extract_missing_relation(&message) {
                                pgrx::notice!("SafeQL: Executing table refinement for missing table '{}'", missing_rel);
                                // 후보 테이블들을 RawStmt 변형으로 생성해서 PQ에 넣는다
                                let refinements = generate_table_refinements_raw(
                                    cand_raw, 
                                    &missing_rel,
                                    current_prio
                                );
        
                                for (new_prio, refined_raw) in refinements {
                                    push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                }
                                continue;
                            }
                        }
                    },
                    Some(PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN) => {
                        if let Some((table_name, missing_col)) = extract_missing_column(&message) {
                            let mut refinements_added = false;

                            // CASE 2) SELECT Att -> SELECT Att' - 칼럼 refinement 수행
                            if ENABLE_COLUMN_REFINEMENT.get() {
                                pgrx::notice!("SafeQL: Executing column refinement for missing column '{}'", missing_col);
                                let column_refinements = generate_column_refinements_raw(
                                    cand_raw,
                                    table_name.as_deref(),
                                    &missing_col,
                                    current_prio 
                                );
                                for (new_prio, refined_raw) in column_refinements {
                                    push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                    refinements_added = true;
                                }
                            }

                            // CASE 3) FROM Rel -> FROM Rel' - 칼럼이 참조하는 테이블 refinement 수행
                            if ENABLE_TABLE_FOR_COLUMN.get() {
                                if let Some(ref table_name_str) = table_name {
                                    // 1) 테이블 이름이 명확한 경우, 해당 테이블에 대해서만 refinement 시도
                                    let table_refinements = generate_table_refinements_for_column_error_raw(
                                        cand_raw,
                                        table_name_str,
                                        current_prio 
                                    );
                                    
                                    for (new_prio, refined_raw) in table_refinements {
                                        push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                        refinements_added = true;
                                    }
                                } else {
                                    // 2) 테이블 이름이 특정되지 않은 경우, FROM절의 모든 테이블에 대해 refinement 시도
                                    let all_table_refinements = generate_table_refinements_for_all_tables_raw(
                                        cand_raw,
                                        current_prio
                                    );
                                    
                                    for (new_prio, refined_raw) in all_table_refinements {
                                        push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                        refinements_added = true;
                                    }
                                }
                            }

                            // CASE 4) FROM Rel WHERE col ... -> FROM Rel Rel2 WHERE ... - column reference refinement
                            if ENABLE_COLUMN_TABLE_REFERENCE.get() {
                                if table_name.is_some() {
                                    let reference_refinements = generate_column_table_reference_refinements_raw(
                                        cand_raw,
                                        table_name.as_deref(),
                                        &missing_col,
                                        current_prio
                                    );
                                    
                                    for (new_prio, refined_raw) in reference_refinements {
                                        push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                        refinements_added = true;
                                    }
                                }
                            }

                            // CASE 5) JOIN refinement
                            refinements_added |= process_join_refinements_for_columns(
                                cand_raw,
                                vec![missing_col.clone()],
                                current_prio,
                                &mut pq,
                                &mut visited,
                                current_hop_count,
                                &mut seq
                            );

                            if refinements_added {
                                continue;
                            }
                        }
                    },
                    Some(PgSqlErrorCode::ERRCODE_UNDEFINED_FUNCTION) => {
                        // CASE 6) Function does not exist - function argument refinement 수행
                        if let Some((function_name, arg_types)) = extract_missing_function(&message) {
                            if let Some(error_pos) = cursor_pos {
                                let mut refinements_added = false;

                                // 에러 위치에서 문제가 된 함수 호출 찾기
                                let problematic_function_calls = unsafe { find_function_calls_at_position(cand_raw, error_pos) };
                                // 함수 정보 추출
                                let function_info = extract_function_info_from_error(&message);

                                for func_call_node in problematic_function_calls {
                                    unsafe {
                                        if check_function_exists(&function_name) {
                                            if let Some(argument_operands) = extract_argument_operands_from_function_call(func_call_node) {
                                                if ENABLE_ARGUMENT_COLUMN_REFINEMENT.get() {
                                                    let argument_column_refinements = generate_function_argument_column_refinements_raw(
                                                        cand_raw,
                                                        &argument_operands,
                                                        error_pos,
                                                        &function_name,
                                                        &arg_types,
                                                        current_prio
                                                    );
                                                    
                                                    for (new_prio, refined_raw) in argument_column_refinements {
                                                        push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                                        refinements_added = true;
                                                    }
                                                }
                                            }
    
                                            if ENABLE_ARGUMENT_TYPECAST_REFINEMENT.get() {
                                                let argument_typecast_refinements = generate_function_typecast_refinements_raw(
                                                    cand_raw,
                                                    &function_info,
                                                    error_pos,
                                                    &function_name,
                                                    &arg_types,
                                                    &message,
                                                    current_prio
                                                );
    
                                                for (new_prio, refined_raw) in argument_typecast_refinements {
                                                    push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                                    refinements_added = true;
                                                }
                                            }
                                        }

                                        if ENABLE_FUNCTION_NAME_REFINEMENT.get() {
                                            let function_name_refinements = generate_function_name_refinements_raw(
                                                cand_raw,
                                                error_pos,
                                                &function_name,
                                                &arg_types,
                                                current_prio
                                            );
                                            
                                            for (new_prio, refined_raw) in function_name_refinements {
                                                push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                                refinements_added = true;
                                            }
                                        }
                                    }
                                }
                                
                                if refinements_added {
                                    continue;
                                }
                            }
                        } else if is_operator_type_error(&message) {
                            // CASE 6-4) Operand Type Mismatch
                            if let Some(error_pos) = cursor_pos {
                                pgrx::notice!("SafeQL: Executing operand refinement for operator error at position {}", error_pos);
                                
                                let problematic_exprs = unsafe { find_expressions_at_position(cand_raw, error_pos) };
                                
                                let refinements_added = process_operand_refinements_for_expressions(
                                    cand_raw,
                                    problematic_exprs,
                                    current_prio,
                                    &mut pq,
                                    &mut visited,
                                    current_hop_count,
                                    &mut seq
                                );
                                
                                if refinements_added {
                                    continue;
                                }
                            }
                        }
                    },
                    Some(PgSqlErrorCode::ERRCODE_AMBIGUOUS_COLUMN) => {
                        // CASE 7) Column reference ambiguous - qualified reference로 변경
                        if ENABLE_COLUMN_AMBIGUITY_REFINEMENT.get() {
                            if let Some(ambiguous_col) = extract_ambiguous_column(&message) {
                                pgrx::notice!("SafeQL: Executing column ambiguity refinement for ambiguous column '{}'", ambiguous_col);
                                
                                let ambiguity_refinements = generate_column_ambiguity_refinements_raw(
                                    cand_raw,
                                    &ambiguous_col,
                                    current_prio
                                );
                                
                                for (new_prio, refined_raw) in ambiguity_refinements {
                                    push_candidate(&mut pq, &mut visited, new_prio, current_hop_count + 1, &mut seq, refined_raw);
                                }
                                continue;
                            }
                        }
                    },
                    _ => {
                        pgrx::notice!("SafeQL: Analysis error message: {}", message);
                    }  // 다른 에러는 무시하고 PQ에 저장된 RawStmt들로 계속 진행
                }
            }
        }
    }

    // 모든 refinement 시도 실패 - 원본으로 리턴
    return init_raw;
    // match try_analyze_raw_once(sql, init_raw, pstate) {
    //     AnalyzeOutcome::Success(_) => return init_raw,
    //     AnalyzeOutcome::Failure { code, message, .. } => {
    //         raise_saved_error(code, message);
    //     }
    // }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// SafeQL refinement를 수행하고 refined Query를 반환
pub fn analyze_with_refinement(sql: &str, pstate: *mut pg_sys::ParseState) -> *mut pg_sys::Query {
    let refined_raw = perform_refinement_search(sql, pstate);
    
    // refined RawStmt를 다시 analyze해서 Query 반환
    match try_analyze_raw_once(sql, refined_raw, pstate) {
        AnalyzeOutcome::Success(q) => q,
        AnalyzeOutcome::Failure { code, message, .. } => {
            raise_saved_error(code, message);
        }
    }
}


struct ExecutionErrorInfo {
    code: Option<PgSqlErrorCode>,
    message: String,
}

fn try_execute_query(rawstmt: *mut pg_sys::RawStmt, _source_sql: &str) -> ExecutionOutcome {
    let refined_sql = deparse_raw_stmt(rawstmt);
    pgrx::notice!("Executing refined SQL: {}", refined_sql);
    
    unsafe {
        // 현재 컨텍스트와 owner 저장
        let old_context = pg_sys::CurrentMemoryContext;
        let old_owner = pg_sys::CurrentResourceOwner;
        
        // Subtransaction 시작 - 리소스 격리를 위해
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pg_sys::MemoryContextSwitchTo(old_context);
        
        let result = PgTryBuilder::new(|| {
            pg_sys::MemoryContextSwitchTo(old_context);
            
            Spi::connect(|client| {
                // read-only 모드로 쿼리 실행
                match client.select(&refined_sql, None, None) {
                    Ok(tuple_table) => {
                        // 결과 행 수 확인
                        if tuple_table.is_empty() {
                            Err(ExecutionErrorInfo {
                                code: None,
                                message: "EMPTY_RESULT".to_string(),
                            })
                        } else {
                            Ok(())
                        }
                    }
                    Err(_) => {
                        Err(ExecutionErrorInfo {
                            code: None,
                            message: "EXECUTION_FAILED".to_string(),
                        })
                    }
                }
            })?;
            
            // 성공 시 subtransaction 커밋
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            
            Ok(())
        })
        .catch_others(|e| {
            // 에러 발생 시 명시적으로 subtransaction 롤백
            pg_sys::MemoryContextSwitchTo(old_context);
            
            let (code, message) = match e {
                CaughtError::PostgresError(ref err_report) | 
                CaughtError::ErrorReport(ref err_report) => {
                    (Some(err_report.sql_error_code()), err_report.message().to_string())
                },
                CaughtError::RustPanic { ref ereport, .. } => {
                    (Some(ereport.sql_error_code()), 
                     format!("Rust panic during SPI execution: {}", ereport.message()))
                }
            };
            
            // 롤백 및 리소스 정리
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            
            Err(ExecutionErrorInfo { code, message })
        })
        .execute();
        
        // 최종 컨텍스트 복원
        pg_sys::MemoryContextSwitchTo(old_context);
        pg_sys::CurrentResourceOwner = old_owner;
        
        // ExecutionOutcome으로 변환
        match result {
            Ok(_) => ExecutionOutcome::Success,
            Err(error_info) => {
                if error_info.message == "EMPTY_RESULT" {
                    ExecutionOutcome::EmptyResult
                } else if let Some(PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE) = error_info.code {
                    ExecutionOutcome::ArgumentFormatError {
                        message: error_info.message.clone()
                    }
                } else {
                    ExecutionOutcome::ExecutionError(error_info.message.clone())
                }
            }
        }
    }
}


/// RawStmt* 로 단 한 번 analyze하고, 에러를 캡처해 돌려준다.
fn try_analyze_raw_once(source_sql: &str, rawstmt: *mut pg_sys::RawStmt, pstate: *mut pg_sys::ParseState) -> AnalyzeOutcome {
    use pgrx::pg_sys::panic::CaughtError;
    
    unsafe {
        // 현재 컨텍스트와 owner 저장
        let old_context = pg_sys::CurrentMemoryContext;
        let old_owner = pg_sys::CurrentResourceOwner;
        
        // Subtransaction 시작
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pg_sys::MemoryContextSwitchTo(old_context);
        
        let csql = CString::new(source_sql).unwrap_or_else(|_| CString::new("").unwrap());
        
        let result = PgTryBuilder::new(|| {
            set_subselect_mode(pstate, true);
            pg_sys::MemoryContextSwitchTo(old_context);
            let q = pg_sys::parse_analyze_fixedparams(
                rawstmt,
                csql.as_ptr() as *const i8,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
            );
            set_subselect_mode(pstate, false);
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            Ok(q)
        })
        .catch_others(|e| {
            set_subselect_mode(pstate, false);
            pg_sys::MemoryContextSwitchTo(old_context);
            
            let outcome = match e {
                CaughtError::PostgresError(ref _err_report) | 
                CaughtError::ErrorReport(ref _err_report) => {
                    let (code, full_message, cursor_pos) = format_complete_error_message(source_sql);
                    AnalyzeOutcome::Failure {
                        code: code,
                        message: full_message,
                        cursor_pos: cursor_pos, 
                    }
                },
                CaughtError::RustPanic { ref ereport, .. } => {
                    AnalyzeOutcome::Failure {
                        code: Some(ereport.sql_error_code()),
                        message: ereport.message().to_string(),
                        cursor_pos: get_error_cursor_pos(),
                    }
                }
            };

            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            
            Err(outcome)
        })
        .execute();
        
        pg_sys::MemoryContextSwitchTo(old_context);
        pg_sys::CurrentResourceOwner = old_owner;
        match result {
            Ok(q) => {
                AnalyzeOutcome::Success(q)
            },
            Err(outcome) => {
                outcome
            }
        }
    }
}


/// PostgreSQL의 reportErrorPosition과 동일하게 동작하는 함수
/// cursor_pos는 1-based byte offset
fn format_error_position(message: &str, query: &str, cursor_pos: i32, encoding: i32) -> String {
    const DISPLAY_SIZE: usize = 60;  // screen width limit, in screen cols
    const MIN_RIGHT_CUT: usize = 10;  // try to keep this far away from EOL

    // Convert loc from 1-based to 0-based
    let loc = (cursor_pos - 1) as usize;
    if cursor_pos < 1 || loc >= query.len() {
        return message.to_string();
    }

    // Need a writable copy of the query
    let mut wquery = query.as_bytes().to_vec();
    let slen = wquery.len() + 1;

    // Arrays to store byte offset and screen column for each logical character
    let mut qidx: Vec<usize> = Vec::with_capacity(slen);
    let mut scridx: Vec<usize> = Vec::with_capacity(slen);

    // Check if multibyte encoding
    let mb_encoding = unsafe { pg_sys::pg_encoding_max_length(encoding) != 1 };

    let mut qoffset: usize = 0;
    let mut scroffset: usize = 0;
    let mut loc_line: i32 = 1;
    let mut ibeg: usize = 0;
    let mut iend: Option<usize> = None;
    let mut cno: usize = 0;

    // Scan through the query
    while qoffset < wquery.len() {
        let ch = wquery[qoffset];

        qidx.push(qoffset);
        scridx.push(scroffset);

        // Replace tabs with spaces
        if ch == b'\t' {
            wquery[qoffset] = b' ';
        }
        // Handle end-of-line
        else if ch == b'\r' || ch == b'\n' {
            if cno < loc {
                if ch == b'\r' || cno == 0 || (cno > 0 && wquery[qidx[cno - 1]] != b'\r') {
                    loc_line += 1;
                }
                ibeg = cno + 1;
            } else {
                iend = Some(cno);
                break;
            }
        }

        // Advance
        if mb_encoding {
            unsafe {
                let w = pg_sys::pg_encoding_dsplen(encoding, &wquery[qoffset] as *const u8 as *const i8);
                let w = if w <= 0 { 1 } else { w as usize };
                scroffset += w;
                
                // pg_mblen 사용 (서버 함수)
                let mblen = pg_sys::pg_mblen(&wquery[qoffset] as *const u8 as *const i8);
                qoffset += mblen as usize;
            }
        } else {
            scroffset += 1;
            qoffset += 1;
        }

        cno += 1;
    }

    // Fix up if we didn't find an end-of-line after loc
    let iend = iend.unwrap_or_else(|| {
        qidx.push(qoffset);
        scridx.push(scroffset);
        cno
    });

    // Print only if loc is within computed query length
    if loc > cno {
        return message.to_string();
    }

    let mut result = message.to_string();

    // Determine if we need to truncate
    let mut ibeg = ibeg;
    let mut iend = iend;
    let mut beg_trunc = false;
    let mut end_trunc = false;

    if scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
        // Try truncating right first
        if scridx[ibeg] + DISPLAY_SIZE >= scridx[loc] + MIN_RIGHT_CUT {
            while iend > ibeg && scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
                iend -= 1;
            }
            end_trunc = true;
        } else {
            // Truncate right if not too close to loc
            while iend > loc && scridx[loc] + MIN_RIGHT_CUT < scridx[iend] {
                iend -= 1;
                end_trunc = true;
            }

            // Truncate left if still too long
            while iend > ibeg && scridx[iend] - scridx[ibeg] > DISPLAY_SIZE {
                ibeg += 1;
                beg_trunc = true;
            }
        }
    }

    // Build the LINE message
    result.push_str(&format!("\nLINE {}: ", loc_line));
    
    // Calculate screen width of prefix for cursor positioning
    let prefix_start = result.len();
    
    if beg_trunc {
        result.push_str("...");
    }

    // Add the query line segment
    let line_start = qidx[ibeg];
    let line_end = if iend < qidx.len() { qidx[iend] } else { wquery.len() };
    if let Ok(line_str) = std::str::from_utf8(&wquery[line_start..line_end]) {
        result.push_str(line_str);
    }

    if end_trunc {
        result.push_str("...");
    }
    result.push('\n');

    // Calculate screen offset for cursor
    let mut cursor_scroffset = 0;
    
    // Add width of "LINE N: " prefix and "..." if truncated
    if mb_encoding {
        unsafe {
            let prefix_bytes = result[prefix_start..].as_bytes();
            let mut i = 0;
            while i < prefix_bytes.len() && result.as_bytes()[prefix_start + i] != b'\n' {
                // pg_mblen 사용
                let mblen = pg_sys::pg_mblen(
                    &prefix_bytes[i] as *const u8 as *const i8
                );
                let w = pg_sys::pg_encoding_dsplen(
                    encoding, 
                    &prefix_bytes[i] as *const u8 as *const i8
                );
                cursor_scroffset += if w <= 0 { 1 } else { w as usize };
                i += mblen as usize;
            }
        }
    } else {
        // Count characters in prefix
        for ch in result[prefix_start..].chars() {
            if ch == '\n' {
                break;
            }
            cursor_scroffset += 1;
        }
    }

    // Add offset within the displayed line segment
    cursor_scroffset += scridx[loc] - scridx[ibeg];

    // Add cursor line
    for _ in 0..cursor_scroffset {
        result.push(' ');
    }
    result.push('^');
    result.push('\n');

    result
}

/// 전체 에러 메시지 + LINE 정보를 결합하는 함수
fn format_complete_error_message(source_sql: &str) -> (Option<PgSqlErrorCode>, String, Option<i32>) {
    unsafe {
        let error_data = pg_sys::CopyErrorData();
        
        let code = Some(PgSqlErrorCode::from((*error_data).sqlerrcode));
        let cursor_pos = if (*error_data).cursorpos > 0 {
            Some((*error_data).cursorpos)
        } else {
            None
        };
        
        let mut full_message = String::new();
        
        // Primary message
        if !(*error_data).message.is_null() {
            let msg = CStr::from_ptr((*error_data).message)
                .to_string_lossy()
                .to_string();
            full_message.push_str(&msg);
        }
        
        // LINE 정보 추가 (cursor_pos가 있으면)
        if let Some(pos) = cursor_pos {
            if pos > 0 && !source_sql.is_empty() {
                let encoding = pg_sys::GetDatabaseEncoding();
                // format_error_position은 메시지를 받아서 LINE 정보를 추가함
                full_message = format_error_position(&full_message, source_sql, pos, encoding);
            }
        }
        
        // Detail 추가
        if !(*error_data).detail.is_null() {
            let detail = CStr::from_ptr((*error_data).detail)
                .to_string_lossy()
                .to_string();
            full_message.push('\n');
            full_message.push_str(&detail);
        }
        
        // Hint 추가
        if !(*error_data).hint.is_null() {
            let hint = CStr::from_ptr((*error_data).hint)
                .to_string_lossy()
                .to_string();
            full_message.push('\n');
            full_message.push_str(&hint);
        }
        
        // Context 추가
        if !(*error_data).context.is_null() {
            let context = CStr::from_ptr((*error_data).context)
                .to_string_lossy()
                .to_string();
            full_message.push('\n');
            full_message.push_str(&context);
        }
        
        pg_sys::FreeErrorData(error_data);
        (code, full_message, cursor_pos)
    }
}

/// Error report에서 cursor position 추출
fn get_error_cursor_pos() -> Option<i32> {
    // PostgreSQL의 ErrorData 구조체에서 cursorpos를 가져옴
    unsafe {
        let error_data = pg_sys::CopyErrorData();
        let cursor_pos = (*error_data).cursorpos;
        pg_sys::FlushErrorState();
        pg_sys::FreeErrorData(error_data);
        
        if cursor_pos > 0 {
            Some(cursor_pos)
        } else {
            None
        }
    }
}


/// 오퍼레이터 타입 에러인지 확인
fn is_operator_type_error(message: &str) -> bool {
    let operator_patterns = [
        r"operator does not exist:",
        r"could not identify an equality operator for type",
        r"operator is not unique:",
    ];
    
    operator_patterns.iter().any(|pattern| {
        Regex::new(pattern).unwrap().is_match(message)
    })
}

fn rawstmt_from_sql(sql: &str) -> Result<*mut pg_sys::RawStmt, String> {
    let csql = CString::new(sql).map_err(|_| "CString::new failed (interior NUL)".to_string())?;
    unsafe {
        let rawtree = pg_sys::raw_parser(csql.as_ptr(), 0);
        if rawtree.is_null() {
            return Err("raw_parser returned NULL".into());
        }
        let raw: *mut pg_sys::RawStmt = memcx::current_context(|mcx| {
            let stmts = List::<*mut c_void>::downcast_ptr_in_memcx(rawtree, mcx).unwrap();
            if stmts.len() == 0 {
                return None;
            }
            Some(*stmts.get(0).unwrap() as *mut pg_sys::RawStmt)
        })
        .ok_or_else(|| "raw_parser returned empty list".to_string())?;
        Ok(raw)
    }
}

/// PQ 후보 삽입: RawStmt* 를 node_to_string 으로 직렬화해 중복 제거
/// hop_count가 max_hops를 넘으면 추가하지 않음
fn push_candidate(
    pq: &mut BinaryHeap<Reverse<(i32, u64, *mut pg_sys::RawStmt, i32)>>,
    visited: &mut HashSet<String>,
    prio: i32,
    hop_count: i32,
    seq: &mut u64,
    raw: *mut pg_sys::RawStmt,
) {
    // hop 수가 제한을 넘으면 추가하지 않음
    if hop_count > MAX_REFINEMENT_HOP.get() {
        return;
    }
    
    let key = unsafe {
        node_to_string(raw as *mut pg_sys::Node).unwrap_or("<ser-failed>")
    };
    if visited.insert(key.to_string()) {
        pq.push(Reverse((prio, *seq, raw, hop_count)));
        *seq += 1;
    }
}


/// 저장된 에러를 그대로 ereport!
fn raise_saved_error(_code: Option<PgSqlErrorCode>, message: String) -> ! {
    pgrx::error!("{}", message);
}

/// 에러 메시지에서 relation "X" 추출
fn extract_missing_relation(errmsg: &str) -> Option<String> {
    // 기존 패턴: relation "hello" does not exist
    let re1 = Regex::new(r#"(?i)relation\s+"([^"]+)"\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re1.captures(errmsg) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    // 새 패턴: missing FROM-clause entry for table "p"
    let re2 = Regex::new(r#"(?i)missing\s+FROM-clause\s+entry\s+for\s+table\s+"([^"]+)""#).unwrap();
    if let Some(cap) = re2.captures(errmsg) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    None
}

fn extract_missing_column(errmsg: &str) -> Option<(Option<String>, String)> {
    // 여러 패턴 시도
    // 1. column "X" does not exist
    // 2. column "X" of relation "Y" does not exist  
    // 3. column Y.X does not exist (qualified name)
    
    // Pattern 1: column "name" of relation "table" does not exist
    let re1 = Regex::new(r#"(?i)column\s+"([^"]+)"\s+of\s+relation\s+"([^"]+)"\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re1.captures(errmsg) {
        let col = cap.get(1).map(|m| m.as_str().to_string())?;
        let rel = cap.get(2).map(|m| m.as_str().to_string());
        return Some((rel, col));
    }
    
    // Pattern 2: column table.column does not exist
    let re2 = Regex::new(r#"(?i)column\s+([^.\s]+)\.([^.\s]+)\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re2.captures(errmsg) {
        let rel = cap.get(1).map(|m| m.as_str().to_string());
        let col = cap.get(2).map(|m| m.as_str().to_string())?;
        return Some((rel, col));
    }
    
    // Pattern 3: column "name" does not exist (no relation specified)
    let re3 = Regex::new(r#"(?i)column\s+"([^"]+)"\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re3.captures(errmsg) {
        let col = cap.get(1).map(|m| m.as_str().to_string())?;
        return Some((None, col));
    }
    
    // Pattern 4: column table.column name with spaces does not exist
    // 예: "column patient.County Name does not exist"
    // 이 패턴에서는 첫 번째 단어가 table.first_word이고, 나머지가 column name의 일부
    let re4 = Regex::new(r#"(?i)column\s+([^.\s]+)\.(.+?)\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re4.captures(errmsg) {
        let rel = cap.get(1).map(|m| m.as_str().to_string());
        let col = cap.get(2).map(|m| m.as_str().trim().to_string())?;
        
        // column name이 비어있지 않은 경우만 반환
        if !col.is_empty() {
            return Some((rel, col));
        }
    }
    
    // Pattern 5: column unqualified name with spaces does not exist
    // 예: "column County Name does not exist"
    // 테이블명 없이 공백을 포함한 컬럼명만 있는 경우
    let re5 = Regex::new(r#"(?i)column\s+([^"]+?)\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re5.captures(errmsg) {
        let col = cap.get(1).map(|m| m.as_str().trim().to_string())?;
        
        // 이미 위의 패턴들로 처리되지 않은 경우만 (즉, '.'이 없는 경우)
        if !col.contains('.') && !col.is_empty() {
            return Some((None, col));
        }
    }
    
    None
}

/// 에러 메시지에서 함수 정보 추출
/// 예: "function pg_catalog.extract(unknown, bigint) does not exist"
fn extract_missing_function(errmsg: &str) -> Option<(String, Vec<String>)> {
    // Pattern 1: function schema.function_name(arg1, arg2, ...) does not exist
    let re1 = Regex::new(r#"(?i)function\s+(?:[^.]+\.)?([^(]+)\(([^)]*)\)\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re1.captures(errmsg) {
        let function_name = cap.get(1).map(|m| m.as_str().trim().to_string())?;
        let args_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        
        let arg_types = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            args_str.split(',')
                .map(|arg| arg.trim().to_string())
                .filter(|arg| !arg.is_empty())
                .collect()
        };
        
        return Some((function_name, arg_types));
    }
    
    // Pattern 2: function function_name(arg1, arg2, ...) does not exist (no schema)
    let re2 = Regex::new(r#"(?i)function\s+([^(]+)\(([^)]*)\)\s+does\s+not\s+exist"#).unwrap();
    if let Some(cap) = re2.captures(errmsg) {
        let function_name = cap.get(1).map(|m| m.as_str().trim().to_string())?;
        let args_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        
        let arg_types = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            args_str.split(',')
                .map(|arg| arg.trim().to_string())
                .filter(|arg| !arg.is_empty())
                .collect()
        };
        
        return Some((function_name, arg_types));
    }
    
    None
}

/// 에러 메시지에서 ambiguous column name 추출
fn extract_ambiguous_column(errmsg: &str) -> Option<String> {
    // Pattern 1: column reference "column_name" is ambiguous
    let re1 = Regex::new(r#"(?i)column\s+reference\s+"([^"]+)"\s+is\s+ambiguous"#).unwrap();
    if let Some(cap) = re1.captures(errmsg) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    // Pattern 2: column "column_name" is ambiguous
    let re2 = Regex::new(r#"(?i)column\s+"([^"]+)"\s+is\s+ambiguous"#).unwrap();
    if let Some(cap) = re2.captures(errmsg) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    // Pattern 3: ambiguous column name: column_name
    let re3 = Regex::new(r#"(?i)ambiguous\s+column\s+name:\s*([^\s]+)"#).unwrap();
    if let Some(cap) = re3.captures(errmsg) {
        return cap.get(1).map(|m| m.as_str().to_string());
    }
    
    None
}

/// 에러 위치에서 함수 호출들 찾기
unsafe fn find_function_calls_at_position(
    raw: *mut pg_sys::RawStmt,
    error_pos: i32
) -> Vec<*mut pg_sys::Node> {
    let mut function_calls = Vec::new();
    let ctx = (&mut function_calls, error_pos);
    let ctx_ptr = &ctx as *const _ as *mut c_void;
    
    unsafe {
        safe_raw_expression_tree_walker(
            (*raw).stmt,
            Some(find_function_call_at_position_walker),
            ctx_ptr
        );
    }
    
    function_calls
}

unsafe extern "C" fn find_function_call_at_position_walker(
    node: *mut pg_sys::Node,
    ctx: *mut c_void
) -> bool {
    if node.is_null() {
        return false;
    }
    
    unsafe {
        let (function_calls, target_pos) = &mut *(ctx as *mut (&mut Vec<*mut pg_sys::Node>, i32));
        
        // FuncCall 노드에서 location 확인
        if (*node).type_ == pg_sys::NodeTag::T_FuncCall {
            let func_call = node as *mut pg_sys::FuncCall;
            let location = (*func_call).location;
            
            // location이 에러 위치 근처에 있으면 추가
            if location >= 0 && location + 1 == *target_pos {
                function_calls.push(node);
                return false; // 찾았으므로 중단
            }
        }
        
        safe_raw_expression_tree_walker(node, Some(find_function_call_at_position_walker), ctx)
    }
}

/// 함수 호출에서 argument operand들 추출
unsafe fn extract_argument_operands_from_function_call(
    func_call_node: *mut pg_sys::Node
) -> Option<Vec<Option<ColumnOperand>>> {
    if func_call_node.is_null() {
        return None;
    }
    
    unsafe {
        if (*func_call_node).type_ == pg_sys::NodeTag::T_FuncCall {
            let func_call = func_call_node as *mut pg_sys::FuncCall;
            
            if !(*func_call).args.is_null() {
                return memcx::current_context(|mcx| {
                    if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*func_call).args, mcx) {
                        let mut argument_operands = Vec::new();
                        
                        for i in 0..args.len() {
                            if let Some(arg_ptr) = args.get(i) {
                                let arg_node = *arg_ptr as *mut pg_sys::Node;
                                let column_operand = extract_column_ref_operand(arg_node);
                                argument_operands.push(column_operand);
                            }
                        }
                        
                        return Some(argument_operands);
                    }
                    None
                });
            }
        }
    }
    
    None
}


/// 에러 위치에서 표현식들 찾기
unsafe fn find_expressions_at_position(
    raw: *mut pg_sys::RawStmt,
    error_pos: i32
) -> Vec<*mut pg_sys::Node> {
    let mut expressions = Vec::new();
    let ctx = (&mut expressions, error_pos);
    let ctx_ptr = &ctx as *const _ as *mut c_void;
    
    unsafe {
        safe_raw_expression_tree_walker(
            (*raw).stmt,
            Some(find_expr_at_position_walker),
            ctx_ptr
        );
    }
    
    expressions
}

unsafe extern "C" fn find_expr_at_position_walker(
    node: *mut pg_sys::Node,
    ctx: *mut c_void
) -> bool {
    if node.is_null() {
        return false;
    }
    
    unsafe {
        let (expressions, target_pos) = &mut *(ctx as *mut (&mut Vec<*mut pg_sys::Node>, i32));
        
        // A_Expr 노드에서 location 확인
        if (*node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = node as *mut pg_sys::A_Expr;
            let location = (*a_expr).location;
            
            // location이 에러 위치 근처에 있으면 추가 (±5 char 범위)
            if location >= 0 && location + 1 == *target_pos {
                expressions.push(node);
                return false; // 찾았으므로 중단
            }
        }
        
        safe_raw_expression_tree_walker(node, Some(find_expr_at_position_walker), ctx)
    }
}


/// 표현식에서 left/right ColumnRef 오퍼랜드들 추출
unsafe fn extract_column_operands_from_expr(
    expr_node: *mut pg_sys::Node
) -> Option<(Option<ColumnOperand>, Option<ColumnOperand>)> {
    if expr_node.is_null() {
        return None;
    }
    
    unsafe {
        if (*expr_node).type_ == pg_sys::NodeTag::T_A_Expr {
            let a_expr = expr_node as *mut pg_sys::A_Expr;
            
            // 왼쪽 오퍼랜드 추출
            let left_operand = if !(*a_expr).lexpr.is_null() {
                extract_column_ref_operand((*a_expr).lexpr)
            } else {
                None
            };
            
            // 오른쪽 오퍼랜드 추출
            let right_operand = if !(*a_expr).rexpr.is_null() {
                extract_column_ref_operand((*a_expr).rexpr)
            } else {
                None
            };
            
            if left_operand.is_some() || right_operand.is_some() {
                return Some((left_operand, right_operand));
            } else {
                return Some((None, None));
            }
        }
    }
    
    None
}

/// 단일 노드에서 ColumnRef 추출
unsafe fn extract_column_ref_operand(node: *mut pg_sys::Node) -> Option<ColumnOperand> {
    if node.is_null() {
        return None;
    }
    
    unsafe {
        if (*node).type_ == pg_sys::NodeTag::T_ColumnRef {
            let col_ref = node as *mut pg_sys::ColumnRef;
            
            if !(*col_ref).fields.is_null() {
                return memcx::current_context(|mcx| {
                    if let Some(fields) = List::<*mut c_void>::downcast_ptr_in_memcx((*col_ref).fields, mcx) {
                        let field_count = fields.len();
                        
                        if field_count == 1 {
                            // unqualified reference
                            if let Some(field_ptr) = fields.get(0) {
                                let field = *field_ptr as *mut pg_sys::Node;
                                if (*field).type_ == pg_sys::NodeTag::T_String {
                                    let str_node = field as *mut pg_sys::String;
                                    let column_name = CStr::from_ptr((*str_node).sval)
                                        .to_string_lossy().into_owned();
                                    return Some(ColumnOperand {
                                        table_name: None,
                                        column_name,
                                    });
                                }
                            }
                        } else if field_count >= 2 {
                            // qualified reference
                            if let (Some(first_ptr), Some(last_ptr)) = (fields.get(0), fields.get(field_count - 1)) {
                                let first_field = *first_ptr as *mut pg_sys::Node;
                                let last_field = *last_ptr as *mut pg_sys::Node;
                                
                                if (*first_field).type_ == pg_sys::NodeTag::T_String &&
                                   (*last_field).type_ == pg_sys::NodeTag::T_String {
                                    let first_str = first_field as *mut pg_sys::String;
                                    let last_str = last_field as *mut pg_sys::String;
                                    
                                    let table_name = CStr::from_ptr((*first_str).sval)
                                        .to_string_lossy().into_owned();
                                    let column_name = CStr::from_ptr((*last_str).sval)
                                        .to_string_lossy().into_owned();
                                        
                                    return Some(ColumnOperand {
                                        table_name: Some(table_name),
                                        column_name,
                                    });
                                }
                            }
                        }
                    }
                    None
                });
            }
        }
    }
    
    None
}


/// pstate 모드 on/off
#[inline]
unsafe fn set_subselect_mode(pstate: *mut pg_sys::ParseState, enable: bool) {
    unsafe {
        if enable {
            (*pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_FROM_SUBSELECT;
            (*pstate).p_lateral_active = true;
        } else {
            (*pstate).p_lateral_active = false;
            (*pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_NONE;
        }
    }
}