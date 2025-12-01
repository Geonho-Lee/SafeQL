use pgrx::pg_sys;
use pgrx::{IntoDatum, PgOid, Spi};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use regex::Regex;

use super::cache::{
    generate_cache_key,
    get_cached_result,
    store_cached_result
};
use crate::gucs::parser::{ENABLE_TYPE_BASED_REFINEMENT, TOP_K_EXPANSION};

/// 문자열을 언더스코어 기준으로 쪼개서 검색 가능한 형태로 변환
fn prepare_search_terms(input: &str) -> Vec<String> {
    input.split('_')
         .filter(|s| !s.is_empty())
         .map(|s| s.to_string())
         .collect()
}

fn escape_sql_literal(s: &str) -> String {
    s.replace("'", "''")
}

fn create_combined_vector_query(terms: &[String]) -> String {
    if terms.len() == 1 {
        let term_escaped = escape_sql_literal(&terms[0]);
        format!("SELECT _vectors_text2vec(quote_literal('{}')) AS v", term_escaped)
    } else {
        let combined_term = terms.join(" ");
        let term_escaped = escape_sql_literal(&combined_term);
        format!("SELECT _vectors_text2vec(quote_literal('{}')) AS v", term_escaped)
    }
}


pub fn list_tables_by_similarity(missing_rel: &str) -> Vec<(String, f32)> {
    // 검색어 준비
    let search_terms = prepare_search_terms(missing_rel);
    let search_key = search_terms.join("_"); // 캐시 키용
    
    // 캐시 키 생성 (TOP_K_EXPANSION 값도 포함)
    let top_k = TOP_K_EXPANSION.get();
    let cache_key = generate_cache_key("table_similarity", &[&search_key, &top_k.to_string()]);
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for table similarity query: {} (terms: {:?}, top_k: {})", missing_rel, search_terms, top_k);
            return result;
        }
    }
    
    // 검색 벡터 쿼리 생성
    let vector_query = create_combined_vector_query(&search_terms);
    
    // 캐시 미스 - 실제 쿼리 실행
    let sql = format!(r#"
        WITH q AS (
            {}
        )
        SELECT 
            schemaname || '.' || tablename AS fqname,
            (embedding <=> q.v) AS distance
        FROM pg_vector_tables, q
        WHERE schemaname = ANY (current_schemas(false))
        ORDER BY distance
        LIMIT $1
    "#, vector_query);
    
    let result = Spi::connect(|client| -> Result<Vec<(String, f32)>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::INT4OID), top_k.into_datum())
        ];
        
        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();
        
        for row in rows {
            if let (Ok(Some(fqname)), Ok(Some(distance))) = 
                (row["fqname"].value::<String>(), row["distance"].value::<f32>()) {
                result.push((fqname, distance));
            }
        }
        
        Ok(result)
    }).unwrap_or_default();
    
    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "table_similarity", &result_json);
    }
    
    // pgrx::notice!("SafeQL: Found {} table suggestions for '{}' (top_k: {})", result.len(), missing_rel, top_k);
    result
}

/// Column 후보들을 가져오는 함수 (캐시 적용)
pub fn list_columns_by_similarity(
    table_name: Option<&str>,
    missing_col: &str,
    exclude_same_name: bool,
) -> Vec<(String, String, f32)> {
    // 검색어 준비
    let search_terms = prepare_search_terms(missing_col);
    let search_key = search_terms.join("_"); // 캐시 키용
    
    // 캐시 키 생성 (TOP_K_EXPANSION 값도 포함)
    let top_k = TOP_K_EXPANSION.get();
    let table_key = table_name.unwrap_or("NULL");
    let cache_key = generate_cache_key(
        "column_similarity", 
        &[table_key, &search_key, &exclude_same_name.to_string(), &top_k.to_string()]
    );
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for column similarity query: table={:?}, col={} (terms: {:?}, top_k: {})", 
            //              table_name, missing_col, search_terms, top_k);
            return result;
        }
    }

    // 검색 벡터 쿼리 생성
    let vector_query = create_combined_vector_query(&search_terms);

    // 캐시 미스 - 실제 쿼리 실행
    let sql = if table_name.is_some() {
        format!(r#"
        WITH q AS (
            {}
        ),
        check_table AS (
            SELECT EXISTS (
                SELECT 1
                FROM pg_vector_fields
                WHERE schemaname = ANY (current_schemas(false))
                  AND tablename = $1
            ) AS has_table
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q, check_table c
        WHERE f.schemaname = ANY (current_schemas(false))
          AND ( $3::bool = false OR f.fieldname <> $2 )
          AND (
                (c.has_table AND f.tablename = $1)
                OR (NOT c.has_table)
          )
        ORDER BY distance
        LIMIT $4
        "#, vector_query)
    } else {
        format!(r#"
        WITH q AS (
            {}
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q
        WHERE f.schemaname = ANY (current_schemas(false))
          AND ( $2::bool = false OR f.fieldname <> $1 )
        ORDER BY distance
        LIMIT $3
        "#, vector_query)
    };

    let result = Spi::connect(|client| -> Result<Vec<(String, String, f32)>, pgrx::spi::Error> {
        let args = if let Some(t) = table_name {
            vec![
                (PgOid::from(pg_sys::TEXTOID), t.into_datum()),
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
                (PgOid::from(pg_sys::BOOLOID), exclude_same_name.into_datum()),
                (PgOid::from(pg_sys::INT4OID), top_k.into_datum()),
            ]
        } else {
            vec![
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
                (PgOid::from(pg_sys::BOOLOID), exclude_same_name.into_datum()),
                (PgOid::from(pg_sys::INT4OID), top_k.into_datum()),
            ]
        };

        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();

        for row in rows {
            if let (Ok(Some(col_name)), Ok(Some(tbl_name)), Ok(Some(distance))) = (
                row["fieldname"].value::<String>(),
                row["tablename"].value::<String>(),
                row["distance"].value::<f32>()
            ) {
                result.push((col_name, tbl_name, distance));
            }
        }

        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "column_similarity", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} column suggestions for '{}' (top_k: {})", result.len(), missing_col, top_k);
    result
}


/// FROM절의 모든 테이블에서 해당 컬럼을 가진 테이블들 찾기 (캐시 적용)
/// Column Ambiguity Refinement 전용 함수 - 효율적으로 한 번에 조회
pub fn find_tables_with_exact_column(table_names: &[String], column_name: &str) -> Vec<String> {
    if table_names.is_empty() {
        return Vec::new();
    }
    
    // 캐시 키 생성 (테이블 목록을 정렬해서 일관성 확보)
    let mut sorted_tables = table_names.to_vec();
    sorted_tables.sort();
    let tables_str = sorted_tables.join(",");
    let cache_key = generate_cache_key("tables_with_exact_column", &[&tables_str, column_name]);
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<String>>(&cached_result) {
            // pgrx::notice!("Cache hit for tables with exact column: tables={:?}, col={}, matches={:?}", 
            //              table_names, column_name, result);
            return result;
        }
    }
    
    // 캐시 미스 - 실제 쿼리 실행
    let sql = r#"
        SELECT DISTINCT f.tablename
        FROM pg_vector_fields f
        WHERE f.schemaname = ANY (current_schemas(false))
          AND f.tablename = ANY($1)
          AND LOWER(f.fieldname) = LOWER($2)
        ORDER BY f.tablename
    "#;

    let result = Spi::connect(|client| -> Result<Vec<String>, pgrx::spi::Error> {
        let table_array: Vec<Option<String>> = table_names.iter().map(|t| Some(t.clone())).collect();
        let args = vec![
            (PgOid::from(pg_sys::TEXTARRAYOID), table_array.into_datum()),
            (PgOid::from(pg_sys::TEXTOID), column_name.into_datum()),
        ];

        let rows = client.select(sql, None, Some(args))?;
        let mut matching_tables = Vec::new();

        for row in rows {
            if let Ok(Some(table_name)) = row["tablename"].value::<String>() {
                matching_tables.push(table_name);
            }
        }

        Ok(matching_tables)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "tables_with_exact_column", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} tables with exact column '{}': {:?}", 
    //              result.len(), column_name, result);
    
    result
}


/// FROM절의 테이블들과 PK-FK 관계로 연결 가능한 모든 테이블 찾기 (컬럼 검색 없이)
pub fn find_all_joinable_tables(
    existing_tables: &[String],
) -> Vec<(String, f32, Vec<JoinCondition>)> {
    if existing_tables.is_empty() {
        return Vec::new();
    }

    // 캐시 키 생성 (테이블 목록을 정렬해서 일관성 확보)
    let mut sorted_tables = existing_tables.to_vec();
    sorted_tables.sort();
    let tables_str = sorted_tables.join(",");
    let cache_key = generate_cache_key("all_joinable_tables", &[&tables_str]);
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, f32, Vec<JoinCondition>)>>(&cached_result) {
            return result;
        }
    }

    // 캐시 미스 - 실제 쿼리 실행
    let sql = r#"
        WITH existing_tables AS (
            SELECT unnest($1::text[]) AS table_name
        ),
        pk_fk_joinables AS (
            SELECT DISTINCT
                t2.relname AS join_table,
                t1.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column,
                1 AS direction
            FROM pg_constraint c
            JOIN pg_class t1 ON c.conrelid = t1.oid
            JOIN pg_class t2 ON c.confrelid = t2.oid
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum = ANY(c.conkey)
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum = ANY(c.confkey)
            JOIN existing_tables et ON LOWER(t1.relname) = LOWER(et.table_name)
            WHERE c.contype = 'f'
            AND n1.nspname = ANY(current_schemas(false))
            AND n2.nspname = ANY(current_schemas(false))
            AND t1.relkind = 'r'
            AND t2.relkind = 'r'
            AND LOWER(t2.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)

            UNION

            SELECT DISTINCT
                t1.relname AS join_table,
                t2.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column,
                2 AS direction
            FROM pg_constraint c
            JOIN pg_class t1 ON c.conrelid = t1.oid
            JOIN pg_class t2 ON c.confrelid = t2.oid
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum = ANY(c.conkey)
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum = ANY(c.confkey)
            JOIN existing_tables et ON LOWER(t2.relname) = LOWER(et.table_name)
            WHERE c.contype = 'f'
            AND n1.nspname = ANY(current_schemas(false))
            AND n2.nspname = ANY(current_schemas(false))
            AND t1.relkind = 'r'
            AND t2.relkind = 'r'
            AND LOWER(t1.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)
        ),
        pk_columns AS (
            SELECT DISTINCT
                c.conrelid AS table_oid,
                a.attname AS pk_column
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p' -- primary key
        ),
        natural_joinables AS (
            SELECT DISTINCT
                t2.relname AS join_table,
                t1.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column,
                3 AS direction
            FROM pg_class t1
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum > 0 AND NOT a1.attisdropped
            JOIN existing_tables et ON LOWER(t1.relname) = LOWER(et.table_name)
            JOIN pg_class t2 ON t2.oid <> t1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum > 0 AND NOT a2.attisdropped
            LEFT JOIN pk_columns pk1 ON pk1.table_oid = t1.oid AND LOWER(pk1.pk_column) = LOWER(a1.attname)
            LEFT JOIN pk_columns pk2 ON pk2.table_oid = t2.oid AND LOWER(pk2.pk_column) = LOWER(a2.attname)
            WHERE n1.nspname = ANY(current_schemas(false))
            AND n2.nspname = ANY(current_schemas(false))
            AND t1.relkind = 'r'
            AND t2.relkind = 'r'
            AND LOWER(a1.attname) = LOWER(a2.attname)
            AND pk1.pk_column IS NULL
            AND pk2.pk_column IS NULL
            AND LOWER(t2.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)
        )
        SELECT 
            jt.join_table::text,
            jt.existing_table::text,
            jt.fk_column::text,
            jt.pk_column::text,
            1.0::float4 AS distance
        FROM (
            SELECT * FROM pk_fk_joinables
            UNION ALL
            SELECT * FROM natural_joinables
        ) jt
        ORDER BY jt.join_table, jt.existing_table;
        "#
        .to_string();

    let result = Spi::connect(|client| -> Result<Vec<(String, f32, Vec<JoinCondition>)>, pgrx::spi::Error> {
        let table_array: Vec<Option<String>> = existing_tables.iter().map(|t| Some(t.clone())).collect();
        let args = vec![
            (PgOid::from(pg_sys::TEXTARRAYOID), table_array.into_datum()),
        ];
        
        let rows = client.select(&sql, None, Some(args))?;
        let mut result_map: HashMap<String, (f32, Vec<JoinCondition>)> = HashMap::new();
        
        for row in rows {
            if let (
                Ok(Some(join_table)), 
                Ok(Some(existing_table)), 
                Ok(Some(fk_column)), 
                Ok(Some(pk_column)),
                Ok(Some(distance))
            ) = (
                row["join_table"].value::<String>(),
                row["existing_table"].value::<String>(), 
                row["fk_column"].value::<String>(),
                row["pk_column"].value::<String>(),
                row["distance"].value::<f32>()
            ) {
                let join_condition = JoinCondition {
                    left_table: existing_table,
                    left_column: pk_column,
                    right_table: join_table.clone(),
                    right_column: fk_column,
                };

                result_map.entry(join_table)
                    .and_modify(|(_, conditions)| conditions.push(join_condition.clone()))
                    .or_insert((distance, vec![join_condition]));
            }
        }
        
        let mut result: Vec<(String, f32, Vec<JoinCondition>)> = result_map
            .into_iter()
            .map(|(table, (distance, conditions))| (table, distance, conditions))
            .collect();
        
        result.sort_by(|a, b| a.0.cmp(&b.0));  // 테이블명으로 정렬
        
        // // TOP_K_EXPANSION 적용
        // let top_k = TOP_K_EXPANSION.get() as usize;
        // if result.len() > top_k {
        //     result.truncate(top_k);
        // }
        
        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "all_joinable_tables", &result_json);
    }

    result
}

/// JOIN 가능한 테이블들을 PK-FK 관계로 찾는 함수 (캐시 적용)
pub fn find_joinable_tables_for_column(
    existing_tables: &[String],
    missing_col: &str,
) -> Vec<(String, f32, Vec<JoinCondition>)> {
    if existing_tables.is_empty() {
        return Vec::new();
    }

    // 검색어 준비
    let search_terms = prepare_search_terms(missing_col);
    let search_key = search_terms.join("_"); // 캐시 키용

    // 캐시 키 생성 (테이블 목록을 정렬해서 일관성 확보)
    let mut sorted_tables = existing_tables.to_vec();
    sorted_tables.sort();
    let tables_str = sorted_tables.join(",");
    let cache_key = generate_cache_key("joinable_tables", &[&tables_str, &search_key]);
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, f32, Vec<JoinCondition>)>>(&cached_result) {
            // pgrx::notice!("Cache hit for joinable tables query: tables={:?}, col={} (terms: {:?})", 
            //              existing_tables, missing_col, search_terms);
            return result;
        }
    }

    // 검색 벡터 쿼리 생성
    let vector_query = create_combined_vector_query(&search_terms);

    // 캐시 미스 - 실제 쿼리 실행
    let sql = format!(r#"
        WITH q AS (
            {}
        ),
        existing_tables AS (
            SELECT unnest($2::text[]) AS table_name
        ),
        pk_fk_joinables AS (
            SELECT DISTINCT
                t2.relname AS join_table,
                t1.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column
            FROM pg_constraint c
            JOIN pg_class t1 ON c.conrelid = t1.oid
            JOIN pg_class t2 ON c.confrelid = t2.oid
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum = ANY(c.conkey)
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum = ANY(c.confkey)
            JOIN existing_tables et ON LOWER(t1.relname) = LOWER(et.table_name)
            WHERE c.contype = 'f'
            AND n1.nspname = ANY(current_schemas(false))
            AND LOWER(t2.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)

            UNION

            SELECT DISTINCT
                t1.relname AS join_table,
                t2.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column
            FROM pg_constraint c
            JOIN pg_class t1 ON c.conrelid = t1.oid
            JOIN pg_class t2 ON c.confrelid = t2.oid
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum = ANY(c.conkey)
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum = ANY(c.confkey)
            JOIN existing_tables et ON LOWER(t2.relname) = LOWER(et.table_name)
            WHERE c.contype = 'f'
            AND n1.nspname = ANY(current_schemas(false))
            AND LOWER(t1.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)
        ),
        natural_joinables AS (
            SELECT DISTINCT
                t2.relname AS join_table,
                t1.relname AS existing_table,
                a1.attname AS fk_column,
                a2.attname AS pk_column
            FROM pg_class t1
            JOIN pg_namespace n1 ON t1.relnamespace = n1.oid
            JOIN pg_attribute a1 ON a1.attrelid = t1.oid AND a1.attnum > 0 AND NOT a1.attisdropped
            JOIN existing_tables et ON LOWER(t1.relname) = LOWER(et.table_name)
            JOIN pg_class t2 ON t2.oid <> t1.oid
            JOIN pg_namespace n2 ON t2.relnamespace = n2.oid
            JOIN pg_attribute a2 ON a2.attrelid = t2.oid AND a2.attnum > 0 AND NOT a2.attisdropped
            WHERE n1.nspname = ANY(current_schemas(false))
            AND n2.nspname = ANY(current_schemas(false))
            AND LOWER(a1.attname) = LOWER(a2.attname)
            AND LOWER(t2.relname) NOT IN (SELECT LOWER(table_name) FROM existing_tables)
        ),
        all_joinables AS (
            SELECT * FROM pk_fk_joinables
            UNION ALL
            SELECT * FROM natural_joinables
        )
        SELECT 
            jt.join_table::text,
            jt.existing_table::text,
            jt.fk_column::text,
            jt.pk_column::text,
            (f.embedding <=> q.v) AS column_distance
        FROM all_joinables jt
        JOIN pg_vector_fields f ON f.tablename = jt.join_table
            AND f.schemaname = ANY(current_schemas(false))
            AND f.fieldname = $1
        CROSS JOIN q
        ORDER BY column_distance, jt.join_table;
    "#, vector_query);

    let result = Spi::connect(|client| -> Result<Vec<(String, f32, Vec<JoinCondition>)>, pgrx::spi::Error> {
        let table_array: Vec<Option<String>> = existing_tables.iter().map(|t| Some(t.clone())).collect();
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
            (PgOid::from(pg_sys::TEXTARRAYOID), table_array.into_datum()),
        ];
        
        let rows = client.select(&sql, None, Some(args))?;
        let mut result_map: HashMap<String, (f32, Vec<JoinCondition>)> = HashMap::new();
        
        for row in rows {
            if let (
                Ok(Some(join_table)), 
                Ok(Some(existing_table)), 
                Ok(Some(fk_column)), 
                Ok(Some(pk_column)),
                Ok(Some(distance))
            ) = (
                row["join_table"].value::<String>(),
                row["existing_table"].value::<String>(), 
                row["fk_column"].value::<String>(),
                row["pk_column"].value::<String>(),
                row["column_distance"].value::<f32>()
            ) {
                let join_condition = JoinCondition {
                    left_table: existing_table,
                    left_column: pk_column,
                    right_table: join_table.clone(),
                    right_column: fk_column,
                };

                result_map.entry(join_table)
                    .and_modify(|(_, conditions)| conditions.push(join_condition.clone()))
                    .or_insert((distance, vec![join_condition]));
            }
        }
        
        let mut result: Vec<(String, f32, Vec<JoinCondition>)> = result_map
            .into_iter()
            .map(|(table, (distance, conditions))| (table, distance, conditions))
            .collect();
        
        result.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // TOP_K_EXPANSION 적용
        let top_k = TOP_K_EXPANSION.get() as usize;
        if result.len() > top_k {
            result.truncate(top_k);
        }
        
        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "joinable_tables", &result_json);
    }

    result
}

/// 오퍼레이터 호환 가능한 컬럼들을 찾는 함수 (캐시 적용)
pub fn find_compatible_columns_for_operator(
    table_name: Option<&str>,
    missing_col: &str,
    error_message: &str,
    operand_position: OperandPosition,
) -> Vec<(String, String, f32)> {
    // 검색어 준비
    let search_terms = prepare_search_terms(missing_col);
    let search_key = search_terms.join("_");
    
    // 에러 메시지에서 오퍼레이터 타입 정보 추출
    let operator_info = extract_operator_info_from_error(error_message);
    
    // 캐시 키 생성
    let table_key = table_name.unwrap_or("NULL");
    let position_key = match operand_position {
        OperandPosition::Left => "LEFT",
        OperandPosition::Right => "RIGHT",
    };
    let operator_key = format!("{}_{}_{}_{}_{}_{}", 
        operator_info.operator_name,
        operator_info.left_type.as_deref().unwrap_or("ANY"),
        operator_info.right_type.as_deref().unwrap_or("ANY"),
        position_key,
        ENABLE_TYPE_BASED_REFINEMENT.get(),
        missing_col
    );
    let cache_key = generate_cache_key(
        "operator_compatible_columns", 
        &[table_key, &search_key, &operator_key]
    );
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for operator compatible columns query: table={:?}, col={}, position={:?} (terms: {:?})", 
            //              table_name, missing_col, operand_position, search_terms);
            return result;
        }
    }

    // 검색 벡터 쿼리 생성
    let vector_query = create_combined_vector_query(&search_terms);

    // 타입 기반 최적화가 활성화되어 있으면 호환 가능한 타입만 검색
    let type_filter = if ENABLE_TYPE_BASED_REFINEMENT.get() {
        if let Some(required_types) = get_compatible_types_for_operator(&operator_info, &operand_position) {
            format!("AND f.fieldtype = ANY(ARRAY[{}])", 
                required_types.iter()
                    .map(|t| format!("'{}'", t))
                    .collect::<Vec<_>>()
                    .join(","))
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // TOP_K_EXPANSION을 적용한 LIMIT 값 계산
    let limit_value = TOP_K_EXPANSION.get();

    // 캐시 미스 - 실제 쿼리 실행
    let sql = if table_name.is_some() {
        format!(r#"
        WITH q AS (
            {}
        ),
        check_table AS (
            SELECT EXISTS (
                SELECT 1
                FROM pg_vector_fields
                WHERE schemaname = ANY (current_schemas(false))
                  AND tablename = $1
            ) AS has_table
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q, check_table c
        WHERE f.schemaname = ANY (current_schemas(false))
          AND f.fieldname <> $2
          AND (
                (c.has_table AND f.tablename = $1)
                OR (NOT c.has_table)
          )
          {}
        ORDER BY distance
        LIMIT {}
        "#, vector_query, type_filter, limit_value)
    } else {
        format!(r#"
        WITH q AS (
            {}
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q
        WHERE f.schemaname = ANY (current_schemas(false))
          AND f.fieldname <> $1
          {}
        ORDER BY distance
        LIMIT {}
        "#, vector_query, type_filter, limit_value)
    };

    let result = Spi::connect(|client| -> Result<Vec<(String, String, f32)>, pgrx::spi::Error> {
        let args = if let Some(t) = table_name {
            vec![
                (PgOid::from(pg_sys::TEXTOID), t.into_datum()),
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
            ]
        } else {
            vec![
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
            ]
        };

        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();

        for row in rows {
            if let (Ok(Some(col_name)), Ok(Some(tbl_name)), Ok(Some(distance))) = (
                row["fieldname"].value::<String>(),
                row["tablename"].value::<String>(),
                row["distance"].value::<f32>()
            ) {
                result.push((col_name, tbl_name, distance));
            }
        }

        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "operator_compatible_columns", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} compatible columns for operator {} (position: {:?}, original_col: {})", 
    //              result.len(), operator_info.operator_name, operand_position, missing_col);

    result
}

/// 오퍼레이터와 현재 타입에 대해 가능한 타입 캐스팅 옵션들을 찾는 함수
pub fn get_typecast_refinements_for_operator(
    operator_info: &OperatorInfo,
    current_left_type: Option<&str>,
    current_right_type: Option<&str>,
) -> Vec<TypecastRefinement> {
    let cache_key = generate_cache_key(
        "typecast_refinements",
        &[
            &operator_info.operator_name,
            current_left_type.unwrap_or("NULL"),
            current_right_type.unwrap_or("NULL"),
        ]
    );

    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<TypecastRefinement>>(&cached_result) {
            // pgrx::notice!("Cache hit for typecast refinements: operator={}, left={:?}, right={:?}",
            //              operator_info.operator_name, current_left_type, current_right_type);
            return result;
        }
    }

    // 캐시 미스 - 실제 쿼리 실행
    let sql = r#"
        SELECT DISTINCT
            ltyp.typname::text AS left_type,
            rtyp.typname::text AS right_type,
            'left'::text AS cast_side
        FROM pg_vector_operators o
        LEFT JOIN pg_type ltyp ON o.left_type = format_type(ltyp.oid, NULL)::text
        LEFT JOIN pg_type rtyp ON o.right_type = format_type(rtyp.oid, NULL)::text
        WHERE o.operator_name = $1
        AND ($2::text IS NULL OR o.right_type = $2::text)
        AND o.left_type IS NOT NULL
        AND o.left_type <> COALESCE($3::text, '')
        UNION ALL
        SELECT DISTINCT
            ltyp.typname::text AS left_type,
            rtyp.typname::text AS right_type,
            'right'::text AS cast_side
        FROM pg_vector_operators o
        LEFT JOIN pg_type ltyp ON o.left_type = format_type(ltyp.oid, NULL)::text
        LEFT JOIN pg_type rtyp ON o.right_type = format_type(rtyp.oid, NULL)::text
        WHERE o.operator_name = $1
        AND ($3::text IS NULL OR o.left_type = $3::text)
        AND o.right_type IS NOT NULL
        AND o.right_type <> COALESCE($2::text, '')
        ORDER BY cast_side, left_type, right_type;
    "#;

    let result = Spi::connect(|client| -> Result<Vec<TypecastRefinement>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), operator_info.operator_name.clone().into_datum()),
            (PgOid::from(pg_sys::TEXTOID), current_right_type.map(|s| s.to_string()).into_datum()),
            (PgOid::from(pg_sys::TEXTOID), current_left_type.map(|s| s.to_string()).into_datum()),
        ];

        let rows = client.select(sql, None, Some(args))?;
        let mut refinements = Vec::new();

        for row in rows {
            if let (
                Ok(Some(left_type)),
                Ok(Some(right_type)),
                Ok(Some(cast_side))
            ) = (
                row["left_type"].value::<String>(),
                row["right_type"].value::<String>(),
                row["cast_side"].value::<String>()
            ) {
                let refinement = match cast_side.as_str() {
                    "left" => TypecastRefinement {
                        cast_position: OperandPosition::Left,
                        target_type: left_type,
                        fixed_type: Some(right_type),
                    },
                    "right" => TypecastRefinement {
                        cast_position: OperandPosition::Right,
                        target_type: right_type,
                        fixed_type: Some(left_type),
                    },
                    _ => continue,
                };
                refinements.push(refinement);
            }
        }

        // pgrx::notice!("SafeQL: Found {} typecast refinements for operator {} (left: {:?}, right: {:?})",
        //              refinements.len(), operator_info.operator_name, current_left_type, current_right_type);

        Ok(refinements)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "typecast_refinements", &result_json);
    }

    result
}

/// 함수 호환 가능한 argument들을 찾는 함수 (캐시 적용)
pub fn find_compatible_arguments_for_function(
    table_name: Option<&str>,
    missing_col: &str,
    function_name: &str,
    argument_position: &ArgumentPosition,
) -> Vec<(String, String, f32)> {
    // 검색어 준비
    let search_terms = prepare_search_terms(missing_col);
    let search_key = search_terms.join("_");
    
    // 캐시 키 생성
    let table_key = table_name.unwrap_or("NULL");
    let function_key = format!("{}_{}_{}_{}", 
        function_name,
        argument_position.index,
        ENABLE_TYPE_BASED_REFINEMENT.get(),
        missing_col
    );
    let cache_key = generate_cache_key(
        "function_compatible_arguments", 
        &[table_key, &search_key, &function_key]
    );
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for function compatible arguments query: table={:?}, col={}, function={}, arg_pos={} (terms: {:?})", 
            //              table_name, missing_col, function_name, argument_position.index, search_terms);
            return result;
        }
    }

    // 검색 벡터 쿼리 생성
    let vector_query = create_combined_vector_query(&search_terms);

    // 타입 기반 최적화가 활성화되어 있으면 호환 가능한 타입만 검색
    let type_filter = if ENABLE_TYPE_BASED_REFINEMENT.get() {
        if let Some(required_types) = get_compatible_types_for_function(function_name, argument_position) {
            format!("AND f.fieldtype = ANY(ARRAY[{}])", 
                required_types.iter()
                    .map(|t| format!("'{}'", t))
                    .collect::<Vec<_>>()
                    .join(","))
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // TOP_K_EXPANSION을 적용한 LIMIT 값 계산
    let limit_value = TOP_K_EXPANSION.get();

    // 캐시 미스 - 실제 쿼리 실행
    let sql = if table_name.is_some() {
        format!(r#"
        WITH q AS (
            {}
        ),
        check_table AS (
            SELECT EXISTS (
                SELECT 1
                FROM pg_vector_fields
                WHERE schemaname = ANY (current_schemas(false))
                  AND tablename = $1
            ) AS has_table
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q, check_table c
        WHERE f.schemaname = ANY (current_schemas(false))
          AND f.fieldname <> $2
          AND (
                (c.has_table AND f.tablename = $1)
                OR (NOT c.has_table)
          )
          {}
        ORDER BY distance
        LIMIT {}
        "#, vector_query, type_filter, limit_value)
    } else {
        format!(r#"
        WITH q AS (
            {}
        )
        SELECT
            f.fieldname,
            f.tablename,
            (f.embedding <=> q.v) AS distance
        FROM pg_vector_fields f, q
        WHERE f.schemaname = ANY (current_schemas(false))
          AND f.fieldname <> $1
          {}
        ORDER BY distance
        LIMIT {}
        "#, vector_query, type_filter, limit_value)
    };

    let result = Spi::connect(|client| -> Result<Vec<(String, String, f32)>, pgrx::spi::Error> {
        let args = if let Some(t) = table_name {
            vec![
                (PgOid::from(pg_sys::TEXTOID), t.into_datum()),
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
            ]
        } else {
            vec![
                (PgOid::from(pg_sys::TEXTOID), missing_col.into_datum()),
            ]
        };

        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();

        for row in rows {
            if let (Ok(Some(col_name)), Ok(Some(tbl_name)), Ok(Some(distance))) = (
                row["fieldname"].value::<String>(),
                row["tablename"].value::<String>(),
                row["distance"].value::<f32>()
            ) {
                result.push((col_name, tbl_name, distance));
            }
        }

        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "function_compatible_arguments", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} compatible arguments for function {} (arg_pos: {}, original_col: {})", 
    //              result.len(), function_name, argument_position.index, missing_col);

    result
}

/// 함수와 현재 타입들에 대해 가능한 타입 캐스팅 옵션들을 찾는 함수
pub fn get_typecast_refinements_for_function(
    function_info: &FunctionInfo,
    current_arg_types: &[String],
) -> Vec<FunctionTypecastRefinement> {
    let cache_key = generate_cache_key(
        "function_typecast_refinements",
        &[
            &function_info.function_name,
            &current_arg_types.join(","),
        ]
    );

    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<FunctionTypecastRefinement>>(&cached_result) {
            // pgrx::notice!("Cache hit for function typecast refinements: function={}, args={:?}",
            //              function_info.function_name, current_arg_types);
            return result;
        }
    }

    // 캐시 미스 - 실제 쿼리 실행
    let sql = r#"
        WITH args_expanded AS (
            SELECT DISTINCT
                f.function_name,
                f.schemaname,
                f.arg_types,
                trim(unnest(string_to_array(f.arg_types, ','))) AS arg_type_str,
                generate_subscripts(string_to_array(f.arg_types, ','), 1) - 1 AS arg_index
            FROM pg_vector_functions f
            WHERE f.function_name = $1
            AND f.schemaname = ANY(current_schemas(true))
        ),
        args_with_internal_types AS (
            SELECT 
                ae.*,
                COALESCE(typ.typname::text, ae.arg_type_str) AS internal_arg_type
            FROM args_expanded ae
            LEFT JOIN pg_type typ ON format_type(typ.oid, NULL)::text = ae.arg_type_str
        )
        SELECT DISTINCT
            (SELECT string_agg(awit2.internal_arg_type, ',' ORDER BY awit2.arg_index)
            FROM args_with_internal_types awit2 
            WHERE awit2.function_name = awit.function_name 
            AND awit2.schemaname = awit.schemaname 
            AND awit2.arg_types = awit.arg_types) AS arg_types,
            awit.internal_arg_type AS arg_type,
            awit.arg_index
        FROM args_with_internal_types awit
        ORDER BY arg_index;
    "#;

    let result = Spi::connect(|client| -> Result<Vec<FunctionTypecastRefinement>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), function_info.function_name.clone().into_datum()),
        ];

        let rows = client.select(sql, None, Some(args))?;
        let mut refinements = Vec::new();

        for row in rows {
            if let (
                Ok(Some(arg_types_str)),
                Ok(Some(arg_type)),
                Ok(Some(arg_index))
            ) = (
                row["arg_types"].value::<String>(),
                row["arg_type"].value::<String>(),
                row["arg_index"].value::<i32>()
            ) {
                let arg_index = arg_index as usize;
                
                // 현재 타입과 다른 경우에만 refinement 생성
                if arg_index < current_arg_types.len() {
                    let current_type = &current_arg_types[arg_index];
                    if arg_type.trim() != current_type.trim() && !arg_type.trim().is_empty() {
                        let refinement = FunctionTypecastRefinement {
                            cast_position: ArgumentPosition { index: arg_index },
                            target_type: arg_type.trim().to_string(),
                            compatible_arg_types: arg_types_str,
                        };
                        refinements.push(refinement);
                    }
                }
            }
        }

        // pgrx::notice!("SafeQL: Found {} function typecast refinements for {} (args: {:?})",
        //              refinements.len(), function_info.function_name, current_arg_types);

        Ok(refinements)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "function_typecast_refinements", &result_json);
    }

    result
}


/// 함수명과 유사한 함수들을 찾는 함수 (캐시 적용)
pub fn find_similar_functions(
    function_name: &str,
    arg_count: usize,
) -> Vec<(String, Vec<String>, String, f32)> {
    let search_terms = prepare_search_terms(function_name);
    let search_key = search_terms.join("_");
    
    let top_k = TOP_K_EXPANSION.get();
    let cache_key = generate_cache_key(
        "similar_functions", 
        &[&search_key, &arg_count.to_string(), &top_k.to_string()]
    );
    
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, Vec<String>, String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for similar functions query: func={}, arg_count={} (terms: {:?}, top_k: {})", 
            //              function_name, arg_count, search_terms, top_k);
            return result;
        }
    }

    let vector_query = create_combined_vector_query(&search_terms);
    
    let sql = format!(r#"
        WITH q AS (
            {}
        )
        SELECT function_name, arg_types, return_type, distance
        FROM (
            SELECT 
                f.function_name,
                f.arg_types,
                f.return_type,
                (f.embedding <=> q.v) AS distance,
                ROW_NUMBER() OVER (PARTITION BY f.function_name ORDER BY (f.embedding <=> q.v)) AS rn
            FROM pg_vector_functions f, q
            WHERE f.schemaname = ANY (current_schemas(true))
            AND array_length(string_to_array(f.arg_types, ','), 1) = $1
            AND f.function_name <> $2
        ) sub
        WHERE rn = 1
        ORDER BY distance
        LIMIT $3;
    "#, vector_query);

    let result = Spi::connect(|client| -> Result<Vec<(String, Vec<String>, String, f32)>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::INT4OID), (arg_count as i32).into_datum()),
            (PgOid::from(pg_sys::TEXTOID), function_name.into_datum()),
            (PgOid::from(pg_sys::INT4OID), top_k.into_datum())
        ];
        
        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();
        
        for row in rows {
            if let (
                Ok(Some(func_name)), 
                Ok(Some(arg_types_str)), 
                Ok(Some(return_type)),
                Ok(Some(distance))
            ) = (
                row["function_name"].value::<String>(),
                row["arg_types"].value::<String>(),
                row["return_type"].value::<String>(),
                row["distance"].value::<f32>()
            ) {
                let arg_types: Vec<String> = arg_types_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                
                result.push((func_name, arg_types, return_type, distance));
            }
        }
        
        Ok(result)
    }).unwrap_or_default();

    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "similar_functions", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} similar functions for '{}' with {} args (top_k: {})", 
    //              result.len(), function_name, arg_count, top_k);
    result
}


/// 함수명이 정확히 일치하는 함수가 존재하는지 확인 (캐시 적용)
pub fn check_function_exists(function_name: &str) -> bool {
    let cache_key = generate_cache_key(
        "function_exists", 
        &[function_name]
    );
    
    // 캐시 확인
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(exists) = cached_result.parse::<bool>() {
            // pgrx::notice!("Cache hit for function existence check: func={}, exists={}", 
            //              function_name, exists);
            return exists;
        }
    }

    // SQL 쿼리로 함수 존재 여부 확인
    let sql = r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_vector_functions f
            WHERE f.function_name = $1
                AND f.schemaname = ANY (current_schemas(true))
            LIMIT 1
        )
    "#;

    let result = Spi::connect(|client| -> Result<bool, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), function_name.into_datum())
        ];
        
        let rows = client.select(sql, None, Some(args))?;
        
        for row in rows {
            if let Ok(Some(exists)) = row[1].value::<bool>() {
                return Ok(exists);
            }
        }
        
        Ok(false)
    }).unwrap_or(false);

    // 결과를 캐시에 저장
    let _ = store_cached_result(&cache_key, "function_exists", &result.to_string());

    // pgrx::notice!("SafeQL: Function '{}' exists: {}", function_name, result);
    result
}

/// literal 값과 가장 유사한 실제 DB 값들을 찾는 함수 (캐시 적용)
/// 가장 가까운 값 하나만 반환 (nearest neighbor)
pub fn find_similar_values_for_literal(
    table_name: &str,
    column_name: &str,
    literal_value: &str,
) -> Vec<(String, String, String, f32)> {
    // 검색어 준비
    let search_terms = prepare_search_terms(literal_value);
    let search_key = search_terms.join("_");
    
    let top_k = TOP_K_EXPANSION.get();
    // 캐시 키 생성
    let cache_key = generate_cache_key(
        "similar_values", 
        &[table_name, column_name, &search_key]
    );
    
    // 캐시에서 결과 조회
    if let Some(cached_result) = get_cached_result(&cache_key) {
        if let Ok(result) = serde_json::from_str::<Vec<(String, String, String, f32)>>(&cached_result) {
            // pgrx::notice!("Cache hit for similar values query: table={}, col={}, literal={} (terms: {:?})", 
            //              table_name, column_name, literal_value, search_terms);
            return result;
        }
    }
    
    // 1. 먼저 검색 벡터를 가져옴
    let vector_query = create_combined_vector_query(&search_terms);
    let vector_query_as_text = format!("SELECT ({})::text", vector_query.trim_start_matches("SELECT ").trim_end_matches(" AS v"));

    let search_vector: Option<String> = Spi::get_one(&vector_query_as_text)
        .unwrap_or(None);
    
    if search_vector.is_none() {
        // pgrx::warning!("Failed to get search vector for literal: {}", literal_value);
        return Vec::new();
    }
    
    let search_vector = search_vector.unwrap();
    
    // 2. 가져온 벡터를 상수로 사용하여 유사도 검색
    let sql = format!(r#"
        SELECT 
            v.value,
            (v.embedding <=> '{}'::vector) AS distance
        FROM pg_vector_values v
        WHERE v.schemaname = ANY (current_schemas(false))
          AND v.tablename = $1
          AND v.columnname = $2
          AND v.value IS NOT NULL
          AND v.value <> ''
        ORDER BY v.embedding <=> '{}'::vector
        LIMIT {}
    "#, search_vector, search_vector, top_k);

    let result = Spi::connect(|client| -> Result<Vec<(String, String, String, f32)>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), table_name.into_datum()),
            (PgOid::from(pg_sys::TEXTOID), column_name.into_datum()),
        ];
        
        let rows = client.select(&sql, None, Some(args))?;
        let mut result = Vec::new();
        
        for row in rows {
            if let (Ok(Some(value)), Ok(Some(distance))) = 
                (row["value"].value::<String>(), row["distance"].value::<f32>()) {
                result.push((
                    value,
                    table_name.to_string(),
                    column_name.to_string(),
                    distance
                ));
            }
        }
        
        Ok(result)
    }).unwrap_or_default();

    // 결과를 캐시에 저장 (실패해도 무시)
    if let Ok(result_json) = serde_json::to_string(&result) {
        let _ = store_cached_result(&cache_key, "similar_values", &result_json);
    }

    // pgrx::notice!("SafeQL: Found {} similar value(s) for literal '{}' in {}.{}", 
    //              result.len(), literal_value, table_name, column_name);
    result
}

/// 오퍼레이터 위치를 나타내는 enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperandPosition {
    Left,
    Right,
}

/// 함수 argument 위치를 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgumentPosition {
    pub index: usize,
}

/// 타입 캐스팅 refinement 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypecastRefinement {
    pub cast_position: OperandPosition,  // 어느 쪽을 캐스팅할 것인지
    pub target_type: String,            // 캐스팅할 타겟 타입
    pub fixed_type: Option<String>,     // 고정된 다른 쪽 타입 (참고용)
}

/// 함수 타입 캐스팅 refinement 정보
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionTypecastRefinement {
    pub cast_position: ArgumentPosition,   // 어느 argument를 캐스팅할 것인지
    pub target_type: String,              // 캐스팅할 타겟 타입
    pub compatible_arg_types: String,     // 호환되는 전체 argument 타입 리스트
}

/// 오퍼레이터와 호환 가능한 타입들 찾기 - operand position별로 처리
fn get_compatible_types_for_operator(
    operator_info: &OperatorInfo,
    operand_position: &OperandPosition,
) -> Option<Vec<String>> {
    if !ENABLE_TYPE_BASED_REFINEMENT.get() {
        return None;
    }
    
    let sql = match operand_position {
        OperandPosition::Left => {
            // left operand를 바꾸려는 경우: right_type이 고정이고 가능한 left_type들을 찾음
            r#"
                SELECT DISTINCT left_type as compatible_type
                FROM pg_vector_operators
                WHERE operator_name = $1
                  AND ($2::text IS NULL OR right_type = $2::text)
                  AND left_type IS NOT NULL
                  AND left_type <> $3::text  -- 현재 실패한 타입은 제외
                ORDER BY left_type
            "#
        },
        OperandPosition::Right => {
            // right operand를 바꾸려는 경우: left_type이 고정이고 가능한 right_type들을 찾음
            r#"
                SELECT DISTINCT right_type as compatible_type
                FROM pg_vector_operators
                WHERE operator_name = $1
                  AND ($2::text IS NULL OR left_type = $2::text)
                  AND right_type IS NOT NULL
                  AND right_type <> $3::text  -- 현재 실패한 타입은 제외
                ORDER BY right_type
            "#
        }
    };
    
    let result = Spi::connect(|client| -> Result<Vec<String>, pgrx::spi::Error> {
        let (fixed_type, failed_type) = match operand_position {
            OperandPosition::Left => (
                operator_info.right_type.clone(), 
                operator_info.left_type.clone()
            ),
            OperandPosition::Right => (
                operator_info.left_type.clone(), 
                operator_info.right_type.clone()
            ),
        };
        
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), operator_info.operator_name.clone().into_datum()),
            (PgOid::from(pg_sys::TEXTOID), fixed_type.into_datum()),
            (PgOid::from(pg_sys::TEXTOID), failed_type.into_datum()),
        ];
        
        let rows = client.select(sql, None, Some(args))?;
        let mut types = Vec::new();
        
        for row in rows {
            if let Ok(Some(compatible_type)) = row["compatible_type"].value::<String>() {
                if !compatible_type.is_empty() {
                    types.push(compatible_type);
                }
            }
        }
        
        // pgrx::notice!("SafeQL: Found {} compatible types for operator {} (position: {:?}): {:?}", 
        //              types.len(), operator_info.operator_name, operand_position, types);
        
        Ok(types)
    }).unwrap_or_default();
    
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// 함수와 호환 가능한 타입들 찾기 - argument position별로 처리
fn get_compatible_types_for_function(
    function_name: &str,
    argument_position: &ArgumentPosition,
) -> Option<Vec<String>> {
    if !ENABLE_TYPE_BASED_REFINEMENT.get() {
        return None;
    }
    
    let sql = r#"
        SELECT DISTINCT 
            (string_to_array(arg_types, ','))[($2::int + 1)] as compatible_type
        FROM pg_vector_functions
        WHERE function_name = $1
        AND schemaname = ANY(current_schemas(true))
        AND array_length(string_to_array(arg_types, ','), 1) > $2::int
        AND (string_to_array(arg_types, ','))[($2::int + 1)] IS NOT NULL
        AND trim((string_to_array(arg_types, ','))[($2::int + 1)]) <> ''
        ORDER BY compatible_type
    "#;
    
    let result = Spi::connect(|client| -> Result<Vec<String>, pgrx::spi::Error> {
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), function_name.to_string().into_datum()),
            (PgOid::from(pg_sys::INT4OID), (argument_position.index as i32).into_datum()),
        ];
        
        let rows = client.select(sql, None, Some(args))?;
        let mut types = Vec::new();
        
        for row in rows {
            if let Ok(Some(compatible_type)) = row["compatible_type"].value::<String>() {
                let trimmed_type = compatible_type.trim();
                if !trimmed_type.is_empty() {
                    types.push(trimmed_type.to_string());
                }
            }
        }
        
        // pgrx::notice!("SafeQL: Found {} compatible types for function {} (arg_pos: {}): {:?}", 
        //              types.len(), function_name, argument_position.index, types);
        
        Ok(types)
    }).unwrap_or_default();
    
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// 오퍼레이터 정보를 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorInfo {
    pub operator_name: String,
    pub left_type: Option<String>,
    pub right_type: Option<String>,
}

/// 함수 정보를 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub function_name: String,
    pub arg_types: Vec<String>,
}

/// 에러 메시지에서 오퍼레이터 정보 추출
pub fn extract_operator_info_from_error(error_message: &str) -> OperatorInfo {
    // "operator does not exist: bigint = text" 패턴
    let operator_pattern = Regex::new(r"operator does not exist:\s*(\w+)\s*([^\s]+)\s*(\w+)").unwrap();
    
    if let Some(cap) = operator_pattern.captures(error_message) {
        let left_type = cap.get(1).map(|m| m.as_str().to_string());
        let operator_name = cap.get(2).map(|m| m.as_str().to_string()).unwrap_or("=".to_string());
        let right_type = cap.get(3).map(|m| m.as_str().to_string());
        
        return OperatorInfo {
            operator_name,
            left_type,
            right_type,
        };
    }
    
    // "could not identify an equality operator for type" 패턴
    let equality_pattern = Regex::new(r"could not identify an equality operator for type\s+(\w+)").unwrap();
    if let Some(cap) = equality_pattern.captures(error_message) {
        let type_name = cap.get(1).map(|m| m.as_str().to_string());
        return OperatorInfo {
            operator_name: "=".to_string(),
            left_type: type_name.clone(),
            right_type: type_name,
        };
    }
    
    // 기본값: 등호 오퍼레이터
    OperatorInfo {
        operator_name: "=".to_string(),
        left_type: None,
        right_type: None,
    }
}

/// 에러 메시지에서 함수 정보 추출
pub fn extract_function_info_from_error(error_message: &str) -> FunctionInfo {
    // "function pg_catalog.extract(unknown, bigint) does not exist" 패턴
    let function_pattern = Regex::new(r"function\s+(?:[^.]+\.)?([^(]+)\(([^)]*)\)\s+does\s+not\s+exist").unwrap();
    
    if let Some(cap) = function_pattern.captures(error_message) {
        let function_name = cap.get(1).map(|m| m.as_str().trim().to_string()).unwrap_or("unknown".to_string());
        let args_str = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        
        let arg_types = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            args_str.split(',')
                .map(|arg| arg.trim().to_string())
                .filter(|arg| !arg.is_empty())
                .collect()
        };
        
        return FunctionInfo {
            function_name,
            arg_types,
        };
    }
    
    // 기본값
    FunctionInfo {
        function_name: "unknown".to_string(),
        arg_types: Vec::new(),
    }
}

/// JOIN 조건을 나타내는 구조체
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_table: String,   // 기존 테이블
    pub left_column: String,  // 기존 테이블의 컬럼 (보통 PK)
    pub right_table: String,  // 새로 JOIN할 테이블
    pub right_column: String, // 새 테이블의 컬럼 (보통 FK)
}