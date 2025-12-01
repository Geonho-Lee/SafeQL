use pgrx::pg_sys;
use pgrx::spi::{Spi, Result as SpiResult, SpiError};
use embedding::get_model_info_by_name;
use crate::gucs::model::VECTOR_EMBEDDING_BATCH_SIZE;

#[pgrx::pg_extern]
fn load_vector_tables() -> SpiResult<()> {
    // 1) read GUCs (will throw a SQL error if not set)
    let model: String = Spi::get_one(
        "SELECT current_setting('vectors.embedding_model_name')"
    )?
    .expect("vectors.embedding_model_name must be set");

    let dim = get_model_info_by_name(model.clone())
        .map_err(|e| SpiError::CursorNotFound(format!("Failed to get model info: {}", e)))?
        .1;

    // 2) 기존 테이블 제거(또는 필요에 따라 TRUNCATE)
    Spi::run("DROP TABLE IF EXISTS pg_vector_tables")?;

    // 3) 모델별 차원에 맞춘 vector(n) 컬럼으로 테이블 생성
    Spi::run(&format!(
        "CREATE TABLE pg_vector_tables (
            schemaname TEXT,
            tablename  TEXT,
            model      TEXT,
            embedding  vector({dim})
        )"
    ))?;

    // 4) INSERT: schemaname, tablename, model, embedding
    Spi::run(&format!(
        r#"
        WITH numbered AS (
            SELECT tablename,
                   schemaname,
                   (row_number() OVER () - 1) / 256 AS batch_id
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND tablename NOT LIKE 'pg_vector_%'
        ),
        batches AS (
            SELECT schemaname,
                   batch_id,
                   array_agg(tablename ORDER BY tablename) AS tbls
            FROM numbered
            GROUP BY schemaname, batch_id
        )
        INSERT INTO pg_vector_tables (schemaname, tablename, model, embedding)
        SELECT
            b.schemaname,
            tbl,
            '{model}',
            v
        FROM batches b,
             unnest(b.tbls) WITH ORDINALITY AS tbl_item(tbl, idx),
             _vectors_text2vec_array(b.tbls) WITH ORDINALITY AS v_item(v, idx2)
        WHERE tbl_item.idx = v_item.idx2;
        "#
    ))?;

    Ok(())
}

#[pgrx::pg_extern]
fn load_vector_fields() -> SpiResult<()> {
    // 1) read GUCs (will throw a SQL error if not set)
    let model: String = Spi::get_one(
        "SELECT current_setting('vectors.embedding_model_name')"
    )?
    .expect("vectors.embedding_model_name must be set");

    let dim = get_model_info_by_name(model.clone())
        .map_err(|e| SpiError::CursorNotFound(format!("Failed to get model info: {}", e)))?
        .1;

    // 2) 기존 테이블 제거
    Spi::run("DROP TABLE IF EXISTS pg_vector_fields")?;

    // 3) 모델별 차원에 맞춘 vector(n) 컬럼 + type 컬럼 추가
    Spi::run(&format!(
        "CREATE TABLE pg_vector_fields (
            schemaname TEXT,
            tablename  TEXT,
            fieldname  TEXT,
            fieldtype  TEXT,
            model      TEXT,
            embedding  vector({dim})
        )"
    ))?;

    // 4) INSERT 시 data_type도 함께 삽입
    Spi::run(&format!(
        r#"
        WITH numbered AS (
            SELECT table_schema AS schemaname,
                   table_name AS tablename,
                   column_name AS fieldname,
                   data_type AS fieldtype,
                   (row_number() OVER () - 1) / 256 AS batch_id
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND table_name NOT LIKE 'pg_vector_%'
        ),
        batches AS (
            SELECT schemaname,
                   tablename,
                   batch_id,
                   array_agg(fieldname ORDER BY fieldname) AS flds,
                   array_agg(fieldtype ORDER BY fieldname) AS ftypes
            FROM numbered
            GROUP BY schemaname, tablename, batch_id
        )
        INSERT INTO pg_vector_fields (schemaname, tablename, fieldname, fieldtype, model, embedding)
        SELECT
            b.schemaname,
            b.tablename,
            fld,
            ftype,
            '{model}',
            v
        FROM batches b,
             unnest(b.flds, b.ftypes) WITH ORDINALITY AS fld_item(fld, ftype, idx),
             _vectors_text2vec_array(b.flds) WITH ORDINALITY AS v_item(v, idx2)
        WHERE fld_item.idx = v_item.idx2;
        "#
    ))?;

    Ok(())
}


#[pgrx::pg_extern]
fn load_vector_operators() -> SpiResult<()> {
    // 1) drop and recreate pg_vector_operators table
    Spi::run("DROP TABLE IF EXISTS pg_vector_operators")?;
    Spi::run(
        "CREATE TABLE pg_vector_operators (
            operator_name TEXT,
            oprkind       char,
            left_type     TEXT,
            right_type    TEXT,
            result_type   TEXT,
            description   TEXT
        )",
    )?;

    // 2) populate table: just metadata (no model, no embedding)
    Spi::run(
        r#"
        INSERT INTO pg_vector_operators
          (operator_name, oprkind, left_type, right_type, result_type, description)
        SELECT *
        FROM (
            SELECT
                o.oprname,
                o.oprkind,
                CASE WHEN o.oprleft = 0 THEN NULL ELSE format_type(o.oprleft, NULL) END AS left_type,
                CASE WHEN o.oprright = 0 THEN NULL ELSE format_type(o.oprright, NULL) END AS right_type,
                format_type(o.oprresult, NULL) AS result_type,
                obj_description(o.oid, 'pg_operator') AS description
            FROM pg_catalog.pg_operator o
        ) sub
        WHERE (left_type IS NULL OR left_type <> 'record')
        AND (right_type IS NULL OR right_type <> 'record')
        ORDER BY oprname;
        "#,
    )?;

    Ok(())
}

#[pgrx::pg_extern]
fn load_vector_functions() -> SpiResult<()> {
    // 1) read GUCs (embedding model info)
    let model: String = Spi::get_one(
        "SELECT current_setting('vectors.embedding_model_name')"
    )?
    .expect("vectors.embedding_model_name must be set");

    let dim = get_model_info_by_name(model.clone())
        .map_err(|e| SpiError::CursorNotFound(format!("Failed to get model info: {}", e)))?
        .1;

    // 2) drop and recreate pg_vector_functions with embedding column
    Spi::run("DROP TABLE IF EXISTS pg_vector_functions")?;
    Spi::run(&format!(
        "CREATE TABLE pg_vector_functions (
            schemaname     TEXT,
            function_name  TEXT,
            arg_types      TEXT,
            return_type    TEXT,
            description    TEXT,
            model          TEXT,
            embedding      vector({dim})
        )"
    ))?;

    // 3) populate table with function metadata + embeddings
    Spi::run(&format!(
        r#"
        WITH temp AS (
            SELECT
                n.nspname AS schemaname,
                p.proname AS function_name,
                pg_get_function_identity_arguments(p.oid) AS arg_types,
                format_type(p.prorettype, NULL) AS return_type,
                obj_description(p.oid, 'pg_proc') AS description,
                (row_number() OVER () - 1) / 256 AS batch_id,
                c.oid AS cast_oid,
                ty.oid AS type_oid
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            LEFT JOIN pg_cast c ON c.castfunc = p.oid
            LEFT JOIN pg_type ty ON ty.typname = p.proname
            WHERE n.nspname NOT IN ('pg_toast')
            AND p.proname !~ '^(RI_FKey_|.*_in$|.*_out$|.*_recv$|.*_send$|.*_typmodin$|.*_typmodout$|.*sel$|.*joinsel$|.*estimate$)'
            AND p.prokind = 'f'
            AND obj_description(p.oid, 'pg_proc') IS NOT NULL
        ), func_list AS (
            SELECT *
            FROM temp
            WHERE cast_oid IS NULL
            AND type_oid IS NULL
            AND description NOT ILIKE '%I/O%'
            AND description NOT ILIKE 'implementation of %'  
            AND function_name NOT ILIKE 'pg_%'
        ), batches AS (
            SELECT
                batch_id,
                array_agg(schemaname ORDER BY schemaname, function_name) AS schemas,
                array_agg(function_name ORDER BY schemaname, function_name) AS funcs,
                array_agg(arg_types ORDER BY schemaname, function_name) AS args,
                array_agg(return_type ORDER BY schemaname, function_name) AS rets,
                array_agg(description ORDER BY schemaname, function_name) AS descs
            FROM func_list
            GROUP BY batch_id
        )
        INSERT INTO pg_vector_functions (schemaname, function_name, arg_types, return_type, description, model, embedding)
        SELECT
            schema,
            func,
            arg,
            ret,
            descr,
            '{model}',
            v
        FROM batches b,
             unnest(b.schemas, b.funcs, b.args, b.rets, b.descs) WITH ORDINALITY 
                AS func_item(schema, func, arg, ret, descr, idx),
             _vectors_text2vec_array(b.descs) WITH ORDINALITY AS v_item(v, idx2)
        WHERE func_item.idx = v_item.idx2;
        "#
    ))?;

    Ok(())
}

use pgrx::IntoDatum;

#[pgrx::pg_extern]
fn load_vector_values() -> SpiResult<()> {
    // 1) read GUCs (embedding model info)
    let model: String = Spi::get_one(
        "SELECT current_setting('vectors.embedding_model_name')"
    )?
    .expect("vectors.embedding_model_name must be set");

    let num_samples: i32 = Spi::get_one(
        "SELECT current_setting('safeql.value_refinement_samples')::INTEGER"
    )?
    .expect("safeql.value_refinement_samples must be set");

    let dim = get_model_info_by_name(model.clone())
        .map_err(|e| SpiError::CursorNotFound(format!("Failed to get model info: {}", e)))?
        .1;

    // 2) drop and recreate pg_vector_values table
    Spi::run("DROP TABLE IF EXISTS pg_vector_values")?;
    Spi::run(&format!(
        "CREATE TABLE pg_vector_values (
            schemaname TEXT,
            tablename  TEXT,
            columnname TEXT,
            value      TEXT,
            model      TEXT,
            embedding  vector({dim})
        )"
    ))?;

    // 3) get all string-type columns from pg_catalog
    let string_columns_query = r#"
        SELECT 
            n.nspname::TEXT AS table_schema,
            c.relname::TEXT AS table_name,
            a.attname::TEXT AS column_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind = 'r'
          AND c.relname NOT LIKE 'pg_vector_%'
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND t.typname IN ('text', 'varchar', 'bpchar', 'char')
        ORDER BY n.nspname, c.relname, a.attname
    "#;

    Spi::connect(|client| {
        let batch_size = VECTOR_EMBEDDING_BATCH_SIZE.get() as usize;
        let tup_table = client.select(string_columns_query, None, None)?;
        
        // 4) Build UNION ALL query for all columns
        let mut union_parts = Vec::new();
        
        for row in tup_table {
            let schema = row["table_schema"].value::<String>()?.unwrap_or_default();
            let table = row["table_name"].value::<String>()?.unwrap_or_default();
            let column = row["column_name"].value::<String>()?.unwrap_or_default();

            let union_part = format!(
                r#"
                (
                    SELECT DISTINCT
                        '{}'::TEXT AS schemaname,
                        '{}'::TEXT AS tablename,
                        '{}'::TEXT AS columnname,
                        {}::TEXT AS value
                    FROM {}.{}
                    WHERE {} IS NOT NULL
                    AND {}::TEXT <> ''
                    LIMIT {}
                )
                "#,
                schema,
                table,
                column,
                quote_ident(&column),
                quote_ident(&schema),
                quote_ident(&table),
                quote_ident(&column),
                quote_ident(&column),
                num_samples
            );
            
            union_parts.push(union_part);
        }

        // If no columns found, return early
        if union_parts.is_empty() {
            pgrx::warning!("No string columns found to process");
            return Ok::<_, SpiError>(());
        }

        // 5) Get all values first
        let all_values_query = format!(
            r#"
            WITH all_values AS (
                {}
            )
            SELECT 
                schemaname,
                tablename,
                columnname,
                value
            FROM all_values
            ORDER BY schemaname, tablename, columnname, value
            "#,
            union_parts.join("\nUNION ALL\n")
        );

        let values_table = client.select(&all_values_query, None, None)?;
        
        // Collect all values
        let mut all_rows: Vec<(String, String, String, String)> = Vec::new();
        for row in values_table {
            let schema = row["schemaname"].value::<String>()?.unwrap_or_default();
            let table = row["tablename"].value::<String>()?.unwrap_or_default();
            let column = row["columnname"].value::<String>()?.unwrap_or_default();
            let value = row["value"].value::<String>()?.unwrap_or_default();
            all_rows.push((schema, table, column, value));
        }

        let total_rows = all_rows.len();
        let total_batches = (total_rows + batch_size - 1) / batch_size;
        
        pgrx::notice!("Starting vector embedding: {} total values, {} batches", total_rows, total_batches);

        // 6) Process in batches
        for (batch_num, chunk) in all_rows.chunks(batch_size).enumerate() {
            let batch_id = batch_num + 1;
            
            // Prepare arrays for this batch
            let schemas: Vec<String> = chunk.iter().map(|(s, _, _, _)| s.clone()).collect();
            let tables: Vec<String> = chunk.iter().map(|(_, t, _, _)| t.clone()).collect();
            let columns: Vec<String> = chunk.iter().map(|(_, _, c, _)| c.clone()).collect();
            let values: Vec<String> = chunk.iter().map(|(_, _, _, v)| v.clone()).collect();
            
            // Build the batch insert query
            let batch_query = format!(
                r#"
                WITH vals AS (
                    SELECT unnest($1::TEXT[]) AS val
                )
                INSERT INTO pg_vector_values (schemaname, tablename, columnname, value, model, embedding)
                SELECT
                    ($2::TEXT[])[idx],
                    ($3::TEXT[])[idx],
                    ($4::TEXT[])[idx],
                    ($1::TEXT[])[idx],
                    $5,
                    v
                FROM generate_series(1, array_length($1::TEXT[], 1)) AS idx,
                     _vectors_text2vec_array($1::TEXT[]) WITH ORDINALITY AS v_item(v, idx2)
                WHERE idx = idx2
                "#
            );
            // Execute batch
            client.select(
                &batch_query,
                None,
                Some(vec![
                    (pgrx::PgBuiltInOids::TEXTARRAYOID.oid(), values.into_datum()),
                    (pgrx::PgBuiltInOids::TEXTARRAYOID.oid(), schemas.into_datum()),
                    (pgrx::PgBuiltInOids::TEXTARRAYOID.oid(), tables.into_datum()),
                    (pgrx::PgBuiltInOids::TEXTARRAYOID.oid(), columns.into_datum()),
                    (pgrx::PgBuiltInOids::TEXTOID.oid(), model.clone().into_datum()),
                ])
            )?;
            
            pgrx::notice!("Batch {}/{} completed ({} values processed)", 
                    batch_id, total_batches, batch_id * batch_size.min(total_rows));
        }

        pgrx::notice!("Vector embedding completed: {} total values processed", total_rows);
        
        Ok::<_, SpiError>(())
    })?;

    Ok(())
}

// Helper function to quote identifiers
fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

#[pgrx::pg_extern]
fn load_vector_similarity_cache() -> SpiResult<()> {
    let create_table_sql = r#"
        CREATE TABLE IF NOT EXISTS pg_vector_similarity_cache (
            cache_key TEXT PRIMARY KEY,
            query_type TEXT NOT NULL,
            result_data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            last_accessed TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    "#;
    
    let create_index_sql = r#"
        CREATE INDEX IF NOT EXISTS idx_pg_vector_cache_type_created 
        ON pg_vector_similarity_cache (query_type, created_at)
    "#;

    Spi::run(create_table_sql)?;
    Spi::run(create_index_sql)?;
    
    Ok(())
}

#[allow(dead_code)]
pub fn vector_catalog_namespace_id() -> pg_sys::Oid {
    unsafe {
        return pg_sys::get_namespace_oid(
            crate::SCHEMA_C_STR.as_ptr() as *const i8,
            false,
        );
    }
}

#[allow(dead_code)]
pub fn pg_catalog_namespace_id() -> pg_sys::Oid {
    unsafe {
        return pg_sys::get_namespace_oid(
            "pg_catalog".as_ptr() as *const i8,
            false,
        );
    }
}

// checks that func_oid is of func_name function in vector_catalog
pub fn is_oid_vector_func(
    func_oid: pg_sys::Oid, 
    func_name: &str
) -> bool {
    assert!(func_oid != pg_sys::Oid::INVALID);
    assert!(func_name.len() > 0);
    
    unsafe {
        let htup = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::PROCOID.try_into().unwrap(),
            func_oid.into()
        );

        assert!(!htup.is_null());

        let op = pg_sys::GETSTRUCT(htup) as pg_sys::Form_pg_proc;
        let nspid = (*op).pronamespace as pg_sys::Oid;
        let procnamedata = (*op).proname as pg_sys::NameData;
        pg_sys::ReleaseSysCache(htup);
    
        let nspname_ptr = pg_sys::get_namespace_name_or_temp(nspid);
        assert!(!nspname_ptr.is_null());
        let nspname = std::ffi::CStr::from_ptr(nspname_ptr).to_str().unwrap().to_owned();
        pg_sys::pfree(nspname_ptr as *mut std::os::raw::c_void);

        let procname = pg_sys::name_data_to_str(&procnamedata);
        return nspname == crate::SCHEMA && procname == func_name;
    }
}