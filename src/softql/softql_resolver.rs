use crate::gucs::parser::{BIND_MODE, BindMode};
use pgrx::pg_sys;
use pgrx::{IntoDatum, PgOid, Spi};
use super::*;
use super::protobuf::a_const::Val;
use super::protobuf::RangeVar;


pub enum ResolvedCall {
    Operator { symbol: String, kind: char },
    Function { schema: String, func: String },
}

/// 1) 문자열 → Val::Sval
pub fn resolve_sval(soft_token: String) -> Option<Val> {
    Spi::connect(|client| -> Result<Option<Val>, pgrx::spi::Error> {
        // both soft and hard use CTE q to pull out $1 once
        let sql = "
            SELECT $1::text AS val
        ";
        let args = vec![
            (PgOid::from(pg_sys::TEXTOID), soft_token.clone().into_datum())
        ];
        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let sval: String = row["val"].value().unwrap().expect("val is NULL");
            return Ok(Some(Val::Sval(protobuf::String { sval })));
        }
        Ok(None)
    }).unwrap_or(None)
}

/// 2) 정수 → Val::Ival
pub fn resolve_ival(soft_token: i32) -> Option<Val> {
    Spi::connect(|client| -> Result<Option<Val>, pgrx::spi::Error> {
        let sql = "
            SELECT $1::int AS val
        ";
        let args = vec![
            (PgOid::from(pg_sys::INT4OID), soft_token.into_datum())
        ];
        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let ival: i32 = row["val"].value().unwrap().expect("val is NULL");
            return Ok(Some(Val::Ival(protobuf::Integer { ival })));
        }
        Ok(None)
    }).unwrap_or(None)
}

/// 3) 부동소수 → Val::Fval
pub fn resolve_fval(soft_token: f64) -> Option<Val> {
    Spi::connect(|client| -> Result<Option<Val>, pgrx::spi::Error> {
        let sql = "
            SELECT $1::float8 AS val
        ";
        let args = vec![
            (PgOid::from(pg_sys::FLOAT8OID), soft_token.into_datum())
        ];
        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let fval: String = row["val"].value().unwrap().expect("val is NULL");
            return Ok(Some(Val::Fval(protobuf::Float { fval })));
        }
        Ok(None)
    }).unwrap_or(None)
}

/// 4) 불리언 → Val::Boolval
pub fn resolve_boolval(soft_token: bool) -> Option<Val> {
    Spi::connect(|client| -> Result<Option<Val>, pgrx::spi::Error> {
        let sql = "
            SELECT $1::bool AS val
        ";
        let args = vec![
            (PgOid::from(pg_sys::BOOLOID), soft_token.into_datum())
        ];
        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let boolval: bool = row["val"].value().unwrap().expect("val is NULL");
            return Ok(Some(Val::Boolval(protobuf::Boolean { boolval })));
        }
        Ok(None)
    }).unwrap_or(None)
}

/// 5) 필드 이름 매핑
pub fn resolve_fieldname(
    schema: String,
    table: String,
    soft_field: String
) -> Option<String> {
    Spi::connect(|client| -> Result<Option<String>, pgrx::spi::Error> {
        let (sql, args) = match BIND_MODE.get() {
            BindMode::soft => {
                // compute embedding once as q.v
                let sql = "
                    WITH q AS (
                        SELECT _vectors_text2vec($3) AS v
                    )
                    SELECT fieldname
                    FROM pg_vector_fields, q
                    WHERE schemaname = $1
                      AND tablename  = $2
                    ORDER BY embedding <=> q.v
                    LIMIT 1
                ";
                let args = vec![
                    (PgOid::from(pg_sys::TEXTOID), schema.clone().into_datum()),
                    (PgOid::from(pg_sys::TEXTOID), table.clone().into_datum()),
                    (PgOid::from(pg_sys::TEXTOID), soft_field.clone().into_datum()),
                ];
                (sql, args)
            }
            BindMode::hard => {
                // use CTE just to extract $3 once
                let sql = "
                    WITH q AS (
                        SELECT $3::text AS v
                    )
                    SELECT fieldname
                    FROM pg_vector_fields, q
                    WHERE schemaname = $1
                      AND tablename  = $2
                      AND fieldname  = q.v
                    LIMIT 1
                ";
                let args = vec![
                    (PgOid::from(pg_sys::TEXTOID), schema.clone().into_datum()),
                    (PgOid::from(pg_sys::TEXTOID), table.clone().into_datum()),
                    (PgOid::from(pg_sys::TEXTOID), soft_field.clone().into_datum()),
                ];
                (sql, args)
            }
        };

        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let field: String = row["fieldname"]
                .value().unwrap().expect("fieldname is NULL");
            return Ok(Some(field));
        }
        Ok(None)
    }).unwrap_or(None)
}


/// 7) 테이블 이름 매핑
pub fn resolve_relname(soft_relname: String) -> Option<(String, String)> {
    Spi::connect(|client| -> Result<Option<(String, String)>, pgrx::spi::Error> {
        let (sql, args) = match BIND_MODE.get() {
            BindMode::soft => {
                let sql = "
                    WITH q AS (
                        SELECT _vectors_text2vec($1) AS v
                    )
                    SELECT schemaname, tablename
                    FROM pg_vector_tables, q
                    WHERE schemaname = ANY (current_schemas(false))
                    ORDER BY embedding <=> q.v
                    LIMIT 1
                ";
                let args = vec![
                    (PgOid::from(pg_sys::TEXTOID), soft_relname.clone().into_datum())
                ];
                (sql, args)
            }
            BindMode::hard => {
                let sql = "
                    WITH q AS (
                        SELECT $1::text AS v
                    )
                    SELECT schemaname, tablename
                    FROM pg_vector_tables, q
                    WHERE schemaname = ANY (current_schemas(false))
                      AND tablename = q.v
                    LIMIT 1
                ";
                let args = vec![
                    (PgOid::from(pg_sys::TEXTOID), soft_relname.clone().into_datum())
                ];
                (sql, args)
            }
        };

        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let schema: String = row["schemaname"]
                .value().unwrap().expect("schemaname is NULL");
            let table: String = row["tablename"]
                .value().unwrap().expect("tablename is NULL");
            return Ok(Some((schema, table)));
        }
        Ok(None)
    }).unwrap_or(None)
}

/// 8) RangeVar 생성
pub fn resolve_rangevar(soft_relname: String) -> Option<RangeVar> {
    let (schema, table) = resolve_relname(soft_relname)?;
    Some(RangeVar {
        catalogname: "".into(),
        schemaname:  schema,
        relname:     table,
        inh:         true,
        relpersistence: "p".into(),
        alias:       None,
        location:    -1,
    })
}

pub fn resolve_call(raw_name: &str) -> Option<ResolvedCall> {
    // snake_case & camelCase → “greater than” 같은 자연어 키
    let search_key = match BIND_MODE.get() {
        BindMode::soft => normalize_soft_name(raw_name),
        BindMode::hard => raw_name.to_owned(),
    };

    Spi::connect(|client| -> Result<Option<ResolvedCall>, pgrx::spi::Error> {
        // ── ① SQL & 파라미터 ----------------------------------------------------
        let (sql, args) = match BIND_MODE.get() {
            /* ---------- SOFT : 벡터 유사도 ---------- */
            BindMode::soft => {
                let sql = r#"
                    WITH q AS (SELECT _vectors_text2vec($1) AS v)
                    SELECT *
                    FROM (
                        SELECT 'operator' AS kind,
                               operator_name,
                               oprkind::text,
                               NULL::text AS schemaname,
                               NULL::text AS function_name,
                               embedding <=> q.v AS distance
                        FROM pg_vector_operators, q
                        UNION ALL
                        SELECT 'function' AS kind,
                               NULL,
                               NULL,
                               schemaname,
                               function_name,
                               embedding <=> q.v
                        FROM pg_vector_functions, q
                    ) ranked
                    ORDER BY distance
                    LIMIT 1;
                "#;
                let args = vec![(PgOid::from(pg_sys::TEXTOID), search_key.clone().into_datum())];
                (sql, args)
            }

            /* ---------- HARD : 정확히 같은 문자열 ---------- */
            BindMode::hard => {
                let sql = r#"
                    WITH q AS (SELECT $1::text AS v)
                    SELECT *
                    FROM (
                        SELECT 'operator' AS kind,
                               operator_name,
                               oprkind::text,
                               NULL::text AS schemaname,
                               NULL::text AS function_name
                        FROM pg_vector_operators, q
                        WHERE operator_name = q.v
                        UNION ALL
                        SELECT 'function' AS kind,
                               NULL,
                               NULL,
                               schemaname,
                               function_name
                        FROM pg_vector_functions, q
                        WHERE function_name = q.v
                    ) ranked
                    LIMIT 1;
                "#;
                let args = vec![(PgOid::from(pg_sys::TEXTOID), search_key.clone().into_datum())];
                (sql, args)
            }
        };

        // ── ② 실행 & 결과 해석 ---------------------------------------------------
        let rows = client.select(sql, None, Some(args))?;
        for row in rows {
            let kind: String = row["kind"].value().unwrap().expect("kind is NULL");
            if kind == "operator" {
                let op: String = row["operator_name"].value().unwrap().expect("operator_name is NULL");
                let oprkind: String = row["oprkind"].value().unwrap().expect("oprkind is NULL");
                return Ok(Some(ResolvedCall::Operator {
                    symbol: op,
                    kind: oprkind.chars().next().unwrap(),
                }));
            } else {
                let schema: String = row["schemaname"].value().unwrap().expect("schemaname is NULL");
                let func: String = row["function_name"].value().unwrap().expect("function_name is NULL");
                return Ok(Some(ResolvedCall::Function { schema, func }));
            }
        }
        Ok(None)
    }).unwrap_or(None)
}

/// Convert “greater_than”, “greaterThanOrEqual”, “GreaterEqual” …
fn normalize_soft_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_lower = false;

    for ch in name.chars() {
        match ch {
            '_' => {                      // snake-case → space
                if !out.ends_with(' ') { out.push(' '); }
                prev_lower = false;
            }
            c if c.is_ascii_uppercase() => { // camelCase → space before upper
                if prev_lower { out.push(' '); }
                out.push(c.to_ascii_lowercase());
                prev_lower = false;
            }
            c => {
                out.push(c);
                prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
            }
        }
    }
    out
}
