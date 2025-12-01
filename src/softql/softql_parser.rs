use prost::Message;
use pgrx::pg_sys;
use serde::de::value::Error;
use super::*;
use super::softql_binder::*;


#[allow(improper_ctypes)]
extern "C" {
    fn handle_parsed_result(buf: *const u8, len: usize) -> *mut pg_sys::RawStmt;
}

#[allow(improper_ctypes)]
extern "C" {
    fn deparseRawStmt(str: *mut pg_sys::StringInfoData, raw_stmt: *mut pg_sys::RawStmt);
}

pub fn parse_softql(statement: &str) -> Result<*mut pg_sys::RawStmt, Error> {
    let static_softql = softql::static_parse_softql(statement).unwrap();
    let raw = bind_softql(&static_softql).unwrap();
    let result = protobuf::ParseResult {
        version: pg_sys::PG_VERSION_NUM as i32,           
        stmts: vec![raw],  
    };

    let encoded: Vec<u8> = result.encode_to_vec();
    let raw_stmt = unsafe {
        handle_parsed_result(encoded.as_ptr(), encoded.len())
    };
    return Ok(raw_stmt);
}

pub fn deparse_raw_stmt(raw_stmt: *mut pg_sys::RawStmt) -> String {
    unsafe {
        let str: *mut pg_sys::StringInfoData = pg_sys::makeStringInfo();
        deparseRawStmt(str, raw_stmt);
        return std::ffi::CStr::from_ptr((*str).data)
            .to_string_lossy()
            .into_owned();
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::softql::softql_parser::*;
    use pgrx::prelude::*;
    use super::protobuf::a_const::Val;
    use super::protobuf::node::Node as NodeOneof;
    
    fn generate_example_sql() -> protobuf::RawStmt {
        let sql = "SELECT 7";

        let a_const = protobuf::AConst {
            isnull: false,
            location: 7,
            val: Some(Val::Ival(protobuf::Integer { ival: 7 })),
        };
        let lit_node = protobuf::Node {
            node: Some(NodeOneof::AConst(a_const)),
        };

        let res_target = protobuf::ResTarget {
            name: "hello".to_string(),
            indirection: Vec::new(),
            val: Some(Box::new(lit_node)),
            location: 7,
        };
        let target_node = protobuf::Node {
            node: Some(NodeOneof::ResTarget(Box::new(res_target))),
        };

        let select_stmt = protobuf::SelectStmt {
            target_list: vec![target_node],
            limit_option: protobuf::LimitOption::Count as i32, 
            op: protobuf::SetOperation::SetopNone as i32,
            ..Default::default()
        };
        let stmt_node = protobuf::Node {
            node: Some(NodeOneof::SelectStmt(Box::new(select_stmt))),
        };

        protobuf::RawStmt {
            stmt: Some(Box::new(stmt_node)),
            stmt_location: 0,
            stmt_len: sql.len() as i32,
        }
    }

    #[pg_test]
    fn test_parse_example_sql() {
        let raw = generate_example_sql();
        let result = protobuf::ParseResult {
            version: pg_sys::PG_VERSION_NUM as i32,         
            stmts: vec![raw],  
        };

        let encoded: Vec<u8> = result.encode_to_vec();
        let raw_stmt = unsafe {
            handle_parsed_result(encoded.as_ptr(), encoded.len())
        };

        assert!(!raw_stmt.is_null(), "Failed to parse SQL");
    }

    #[pg_test]
    fn test_deparse_example_sql() {
        let raw = generate_example_sql();
        let result = protobuf::ParseResult {
            version: pg_sys::PG_VERSION_NUM as i32,
            stmts: vec![raw],
        };

        let encoded: Vec<u8> = result.encode_to_vec();
        let raw_stmt = unsafe {
            handle_parsed_result(encoded.as_ptr(), encoded.len())
        };

        assert!(!raw_stmt.is_null(), "Failed to parse SQL");

        let sql_string = deparse_raw_stmt(raw_stmt);
        println!("Deparsed SQL: {}", sql_string);

        // 간단한 검증 (출력에 SELECT와 숫자 7이 포함되어야 함)
        assert!(sql_string.to_uppercase().contains("SELECT"));
        assert!(sql_string.contains("7"));
    }
}
