use pgrx::prelude::*;
use pgrx::spi::Spi;
use pgrx::pg_sys;


pub fn show_all_tables_and_columns() -> String {
    let result = Spi::connect(|client| {
        let tables_query = r#"
            WITH search_path_items AS (
            SELECT 
                trim(both '"' FROM unnest(regexp_split_to_array(current_setting('search_path'), '\s*,\s*'))) AS schema_name
            ),
            resolved_path AS (
            SELECT 
                CASE 
                WHEN schema_name = '$user' THEN current_user
                ELSE schema_name
                END AS resolved_schema
            FROM search_path_items
            )
            SELECT table_name::text
            FROM information_schema.tables
            WHERE table_schema IN (SELECT resolved_schema FROM resolved_path)
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        "#;

        let result: String = client.select(tables_query, None, None)
            .expect("쿼리 실행 실패")
            .map(|row| {
                // 각 행을 튜플로 변환
                row.get_by_name::<String, _>("table_name")
                        .expect("table_name not found")
                        .unwrap()
            })
            .map(|table_name|{
                // 튜플을 벡터로 변환
                let column_query = r#"
                    SELECT column_name::text, data_type::text, is_nullable::text
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                    AND table_name = $1
                    ORDER BY ordinal_position
                "#;
                let columns = client.select(
                    column_query,
                    None, 
                    Some(vec![(PgOid::from(pg_sys::TEXTOID), table_name.clone().into_datum())]),  // $1 바인딩
                ).expect("쿼리 실행 실패");
                
                let columns_info = columns.map(|col_row| {
                        let column_name: String = col_row
                            .get_by_name("column_name")
                            .expect("column_name not found")
                            .unwrap();

                        let data_type: String = col_row
                            .get_by_name("data_type")
                            .expect("data_type not found")
                            .unwrap();

                        let is_nullable: String = col_row
                            .get_by_name("is_nullable")
                            .expect("is_nullable not found")
                            .unwrap();

                        (column_name, data_type, is_nullable)
                    })
                    .collect::<Vec<(String, String, String)>>();

                (table_name, columns_info)
            })
            .map(|(table_name, columns_info)| {
                // 테이블/컬럼 정보를 "CREATE TABLE" 형태로 포매팅
                format_postgresql_create_table(&table_name, &columns_info)
            })
            .collect::<Vec<String>>()
            .join("\n\n");
        result
    });
    result
}

/// 실제 "CREATE TABLE ..." 문자열을 만들어 주는 유틸 함수
fn format_postgresql_create_table(
    table_name: &str,
    columns_info: &[(String, String, String)],
) -> String {
    let mut lines = Vec::new();

    // 첫 줄
    lines.push(format!("CREATE TABLE {}\n(", table_name));

    for (i, (column_name, data_type, is_nullable)) in columns_info.iter().enumerate() {
        // nullable 여부
        let null_status = if is_nullable == "YES" {
            "NULL"
        } else {
            "NOT NULL"
        };

        let postgres_data_type = data_type.to_ascii_uppercase();

        let mut column_line = format!("    {} {} {}", column_name, postgres_data_type, null_status);

        if i < columns_info.len() - 1 {
            column_line.push(',');
        }

        lines.push(column_line);
    }

    lines.push(");".to_owned());
    lines.join("\n")
}
