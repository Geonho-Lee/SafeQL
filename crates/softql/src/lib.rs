// src/lib.rs
pub mod ast;
pub mod parser;

// re-export for convenience
pub use parser::static_parse_softql;

#[cfg(test)]
mod tests {
    use super::*; // static_parse_softql, etc.

    // ────────────── 기본 동작 ──────────────
    #[test]
    fn test_basic_query() {
        let ast = static_parse_softql("customers").unwrap();
        assert_eq!(ast.initial_table, "customers");
        assert!(ast.operations.is_empty());
    }

    // ────────────── PROJECT ──────────────
    #[test]
    fn test_project_column() {
        let ast = static_parse_softql(r#"customers.project(customers.a)"#).unwrap();
        assert_eq!(ast.initial_table, "customers");
        assert_eq!(ast.operations.len(), 1);
    }

    // ────────────── WHERE / 숫자 리터럴 ──────────────
    #[test]
    fn test_number_literal_in_where() {
        let ast = static_parse_softql(
            r#"customers.where(greater(customers.a, 400))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_negative_number_literal() {
        let ast = static_parse_softql(
            r#"customers.where(greater(customers.value, -123.45))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    // ────────────── 중첩 함수 호출 ──────────────
    #[test]
    fn test_nested_function_call() {
        let ast = static_parse_softql(
            r#"customers.aggregate(calcSum(convertToNumber(customers.amount)))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_multi_arg_function_call() {
        let ast = static_parse_softql(
            r#"customers.where(equals(customers.a, customers.b, customers.c, customers.d))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    // ────────────── 오류 케이스 ──────────────
    #[test]
    fn test_unmatched_parenthesis_error() {
        // logical_expr (a AND (b OR c  <= 닫히지 않음
        let input = r#"
            customers.where(a AND (b OR c)
        "#;
        assert!(
            static_parse_softql(input).is_err(),
            "Should fail due to unmatched parenthesis"
        );
    }

    #[test]
    fn test_empty_input() {
        assert!(static_parse_softql("").is_err());
    }

    // ────────────── 다양한 리터럴 ──────────────
    #[test]
    fn test_null_literal_in_expression() {
        let ast = static_parse_softql(
            r#"customers.project(null, 123, "hello", true)"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_null_literal_in_function_args() {
        let ast = static_parse_softql(
            r#"customers.where(equals(customers.column, null))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_logical_expr_missing_rhs() {
        assert!(static_parse_softql(
            r#"customers.where(equals(customers.id, 10) AND )"#
        )
        .is_err());
    }

    #[test]
    fn test_single_quote_string() {
        let ast = static_parse_softql(
            r#"test.where(equals(test.virtual, 'F')).where(equals(test.virtual, "F"))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 2);
    }

    // ────────────── JOIN + WHERE ──────────────
    #[test]
    fn test_join() {
        let ast = static_parse_softql(
            r#"
              customers
                .join(yearmonth, equals(customers.id, yearmonth.id, yearmonth.value, yearmonth.value, yearmonth.value))
                .join(yearmonth, equals(customers.id, yearmonth.id))
                .where(greater(customers.amount, 100))
            "#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 3);
    }

    // ────────────── AGG / PROJECT / ORDER ──────────────
    #[test]
    fn test_aggregate_single() {
        let ast = static_parse_softql("customers.aggregate('a')").unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_project() {
        let ast = static_parse_softql(
            r#"customers.project(calcSum(customers.amount))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_project_multiple() {
        let ast = static_parse_softql(
            r#"customers.project(calcSum(customers.amount), calcSum(customers.amount))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    // where → aggregate
    #[test]
    fn test_where_then_aggregate() {
        let ast = static_parse_softql(
            r#"customers.where(greater(customers.amount, 100)).aggregate(calcSum(customers.amount))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 2);
    }

    // join → where → aggregate
    #[test]
    fn test_join_where_aggregate() {
        let ast = static_parse_softql(
            r#"
              customers
                .join(yearmonth, equals(customers.id, yearmonth.id))
                .where(greater(yearmonth.value, 100) AND equals(customers.active, "Y"))
                .aggregate(calcSum(customers.amount))
            "#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 3);
    }

    #[test]
    fn test_complex_bracket_logical_expr() {
        let ast = static_parse_softql(
            r#"
              customers.where(
                (NOT equals(customers.a, 1)) 
                OR 
                (equals(customers.b, 2) AND equals(customers.c, 3))
              )
            "#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_multiple_and_expressions() {
        let ast = static_parse_softql(
            r#"
              customers.where(
                  equals(status.statusid, 2) 
                  AND notEquals(results.time, null) 
                  AND greater(results.raceid, 50) 
                  AND less(results.raceid, 100)
              )
            "#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_boolean_literal_in_where() {
        let ast = static_parse_softql(
            r#"customers.where(true).where(NOT false)"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 2);
    }

    // ────────────── GROUP / HAVING / LIMIT ──────────────
    #[test]
    fn test_group_and_having() {
        let ast = static_parse_softql(
            r#"customers.group(customers.country).having(greater(count(customers.id), 10))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 2); // group + having
    }

    #[test]
    fn test_limit() {
        let ast = static_parse_softql("customers.limit(100)").unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_full_chain_group_having_limit() {
        let ast = static_parse_softql(
            r#"
              customers
                .where(equals(region.id, 1))
                .group(region.name)
                .having(greater(sum(customers.amount), 1000))
                .aggregate(sum(customers.amount))
                .project(region.name, sum(customers.amount))
                .order(sum(customers.amount))
                .limit(50)
            "#,
        )
        .unwrap();
        // where, group, having, aggregate, project, order, limit = 7
        assert_eq!(ast.operations.len(), 7);
    }

    // ────────────── 기타 ──────────────
    #[test]
    fn test_aggregate_trailing_comma() {
        assert!(static_parse_softql(
            "customers.aggregate(calcSum(a), calcSum(b),)"
        )
        .is_err());
    }

    #[test]
    fn test_order_then_limit_chain() {
        // order() 다음에 limit() 연달아 호출
        let ast = static_parse_softql(
            r#"customers.order(customers.id).limit(10)"#,
        )
        .unwrap();
        // order + limit = 2 operations
        assert_eq!(ast.operations.len(), 2);
    }

    #[test]
    fn test_limit_negative_number_literal() {
        // grammar 상 number_literal 은 부호(-)를 허용하므로 limit(-1)도 파싱 가능
        let ast = static_parse_softql(r#"customers.limit(-1)"#).unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_zero_arg_function_call() {
        // 인자가 없는 함수 random() --> function_args? 가 None 인 경우
        let ast = static_parse_softql(r#"customers.project(random())"#).unwrap();
        assert_eq!(ast.operations.len(), 1);
    }

    #[test]
    fn test_double_nested_not() {
        // NOT (NOT …) 과 같이 중첩된 NOT 연산 확인
        let ast = static_parse_softql(
            r#"customers.where(NOT (NOT equals(customers.a, 1)))"#,
        )
        .unwrap();
        assert_eq!(ast.operations.len(), 1);
    }
}
