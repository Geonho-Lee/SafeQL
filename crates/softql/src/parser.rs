use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use crate::ast::*; // SoftQLQuery, Operator, …

// ──────────────────────────────
// pest parser definition
// ──────────────────────────────
#[derive(Parser)]
#[grammar = "softql.pest"]
pub struct SoftQLParser;

// ──────────────────────────────
// public entry‐point
// ──────────────────────────────
pub fn static_parse_softql(input: &str) -> Result<SoftQLQuery, pest::error::Error<Rule>> {
    let mut pairs = SoftQLParser::parse(Rule::softql, input)?;
    let softql_pair = pairs
        .next()
        .expect("Expected top-level softql rule to yield one pair");
    Ok(build_query(softql_pair))
}

// ──────────────────────────────
// softql  =  SOI ~ query ~ EOI
// query   =  identifier (“.” operator_call)*
// ──────────────────────────────
fn build_query(pair: Pair<Rule>) -> SoftQLQuery {
    debug_assert_eq!(pair.as_rule(), Rule::softql);
    let mut inner = pair.into_inner(); // → [ query ]

    let query_pair = inner
        .next()
        .expect("softql must contain exactly one query pair");
    build_query_inner(query_pair)
}

fn build_query_inner(pair: Pair<Rule>) -> SoftQLQuery {
    debug_assert_eq!(pair.as_rule(), Rule::query);
    let mut inner = pair.into_inner();

    // first identifier - the anchor table
    let initial_table = inner
        .next()
        .expect("query must start with identifier")
        .as_str()
        .to_owned();

    // remaining children are operator_call pairs
    let mut operations = Vec::<Operator>::new();
    for op_pair in inner {
        // dotted “.” literals do NOT appear here – only operator_call
        operations.push(build_operator(op_pair));
    }

    SoftQLQuery {
        initial_table,
        operations,
    }
}

// ──────────────────────────────
// operator_call dispatcher
// ──────────────────────────────
fn build_operator(pair: Pair<Rule>) -> Operator {
    match pair.as_rule() {
        Rule::join_call => Operator::Join(build_join_clause(pair)),
        Rule::where_call => Operator::Filter(build_where_or_having(pair)),
        Rule::group_call => Operator::Group(build_group_clause(pair)),
        Rule::having_call => Operator::Having(build_where_or_having(pair)),
        Rule::aggregate_call => Operator::Aggregate(build_multi_expression_clause(pair)),
        Rule::project_call => Operator::Project(build_multi_expression_clause(pair)),
        Rule::order_call => Operator::Order(build_multi_expression_clause(pair)),
        Rule::limit_call => Operator::Limit(build_limit_clause(pair)),
        _ => unreachable!("Unhandled operator rule"),
    }
}

// ──────────────────────────────
// JOIN
// join("(" ws* table_reference ("," predicate)? ws* ")")
// ──────────────────────────────
fn build_join_clause(pair: Pair<Rule>) -> JoinClause {
    let mut inner = pair.into_inner();
    let table = inner
        .next()
        .expect("join requires table_reference")
        .as_str()
        .to_owned();

    let predicate = inner.next().map(build_predicate);

    JoinClause { table, predicate }
}

// ──────────────────────────────
// WHERE / HAVING   → PredicateExpr
// ──────────────────────────────
fn build_where_or_having(pair: Pair<Rule>) -> PredicateExpr {
    // pair inner: predicate
    let pred_pair = pair
        .into_inner()
        .next()
        .expect("where/having call must contain predicate");
    build_predicate(pred_pair)
}

// ──────────────────────────────
// GROUP      → single Expression
// ──────────────────────────────
fn build_group_clause(pair: Pair<Rule>) -> Expression {
    let expr_pair = pair
        .into_inner()
        .next()
        .expect("group requires one expression");
    build_expression(expr_pair)
}

// ──────────────────────────────
// AGG / PROJECT / ORDER  → Vec<Expression>
// ──────────────────────────────
fn build_multi_expression_clause(pair: Pair<Rule>) -> Vec<Expression> {
    let mex_pair = pair
        .into_inner()
        .next()
        .expect("clause must contain multi_expressions");
    build_multi_expressions(mex_pair)
}

// ──────────────────────────────
// LIMIT       → String (number literal)
// ──────────────────────────────
fn build_limit_clause(pair: Pair<Rule>) -> String {
    pair.into_inner()
        .next()
        .expect("limit requires number_literal")
        .as_str()
        .to_owned()
}

// ──────────────────────────────
// PREDICATES  (or_expr, and_expr, unary …)
// ──────────────────────────────
fn build_predicate(pair: Pair<Rule>) -> PredicateExpr {
    debug_assert_eq!(pair.as_rule(), Rule::predicate);
    let inner = pair.into_inner().next().unwrap(); // or_expr
    build_or_expr(inner)
}

fn build_or_expr(pair: Pair<Rule>) -> PredicateExpr {
    let mut inner = pair.into_inner();
    let mut expr = build_and_expr(inner.next().unwrap());

    for and_pair in inner {
        let rhs = build_and_expr(and_pair);
        expr = PredicateExpr::Or(Box::new(expr), Box::new(rhs));
    }
    expr
}

fn build_and_expr(pair: Pair<Rule>) -> PredicateExpr {
    let mut inner = pair.into_inner();
    let mut expr = build_unary_expr(inner.next().unwrap());

    for uni in inner {
        let rhs = build_unary_expr(uni);
        expr = PredicateExpr::And(Box::new(expr), Box::new(rhs));
    }
    expr
}

fn build_unary_expr(pair: Pair<Rule>) -> PredicateExpr {
    match pair.as_rule() {
        Rule::unary_expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().unwrap();
            match first.as_rule() {
                Rule::NOT => {
                    let rhs = inner.next().expect("NOT must be followed by expr");
                    PredicateExpr::Not(Box::new(build_unary_expr(rhs)))
                }
                Rule::or_expr => build_or_expr(first),
                Rule::condition => build_condition(first),
                _ => unreachable!(),
            }
        }
        Rule::condition => build_condition(pair),
        _ => unreachable!("unexpected unary_expr variant"),
    }
}

fn build_condition(pair: Pair<Rule>) -> PredicateExpr {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::function_call => PredicateExpr::FuncCall(build_function_call(inner)),
        Rule::boolean_literal => {
            let v = inner.as_str().eq_ignore_ascii_case("true");
            PredicateExpr::BoolLiteral(v)
        }
        _ => unreachable!("condition expected function_call|boolean_literal"),
    }
}

// ──────────────────────────────
// EXPRESSIONS
// ──────────────────────────────
fn build_multi_expressions(pair: Pair<Rule>) -> Vec<Expression> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::expression)
        .map(build_expression)
        .collect()
}

fn build_expression(pair: Pair<Rule>) -> Expression {
    debug_assert_eq!(pair.as_rule(), Rule::expression);
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::function_call => Expression::FunctionCall(build_function_call(inner)),
        Rule::table_field => {
            let mut idents = inner.into_inner();
            let tbl = idents.next().unwrap().as_str().to_owned();
            let col = idents.next().unwrap().as_str().to_owned();
            Expression::TableField(tbl, col)
        }
        Rule::string_literal => {
            let raw = inner.as_str();
            let unquoted = &raw[1..raw.len() - 1]; // strip ' / "
            Expression::StringLiteral(unquoted.to_owned())
        }
        Rule::number_literal => Expression::NumberLiteral(inner.as_str().to_owned()),
        Rule::boolean_literal => Expression::BoolLiteral(inner.as_str().eq_ignore_ascii_case("true")),
        Rule::null_literal => Expression::NullLiteral,
        _ => unreachable!("unexpected expression child"),
    }
}

// ──────────────────────────────
// FUNCTION CALL
// ──────────────────────────────
fn build_function_call(pair: Pair<Rule>) -> FunctionCall {
    let mut inner = pair.into_inner();
    let name = inner
        .next()
        .expect("function_call missing identifier")
        .as_str()
        .to_owned();

    let mut args = Vec::<Expression>::new();
    if let Some(arg_list) = inner.next() {
        // function_args
        for expr_pair in arg_list.into_inner().filter(|p| p.as_rule() == Rule::expression) {
            args.push(build_expression(expr_pair));
        }
    }

    FunctionCall { name, args }
}
