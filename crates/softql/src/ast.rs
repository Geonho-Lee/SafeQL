use serde::Deserialize;

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct SoftQLQuery {
    pub initial_table: String,
    pub operations: Vec<Operator>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub enum Operator {
    Join(JoinClause),
    Filter(PredicateExpr), // equivalent to `.where()`
    Group(Expression),
    Having(PredicateExpr),
    Aggregate(Vec<Expression>),
    Project(Vec<Expression>),
    Order(Vec<Expression>),
    Limit(String), // store as string first, you can parse to usize later
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct JoinClause {
    pub table: String,
    pub predicate: Option<PredicateExpr>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub enum PredicateExpr {
    And(Box<PredicateExpr>, Box<PredicateExpr>),
    Or(Box<PredicateExpr>, Box<PredicateExpr>),
    Not(Box<PredicateExpr>),
    FuncCall(FunctionCall),
    BoolLiteral(bool),
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expression>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub enum Expression {
    FunctionCall(FunctionCall),
    TableField(String, String),
    StringLiteral(String),
    NumberLiteral(String),
    BoolLiteral(bool),
    NullLiteral,
}

