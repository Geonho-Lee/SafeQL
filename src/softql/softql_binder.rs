use softql::ast::*;
use serde::de::value::Error;
use serde::de::Error as SerdeDeErrorTrait;
use super::*;
use super::softql_resolver::*;
use super::protobuf::node::Node as NodeOneof;
use super::protobuf::Node;

/// SoftQLQuery → PostgreSQL RawStmt
pub fn bind_softql(query: &SoftQLQuery) -> Result<protobuf::RawStmt, Error> {
    // 1) 기본 SelectStmt 생성
    let mut stmt = protobuf::SelectStmt {
        limit_option: protobuf::LimitOption::Count as i32,
        op: protobuf::SetOperation::SetopNone as i32,
        ..Default::default()
    };

    // 2) FROM 절: 초기 테이블
    let rv0 = resolve_rangevar(query.initial_table.clone()).ok_or_else(|| {
        SerdeDeErrorTrait::custom(format!(
            "Unknown relation in FROM: {}",
            query.initial_table
        ))
    })?;
    stmt.from_clause.push(Node {
        node: Some(NodeOneof::RangeVar(rv0)),
    });

    // 3) 연산자 순회
    for op in &query.operations {
        match op {
            Operator::Join(j) => {
                // a) 조인 테이블 추가
                let rvj = resolve_rangevar(j.table.clone()).ok_or_else(|| {
                    SerdeDeErrorTrait::custom(format!(
                        "Unknown join relation: {}",
                        j.table
                    ))
                })?;
                stmt.from_clause.push(Node {
                    node: Some(NodeOneof::RangeVar(rvj)),
                });
                // b) ON 절(predicate) → WHERE로 처리
                if let Some(pred) = &j.predicate {
                    let be = build_predicate_node(pred)?;
                    stmt.where_clause = Some(Box::new(be));
                }
            }
            Operator::Filter(pred) => {
                // WHERE 절
                let be = build_predicate_node(pred)?;
                stmt.where_clause = Some(Box::new(be));
            }
            Operator::Group(expr) => {
                let node = match expr {
                    Expression::FunctionCall(f) => build_func_call_node(f)?,
                    _ => build_expr_node(expr)?,
                };
                stmt.group_clause.push(node);
            }
            Operator::Having(pred) => {
                stmt.having_clause = Some(Box::new(build_predicate_node(pred)?));
            }

            Operator::Aggregate(es) | Operator::Project(es) => {
                // SELECT 컬럼
                for e in es {
                    let rt = build_res_target(e)?;
                    stmt.target_list.push(rt);
                }
            }
            Operator::Order(es) => {
                // ORDER BY도 원본 코드에선 target_list에 쌓으므로 동일하게 처리
                for e in es {
                    // 1) Expression → 정렬용 Node 생성
                    let sort_node = match e {
                        Expression::FunctionCall(inner_f) => build_func_call_node(inner_f)?,
                        _ => build_expr_node(e)?,
                    };
                    
                    // 2) SortBy 메시지 생성 (기본: ASC, NULLS LAST)
                    let sb = protobuf::SortBy {
                        node: Some(Box::new(sort_node)),
                        sortby_dir: protobuf::SortByDir::SortbyAsc as i32,
                        sortby_nulls: protobuf::SortByNulls::SortbyNullsLast as i32,
                        use_op: Vec::new(),
                        location: 0,
                    };

                    // 3) sort_clause 에 추가
                    stmt.sort_clause.push(Node {
                        node: Some(NodeOneof::SortBy(Box::new(sb))),
                    }                );
                }
            }
            Operator::Limit(nstr) => {
                let num_expr = Expression::NumberLiteral(nstr.clone());
                let limit_node = build_expr_node(&num_expr)?;
                stmt.limit_count = Some(Box::new(limit_node));
            }
        }
    }

    // 4) RawStmt 래핑
    let select_node = Node {
        node: Some(NodeOneof::SelectStmt(Box::new(stmt))),
    };
    Ok(protobuf::RawStmt {
        stmt: Some(Box::new(select_node)),
        stmt_location: -1,
        stmt_len: 0,
    })
}

/// Expression 또는 FunctionCall → ResTarget 노드
fn build_res_target(e: &Expression) -> Result<Node, Error> {
    let inner = match e {
        Expression::FunctionCall(f) => build_func_call_node(f)?,
        _ => build_expr_node(e)?,
    };
    let alias = infer_alias(e);
    let rt = protobuf::ResTarget {
        name: alias,
        indirection: Vec::new(),
        val: Some(Box::new(inner)),
        location: 0,
    };
    Ok(Node {
        node: Some(NodeOneof::ResTarget(Box::new(rt))),
    })
}

/// PredicateExpr → BoolExpr 또는 AConst 노드
fn build_predicate_node(p: &PredicateExpr) -> Result<Node, Error> {
    match p {
        PredicateExpr::And(l, r) => {
            let args = vec![build_predicate_node(l)?, build_predicate_node(r)?];
            let be = protobuf::BoolExpr {
                xpr: None,
                boolop: protobuf::BoolExprType::AndExpr as i32,
                args,
                location: 0,
            };
            Ok(Node {
                node: Some(NodeOneof::BoolExpr(Box::new(be))),
            })
        }
        PredicateExpr::Or(l, r) => {
            let args = vec![build_predicate_node(l)?, build_predicate_node(r)?];
            let be = protobuf::BoolExpr {
                xpr: None,
                boolop: protobuf::BoolExprType::OrExpr as i32,
                args,
                location: 0,
            };
            Ok(Node {
                node: Some(NodeOneof::BoolExpr(Box::new(be))),
            })
        }
        PredicateExpr::Not(inner) => {
            let args = vec![build_predicate_node(inner)?];
            let be = protobuf::BoolExpr {
                xpr: None,
                boolop: protobuf::BoolExprType::NotExpr as i32,
                args,
                location: 0,
            };
            Ok(Node {
                node: Some(NodeOneof::BoolExpr(Box::new(be))),
            })
        }
        PredicateExpr::FuncCall(f) => {
            // 함수호출도 Expression처럼 처리
            build_func_call_node(f)
        }
        PredicateExpr::BoolLiteral(b) => {
            let a = protobuf::AConst {
                isnull: false,
                location: 0,
                val: resolve_boolval(*b),
            };
            Ok(Node {
                node: Some(NodeOneof::AConst(a)),
            })
        }
    }
}

/// Expression → ColumnRef, AConst 노드
fn build_expr_node(e: &Expression) -> Result<Node, Error> {
    match e {
        Expression::TableField(rel, fld) => {
            let (schema, table) = resolve_relname((*rel).clone()).ok_or_else(|| {
                SerdeDeErrorTrait::custom(format!(
                    "Unknown relation name in Expression::TableField: {}",
                    rel
                ))
            })?;
            let field = resolve_fieldname(schema.clone(), table.clone(), (*fld).clone()).ok_or_else(|| {
                SerdeDeErrorTrait::custom(format!(
                    "Unknown field '{}' in {}.{}",
                    fld, schema, table
                ))
            })?;
            let cref = protobuf::ColumnRef {
                fields: vec![
                    Node {
                        node: Some(NodeOneof::String(protobuf::String {
                            sval: schema.clone(),
                        })),
                    },
                    Node {
                        node: Some(NodeOneof::String(protobuf::String {
                            sval: table.clone(),
                        })),
                    },
                    Node {
                        node: Some(NodeOneof::String(protobuf::String {
                            sval: field.clone(),
                        })),
                    },
                ],
                location: 0,
            };
            Ok(Node {
                node: Some(NodeOneof::ColumnRef(cref)),
            })
        }
        Expression::StringLiteral(s) => {
            let a = protobuf::AConst {
                isnull: false,
                location: 0,
                val: resolve_sval(s.clone()),
            };
            Ok(Node {
                node: Some(NodeOneof::AConst(a)),
            })
        }
        Expression::NumberLiteral(n) => {
            if let Ok(ival) = n.parse::<i32>() {
                let a = protobuf::AConst {
                    isnull: false,
                    location: 0,
                    val: resolve_ival(ival),
                };
                Ok(Node {
                    node: Some(NodeOneof::AConst(a)),
                })
            } else if let Ok(fval) = n.parse::<f64>() {
                let a = protobuf::AConst {
                    isnull: false,
                    location: 0,
                    val: resolve_fval(fval),
                };
                Ok(Node {
                    node: Some(NodeOneof::AConst(a)),
                })
            } else {
                panic!("Invalid numeric literal: {}", n);
            }
        }
        Expression::BoolLiteral(b) => {
            let a = protobuf::AConst {
                isnull: false,
                location: 0,
                val: resolve_boolval(*b),
            };
            Ok(Node {
                node: Some(NodeOneof::AConst(a)),
            })
        }
        Expression::NullLiteral => {
            let a = protobuf::AConst {
                isnull: true,
                location: 0,
                val: None,
            };
            Ok(Node {
                node: Some(NodeOneof::AConst(a)),
            })
        }
        Expression::FunctionCall(_) => {
            unreachable!("FunctionCall should be handled in build_res_target or build_predicate_node")
        }
    }
}

fn build_func_call_node(f: &FunctionCall) -> Result<Node, Error> {
    // --- (1) build children first ------------------------------------------------
    let mut args = Vec::with_capacity(f.args.len());
    for a in &f.args {
        args.push(match a {
            Expression::FunctionCall(inner) => build_func_call_node(inner)?,
            _ => build_expr_node(a)?,
        });
    }

    match resolve_call(&f.name) {
        /* ---------- 1) 연산자 ---------- */
        Some(ResolvedCall::Operator { symbol, kind }) => {
            let name_vec = vec![Node {
                node: Some(NodeOneof::String(protobuf::String { sval: symbol.clone() })),
            }];
    
            let ax = match (kind, args.len()) {
                ('b', 2) => protobuf::AExpr {
                    kind: protobuf::AExprKind::AexprOp as i32,
                    name: name_vec,
                    lexpr: Some(Box::new(args[0].clone())),
                    rexpr: Some(Box::new(args[1].clone())),
                    location: 0,
                },
                ('l', 1) => protobuf::AExpr {
                    kind: protobuf::AExprKind::AexprOp as i32,
                    name: name_vec,
                    lexpr: None,
                    rexpr: Some(Box::new(args[0].clone())),
                    location: 0,
                },
                _ => unreachable!(
                    "operator {:?} supports arity {:?}, got {} args",
                    symbol, kind, args.len()
                ),
            };
            return Ok(Node { node: Some(NodeOneof::AExpr(Box::new(ax))) });
        }
    
        /* ---------- 2) 일반 함수 ---------- */
        Some(ResolvedCall::Function { schema, func }) => {
            let pgf = protobuf::FuncCall {
                funcname: vec![
                    Node {
                        node: Some(NodeOneof::String(protobuf::String { sval: schema.clone() })),
                    },
                    Node {
                        node: Some(NodeOneof::String(protobuf::String { sval: func.clone() })),
                    },
                ],
                args,
                agg_order: Vec::new(),
                agg_filter: None,
                over: None,
                agg_within_group: false,
                agg_star: false,
                agg_distinct: false,
                func_variadic: false,
                funcformat: protobuf::CoercionForm::CoerceExplicitCall as i32,
                location: 0,
            };
            return Ok(Node { node: Some(NodeOneof::FuncCall(Box::new(pgf))) });
        }
    
        /* ---------- 3) 미해결 ---------- */
        None => {
            return Err(SerdeDeErrorTrait::custom(format!(
                "Unknown operator/function: {}",
                f.name
            )));
        }
    }
}

/// ResTarget 이름(pseudonym) 추론
fn infer_alias(e: &Expression) -> String {
    match e {
        Expression::TableField(_, f) => f.clone(),
        Expression::StringLiteral(s) => s.clone(),
        Expression::NumberLiteral(n) => n.clone(),
        Expression::BoolLiteral(b) => b.to_string(),
        Expression::NullLiteral => "null".into(),
        Expression::FunctionCall(f) => f.name.clone(),
    }
}
