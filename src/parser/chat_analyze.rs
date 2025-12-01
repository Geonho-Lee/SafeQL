use std::ffi::c_void;
use pgrx::{memcx, pg_sys, FromDatum};
use pgrx::list::List;
use pgrx::error;
use pgrx::nodes::node_to_string;
use pgrx::prelude::pg_extern;
use pgrx::pg_sys::parse_analyze_fixedparams;
use crate::utils::{catalog, schema};
use crate::gucs::model::text2softql_options;
use crate::safeql::{analyze_with_refinement, perform_refinement_search};
use crate::softql::{parse_softql, deparse_raw_stmt};
use text2softql::text2softql;



/// `pstate`가 최상위 ParseState 인지 검사
#[inline]
fn pstate_is_top_level(pstate: *mut pg_sys::ParseState) -> bool {
    unsafe { (*pstate).parentParseState.is_null() }
}


#[repr(C)]
pub struct SoftqlCtx {
    flagged: *mut pg_sys::List,   // rtindex 들을 담을 List<int>
}

fn ensure_ctx(pstate: *mut pg_sys::ParseState) -> *mut SoftqlCtx {
    unsafe {
        if (*pstate).p_ref_hook_state.is_null() {
            // CurrentMemoryContext = pstate 가 속한 parse context
            let ctx = pg_sys::palloc0(size_of::<SoftqlCtx>()) as *mut SoftqlCtx;
            (*ctx).flagged = std::ptr::null_mut() as *mut pg_sys::List;      // 빈 리스트
            (*pstate).p_ref_hook_state = ctx as *mut _;
        }
        (*pstate).p_ref_hook_state as *mut SoftqlCtx
    }    
}

#[pg_extern(create_or_replace)]
fn print_softql(query_string: &str) -> String {
    // Parse the input string into a SoftQL AST
    let ast = parse_softql(query_string).unwrap() as *mut pg_sys::Node;

    unsafe { node_to_string(ast) }.unwrap().to_string()
}

#[pg_extern(create_or_replace)]
fn softql_to_sql(query_string: &str) -> String {
    // Parse the input string into a SoftQL AST
    let ast = parse_softql(query_string).unwrap();
    return deparse_raw_stmt(ast);
}

/// SafeQL refinement를 수행하고 refined SQL string을 반환
#[pg_extern(create_or_replace)]
pub fn safeql_to_sql(sql: &str) -> String {
    // ParseState 생성
    let pstate = unsafe { pg_sys::make_parsestate(std::ptr::null_mut()) };
    
    // Refinement 수행
    let refined_raw = perform_refinement_search(sql, pstate);
    
    // ParseState 정리
    unsafe { pg_sys::free_parsestate(pstate) };
    
    // RawStmt를 SQL string으로 변환
    deparse_raw_stmt(refined_raw)
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn convert_chat_walker(
    _node: *mut pg_sys::Node, 
    _pstate: *mut c_void
) -> bool {
    // if node is not exists
    if _node.is_null() {
        return false;
    }


    unsafe {
        match (*_node).type_ {
            pg_sys::NodeTag::T_RangeTblEntry => {
                let _rte = _node as *mut pg_sys::RangeTblEntry;

                match (*_rte).rtekind {
                    pg_sys::RTEKind::RTE_SUBQUERY => {
                        convert_chat_walker((*_rte).subquery as *mut pg_sys::Node, _pstate);
                    },
                    pg_sys::RTEKind::RTE_FUNCTION => {
                        if is_rte_chat(_rte) {
                            let ctx = ensure_ctx(_pstate as *mut pg_sys::ParseState);
                            let pos = current_rtindex(_pstate as *mut pg_sys::ParseState, _rte);
                            convert_chat_to_subquery(_rte, _pstate as *mut pg_sys::ParseState);
                            (*ctx).flagged = pg_sys::lappend_int((*ctx).flagged, pos);
                        }
                        else if is_rte_softql(_rte) {
                            let ctx = ensure_ctx(_pstate as *mut pg_sys::ParseState);
                            let pos = current_rtindex(_pstate as *mut pg_sys::ParseState, _rte);
                            convert_softql_to_subquery(_rte, _pstate as *mut pg_sys::ParseState);
                            (*ctx).flagged = pg_sys::lappend_int((*ctx).flagged, pos);
                        }
                        else if is_rte_safeql(_rte) {
                            let ctx = ensure_ctx(_pstate as *mut pg_sys::ParseState);
                            let pos = current_rtindex(_pstate as *mut pg_sys::ParseState, _rte);
                            convert_safeql_to_subquery(_rte, _pstate as *mut pg_sys::ParseState);
                            (*ctx).flagged = pg_sys::lappend_int((*ctx).flagged, pos);
                        }
                        
                        else {
                            return false;
                        }
                        return false;
                    },
                    _ => {
                        return false;
                    }
            
                }
            },
            pg_sys::NodeTag::T_RangeTblFunction => {
                let _rtfunc = _node as *mut pg_sys::RangeTblFunction;
                let funcexpr = (*_rtfunc).funcexpr as *mut pg_sys::FuncExpr;

                // todo!("handle chat function in RangeTblFunction");
                return pg_sys::expression_tree_walker(
                    funcexpr as *mut pg_sys::Node,
                    Some(convert_chat_walker),
                    _pstate as *mut c_void,
                );
            }
            pg_sys::NodeTag::T_Query => {
                if pstate_is_top_level(_pstate as *mut pg_sys::ParseState) {
                    (*(_pstate as *mut pg_sys::ParseState)).p_ref_hook_state = std::ptr::null_mut();
                }
                let flags = pg_sys::QTW_EXAMINE_RTES_BEFORE | pg_sys::QTW_IGNORE_RT_SUBQUERIES | pg_sys::QTW_IGNORE_JOINALIASES;
                let result =  pg_sys::query_tree_walker(
                    _node as *mut pg_sys::Query,
                    Some(convert_chat_walker),
                    _pstate as *mut c_void,
                    flags as i32,
                );
                if pstate_is_top_level(_pstate as *mut pg_sys::ParseState) {
                    rewrite_projection(_node as *mut pg_sys::Query, _pstate as *mut pg_sys::ParseState);
                }

                // todo!("handle explain analyze");
                return result;
            },
            _ => {
                // todo!("handle other node types e.g., FuncExpr, RangeTblFunction");
                return false;
            }
        }
    }

    return false;
}


unsafe fn current_rtindex(
    pstate: *mut pg_sys::ParseState,
    target_rte: *mut pg_sys::RangeTblEntry,
) -> i32 {
    memcx::current_context(|mcx| {
        /* ① p_rtable 리스트를 downcast */
        let rtes = match unsafe { List::<*mut c_void>::downcast_ptr_in_memcx(
            (*pstate).p_rtable,
            mcx,
        ) } {
            Some(v) => v,
            None => return 0,        // FROM 절이 비어 있음
        };

        /* ② 순회하며 동일 포인터를 찾는다 */
        for (idx, &ptr) in rtes.iter().enumerate() {
            if ptr as *mut pg_sys::RangeTblEntry == target_rte {
                return (idx + 1) as i32;   // ← 1-base
            }
        }
        unreachable!();
    })
}


fn is_rte_chat(
    _rte: *mut pg_sys::RangeTblEntry
) -> bool {
    let funcexpr = get_single_funcexpr_from_rte(_rte);
    if funcexpr.is_null() {
        return false;
    }

    let funcid = unsafe {
        (*funcexpr).funcid
    };

    return catalog::is_oid_vector_func(funcid, "chat") 
}


fn convert_chat_to_subquery(
    _rte: *mut pg_sys::RangeTblEntry,
    _pstate: *mut pg_sys::ParseState
) -> bool {
    
    if unsafe { (*_rte).funcordinality } {
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "Chat function cannot be used with ordinality"
        );
        return false;
    }

    let funcexpr = get_single_funcexpr_from_rte(_rte);
    assert!(!funcexpr.is_null());

    memcx::current_context(|mcx| {
        let args = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*funcexpr).args, mcx).unwrap()
        };
        if args.len() != 2 {
            pgrx::ereport!(
                pgrx::PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
                "Chat function must have exactly 2 arguments"
            );
            return false;
        }

        let arg1 = *args.get(0).unwrap() as *mut pg_sys::Node;
        let arg2 = *args.get(1).unwrap() as *mut pg_sys::Node;
        
        let schema = schema::show_all_tables_and_columns();
        let context = expr_get_const_str(arg1);
        let prompt = expr_get_const_str(arg2);

        pgrx::ereport!(
            pgrx::PgLogLevel::NOTICE,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            format!("Chat function schema: {}, context: {}, prompt: {}", schema, context, prompt)
        );

        let resp = match text2softql(schema, context, prompt, text2softql_options()) {
            Ok(r) => r,
            Err(e) => error!("{}", e.to_string()),
        };
        let softql = match resp.try_pop_softql() {
            Ok(softql) => softql,
            Err(e) => error!("{}", e.to_string()),
        };

        pgrx::ereport!(
            pgrx::PgLogLevel::NOTICE,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            format!("Chat function softql: {}", softql)
        );

        // parse softql 
        let softql_stmt = match parse_softql(&softql) {
            Ok(softql) => softql,
            Err(e) => error!("{}", e.to_string()),
        };

        // todo!("set error position");

        unsafe {
            (*_pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_FROM_SUBSELECT;
            (*_pstate).p_lateral_active = true;
        }

        // parse raw_stmt to Query

        let query = unsafe { 
                                    parse_analyze_fixedparams(
                                        softql_stmt,
                                        softql.as_ptr() as *const ::core::ffi::c_char,
                                        std::ptr::null_mut(),
                                        0,                                       
                                        std::ptr::null_mut()                                    ) 
                                };
        
        unsafe {
            (*_pstate).p_lateral_active = false; 
            (*_pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_NONE;
        }

        unsafe {
            (*_rte).rtekind = pg_sys::RTEKind::RTE_SUBQUERY;
            (*_rte).subquery = query;
        }

        return false;
    })
}

fn is_rte_softql(
    _rte: *mut pg_sys::RangeTblEntry
) -> bool {
    let funcexpr = get_single_funcexpr_from_rte(_rte);
    if funcexpr.is_null() {
        return false;
    }

    let funcid = unsafe {
        (*funcexpr).funcid
    };

    return catalog::is_oid_vector_func(funcid, "softql") 
}


fn convert_softql_to_subquery(
    _rte: *mut pg_sys::RangeTblEntry,
    _pstate: *mut pg_sys::ParseState
) -> bool {
    
    if unsafe { (*_rte).funcordinality } {
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "Softql function cannot be used with ordinality"
        );
        return false;
    }

    let funcexpr = get_single_funcexpr_from_rte(_rte);
    assert!(!funcexpr.is_null());

    memcx::current_context(|mcx| {
        let args = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*funcexpr).args, mcx).unwrap()
        };
        if args.len() != 1 {
            pgrx::ereport!(
                pgrx::PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
                "Spftq; function must have exactly 1 arguments"
            );
            return false;
        }

        let arg1 = *args.get(0).unwrap() as *mut pg_sys::Node;
        
        let softql = expr_get_const_str(arg1);

        pgrx::ereport!(
            pgrx::PgLogLevel::NOTICE,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            format!("Softql function: {}", softql)
        );

        // parse softql 
        let softql_stmt = match parse_softql(&softql) {
            Ok(softql) => softql,
            Err(e) => error!("{}", e.to_string()),
        };

        // todo!("set error position");

        unsafe {
            (*_pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_FROM_SUBSELECT;
            (*_pstate).p_lateral_active = true;
        }

        // parse raw_stmt to Query

        let query = unsafe { 
                                    parse_analyze_fixedparams(
                                        softql_stmt,
                                        softql.as_ptr() as *const ::core::ffi::c_char,
                                        std::ptr::null_mut(),
                                        0,                                       
                                        std::ptr::null_mut()                                    ) 
                                };
        
        // change coldeflist to targetlist type of query
        update_rtfunc_columns_types(_rte, query);

        unsafe {
            (*_pstate).p_lateral_active = false; 
            (*_pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_NONE;
        }

        unsafe {
            (*_rte).rtekind = pg_sys::RTEKind::RTE_SUBQUERY;
            (*_rte).subquery = query;
        }

        return false;
    })
}

fn is_rte_safeql(_rte: *mut pg_sys::RangeTblEntry) -> bool {
    let funcexpr = get_single_funcexpr_from_rte(_rte);
    if funcexpr.is_null() {
        return false;
    }
    let funcid = unsafe { (*funcexpr).funcid };
    return catalog::is_oid_vector_func(funcid, "safeql");
}

fn convert_safeql_to_subquery(
    _rte: *mut pg_sys::RangeTblEntry,
    _pstate: *mut pg_sys::ParseState,
) -> bool {
    if unsafe { (*_rte).funcordinality } {
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "SafeQL function cannot be used with ordinality"
        );
        return false;
    }

    let funcexpr = get_single_funcexpr_from_rte(_rte);
    assert!(!funcexpr.is_null());

    memcx::current_context(|mcx| {
        let args = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*funcexpr).args, mcx).unwrap()
        };
        if args.len() != 1 {
            pgrx::ereport!(
                pgrx::PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
                "SafeQL function must have exactly 1 argument"
            );
            return false;
        }

        let arg1 = *args.get(0).unwrap() as *mut pg_sys::Node;
        let sql = expr_get_const_str(arg1);

        pgrx::ereport!(
            pgrx::PgLogLevel::NOTICE,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            format!("SafeQL function: {}", sql)
        );

        let query = analyze_with_refinement(&sql, _pstate as *mut pg_sys::ParseState);

        unsafe {
            (*_pstate).p_lateral_active = false;
            (*_pstate).p_expr_kind = pg_sys::ParseExprKind::EXPR_KIND_NONE;
            (*_rte).rtekind = pg_sys::RTEKind::RTE_SUBQUERY;
            (*_rte).subquery = query;
        }

        return false;
    })
}


fn rewrite_projection(
    q: *mut pg_sys::Query,
    pstate: *mut pg_sys::ParseState
) {
    if q.is_null() {
        return;
    }

    /* ParseState 쪽 플래그 리스트 얻기 */
    let ctx = unsafe { (*pstate).p_ref_hook_state as *mut SoftqlCtx };
    if ctx.is_null() { return; }

    memcx::current_context(|mcx| {
        /* ─ 1. FROM 리스트 downcast ────────────────────────── */
        let rtes_raw = match unsafe { List::<*mut c_void>::downcast_ptr_in_memcx((*q).rtable, mcx) } {
            Some(v) => v,
            None => return, // 빈 FROM
        };
        let flagged_opt = unsafe { List::<i32>::downcast_ptr_in_memcx(
            (*ctx).flagged, mcx
        )};

        /* 새 targetList를 여기서 만든다 */
        let mut new_tlist: *mut pg_sys::List = std::ptr::null_mut();

        for (idx, &rte_ptr) in rtes_raw.iter().enumerate() {
            let rte = rte_ptr as *mut pg_sys::RangeTblEntry;
            let is_target = flagged_opt
                .as_ref()
                .map_or(false, |fl| fl.iter().any(|&n| n == (idx + 1) as i32));
            if !is_target {
                continue;
            }
            
            if unsafe { (*rte).rtekind == pg_sys::RTEKind::RTE_SUBQUERY } {
                let subq = unsafe { (*rte).subquery };
                if subq.is_null() {
                    continue;
                }

                /* ─ 2. subquery.targetList downcast ─────────── */
                let sub_raw = match unsafe { List::<*mut c_void>::downcast_ptr_in_memcx(
                    (*subq).targetList,
                    mcx,
                ) } {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };

                let ncols = sub_raw.len() as i32;
                if ncols == 0 {
                    continue;
                }

                /* ─ 3. alias / eref colnames & natts 재작성 ── */
                let mut names: *mut pg_sys::List = std::ptr::null_mut();
                for &te_ptr in sub_raw.iter() {
                    let te = te_ptr as *mut pg_sys::TargetEntry;
                    if unsafe { (*te).resjunk } {
                        continue;                     // junk 컬럼 건너뛰기
                    }
                    names = unsafe { pg_sys::lappend(
                        names,
                        pg_sys::makeString((*te).resname) as *mut _,
                    ) };
                }

                unsafe {
                    if !(*rte).alias.is_null() {
                        (*(*rte).alias).colnames = names;
                    }
                    (*(*rte).eref).colnames = names;
                }

                /* ─ 3. 바깥 Query.targetList 재구성 ─────────── */
                for (_subidx, &te_ptr) in sub_raw.iter().enumerate(){
                    let te = te_ptr as *mut pg_sys::TargetEntry;
                    if unsafe { (*te).resjunk } {
                        continue;                     // junk 컬럼 건너뛰기
                    }
                    let var = unsafe { pg_sys::makeVar(
                        (idx + 1) as i32,
                        (*te).resno as i16,
                        pg_sys::exprType((*te).expr as *mut pg_sys::Node),
                        pg_sys::exprTypmod((*te).expr as *mut pg_sys::Node),
                        pg_sys::exprCollation((*te).expr as *mut pg_sys::Node),
                        0,
                    ) };
                    let new_te = unsafe { pg_sys::makeTargetEntry(
                        var as *mut pg_sys::Expr,
                        (*te).resno as i16,
                        (*te).resname,
                        false,
                    ) };

                    new_tlist = unsafe { pg_sys::lappend(new_tlist, new_te as *mut _) };
                }
            }
        }

        /* 실제 softql RTE를 하나라도 찾았으면 타깃 리스트 교체 */
        if !new_tlist.is_null() {
            unsafe { (*q).targetList = new_tlist };
        }
    });
}

fn update_rtfunc_columns_types(
    rte: *mut pg_sys::RangeTblEntry,
    query: *mut pg_sys::Query
) {
    memcx::current_context(|mcx| {
        let functions = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*rte).functions, mcx).unwrap()
        };
        let rtfunc: *mut pg_sys::RangeTblFunction = *functions.get(0).unwrap() as *mut pg_sys::RangeTblFunction;
        let targetlist = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*query).targetList, mcx).unwrap()
        };
        /* 열 개수 파악 */
        let ncols = targetlist.len() as i32;
        unsafe { (*rtfunc).funccolcount = ncols };

        /* 결과를 담을 List* 초기화 */
        let mut coltypes_list: *mut pg_sys::List = std::ptr::null_mut();
        let mut typmods_list : *mut pg_sys::List = std::ptr::null_mut();
        let mut coll_list    : *mut pg_sys::List = std::ptr::null_mut();

        /* 한 번의 루프에서 배열 채우고 List 확장 */
        for (_idx, cell) in targetlist.iter().enumerate() {
            let te_ptr = *cell as *mut pg_sys::TargetEntry;
            let expr   = unsafe { (*te_ptr).expr } as *mut pg_sys::Node;

            let ty_oid   = unsafe { pg_sys::exprType(expr) };
            let ty_mod   = unsafe { pg_sys::exprTypmod(expr) };
            let coll_oid = unsafe { pg_sys::exprCollation(expr) };

            unsafe {
                coltypes_list = pg_sys::lappend_oid(coltypes_list, ty_oid);
                typmods_list  = pg_sys::lappend_int(typmods_list, ty_mod);
                coll_list     = pg_sys::lappend_oid(coll_list,    coll_oid);
            }
        }

        /* RangeTblFunction에 최종 세팅 */
        unsafe {
            (*rtfunc).funccoltypes      = coltypes_list;
            (*rtfunc).funccoltypmods    = typmods_list;
            (*rtfunc).funccolcollations = coll_list;
        }
    })
}


fn get_single_funcexpr_from_rte(
    _rte: *mut pg_sys::RangeTblEntry
) -> *mut pg_sys::FuncExpr {
    memcx::current_context(|mcx| {
        let functions = unsafe {
            List::<*mut c_void>::downcast_ptr_in_memcx((*_rte).functions, mcx).unwrap()
        };
        if functions.len() != 1 {
            return std::ptr::null_mut();
        }

        let rtfunc = *functions.get(0).unwrap() as *mut pg_sys::RangeTblFunction;
        let funcexpr = unsafe { (*rtfunc).funcexpr } as *mut pg_sys::FuncExpr;
        return funcexpr;
    })
}

fn expr_get_const_str(
    _expr: *mut pg_sys::Node
) -> String {
    if unsafe { (*_expr).type_ != pg_sys::NodeTag::T_Const } {
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
            "Chat function must have 2 text arguments"
        );
        return String::new();
    }

    let expr = _expr.cast::<pg_sys::Const>();
    if unsafe { (*expr).consttype } != pg_sys::TEXTOID {
        pgrx::ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
            "Chat function must have 2 text arguments"
        );
        return String::new();
    }

    let constvalue = unsafe { (*expr).constvalue };
    return unsafe { <String as FromDatum>::from_datum(constvalue, false).unwrap() };
}