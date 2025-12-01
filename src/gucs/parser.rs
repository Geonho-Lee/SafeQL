// guc.rs - GUC parameter 설정
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::PostgresGucEnum;

#[derive(Debug, Clone, Copy, PostgresGucEnum)]
#[allow(non_camel_case_types)]
pub enum BindMode {
    soft,
    hard,
}

pub static BIND_MODE: GucSetting<BindMode> = GucSetting::<BindMode>::new(BindMode::soft);

// SafeQL refinement 활성화 옵션 GUC 변수들
pub static ENABLE_SAFEQL_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_TABLE_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_COLUMN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_TABLE_FOR_COLUMN: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_COLUMN_TABLE_REFERENCE: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_JOIN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_OPERAND_COLUMN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_OPERAND_TABLE_FOR_COLUMN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_OPERAND_COLUMN_TABLE_REFERENCE_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_OPERAND_TYPECAST_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static ENABLE_ARGUMENT_COLUMN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_ARGUMENT_TABLE_FOR_COLUMN_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_ARGUMENT_COLUMN_TABLE_REFERENCE_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_ARGUMENT_TYPECAST_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_FUNCTION_NAME_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_COLUMN_AMBIGUITY_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static ENABLE_VALUE_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);

// SafeQL search 최적화 옵션 GUC 변수들
pub static ENABLE_TYPE_BASED_REFINEMENT: GucSetting<bool> = GucSetting::<bool>::new(true);

// SafeQL search limits
pub static MAX_REFINEMENT_HOP: GucSetting<i32> = GucSetting::<i32>::new(5);
pub static MAX_REFINEMENT_NUM: GucSetting<i32> = GucSetting::<i32>::new(300);
pub static VALUE_REFINEMENT_SAMPLES: GucSetting<i32> = GucSetting::<i32>::new(1000000);


// SafeQL top k search 설정
pub static TOP_K_EXPANSION: GucSetting<i32> = GucSetting::<i32>::new(3);

// SafeQL refinement priority 가중치 GUC 변수들
pub static TABLE_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static COLUMN_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static TABLE_FOR_COLUMN_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static COLUMN_TABLE_REFERENCE_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static JOIN_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(2.0);
pub static OPERAND_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static ARGUMENT_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static TYPECAST_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(0.1);
pub static FUNCTION_NAME_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);
pub static COLUMN_AMBIGUITY_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(0.0);
pub static VALUE_REFINEMENT_WEIGHT: GucSetting<f64> = GucSetting::<f64>::new(1.0);


pub unsafe fn init() {
    GucRegistry::define_enum_guc(
        "vectors.bind_mode",
        "Bind mode.",
        "",
        &BIND_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );

    // SafeQL refinement 활성화 옵션 설정
    GucRegistry::define_bool_guc(
        "safeql.enable_refinement",
        "Enable or disable SafeQL refinement globally",
        "When disabled, SafeQL will not perform any query refinements. Default is true.",
        &ENABLE_SAFEQL_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_table_refinement",
        "Enable table name refinement (CASE 1: FROM Rel -> FROM Rel')",
        "When disabled, table name corrections will not be suggested. Default is true.",
        &ENABLE_TABLE_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_column_refinement",
        "Enable column name refinement (CASE 2: SELECT Att -> SELECT Att')",
        "When disabled, column name corrections will not be suggested. Default is true.",
        &ENABLE_COLUMN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_table_for_column",
        "Enable table refinement for column errors (CASE 3: FROM Rel -> FROM Rel')",
        "When disabled, table changes for column errors will not be suggested. Default is true.",
        &ENABLE_TABLE_FOR_COLUMN,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_column_table_reference",
        "Enable column table reference refinement (CASE 4: SELECT R.Att -> SELECT S.Att)",
        "When disabled, column table reference changes will not be suggested. Default is false.",
        &ENABLE_COLUMN_TABLE_REFERENCE,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_join_refinement",
        "Enable join refinement (CASE 5: FROM R -> FROM R JOIN S)",
        "When disabled, table joins will not be suggested. Default is true.",
        &ENABLE_JOIN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_operand_column_refinement",
        "Enable operand replacement refinement (CASE 6: Expr -> Expr')",
        "When disabled, operand-level replacements will not be suggested. Default is true.",
        &ENABLE_OPERAND_COLUMN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    GucRegistry::define_bool_guc(
        "safeql.enable_operand_table_for_column_refinement",
        "Enable table-level replacement refinement for column operands (CASE 7: Table -> Table')",
        "When disabled, table candidates for operand columns will not be suggested. Default is true.",
        &ENABLE_OPERAND_TABLE_FOR_COLUMN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    GucRegistry::define_bool_guc(
        "safeql.enable_operand_column_table_reference_refinement",
        "Enable column–table reference refinement (CASE 8: Expr.Col -> Expr'.Col')",
        "When disabled, column-to-table reference refinements will not be generated. Default is true.",
        &ENABLE_OPERAND_COLUMN_TABLE_REFERENCE_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_operand_typecast_refinement",
        "Enable operand type casting refinement (CASE 6b: Expr -> CAST(Expr AS type))",
        "When disabled, operand type casting suggestions will not be generated. Default is false.",
        &ENABLE_OPERAND_TYPECAST_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    
    GucRegistry::define_bool_guc(
        "safeql.enable_argument_column_refinement",
        "Enable argument replacement refinement (CASE 9: Arg -> Arg')",
        "When disabled, argument-level column replacements will not be suggested. Default is true.",
        &ENABLE_ARGUMENT_COLUMN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    GucRegistry::define_bool_guc(
        "safeql.enable_argument_table_for_column_refinement",
        "Enable table-level replacement refinement for argument columns (CASE 10: Table -> Table')",
        "When disabled, table candidates for argument columns will not be suggested. Default is true.",
        &ENABLE_ARGUMENT_TABLE_FOR_COLUMN_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    GucRegistry::define_bool_guc(
        "safeql.enable_argument_column_table_reference_refinement",
        "Enable argument column–table reference refinement (CASE 11: Arg.Col -> Arg'.Col')",
        "When disabled, argument-level column-to-table reference refinements will not be generated. Default is true.",
        &ENABLE_ARGUMENT_COLUMN_TABLE_REFERENCE_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    GucRegistry::define_bool_guc(
        "safeql.enable_argument_typecast_refinement",
        "Enable function argument type casting refinement (CASE 6b: Expr -> CAST(Expr AS type))",
        "When disabled, function argument type casting suggestions will not be generated. Default is true.",
        &ENABLE_ARGUMENT_TYPECAST_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_function_name_refinement",
        "Enable function name refinement (CASE 6-4: func() -> similar_func())",
        "When disabled, function name corrections will not be suggested. Default is true.",
        &ENABLE_FUNCTION_NAME_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_column_ambiguity_refinement",
        "Enable column ambiguity refinement (CASE 9: unqualified column -> qualified column)",
        "When disabled, column ambiguity resolution will not be suggested. Default is true.",
        &ENABLE_COLUMN_AMBIGUITY_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_type_based_refinement",
        "Enable type-based refinement optimization",
        "When disabled, type-driven refinement pruning will not be applied. Default is true.",
        &ENABLE_TYPE_BASED_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "safeql.enable_value_refinement",
        "Enable value refinement (CASE 10: literal value -> nearest DB value)",
        "When disabled, value refinement will not be suggested. Default is true.",
        &ENABLE_VALUE_REFINEMENT,
        GucContext::Userset,
        GucFlags::default(),
    );

    // SafeQL search limits
    GucRegistry::define_int_guc(
        "safeql.max_refinement_hop",
        "Maximum number of refinement hops allowed from original SQL",
        "Controls how many changes from the original SQL are allowed. Higher values allow more modifications but may impact performance. Default is 5.",
        &MAX_REFINEMENT_HOP,
        1,      // min value
        20,     // max value
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "safeql.max_refinement_num",
        "Maximum number of candidates to search during refinement",
        "Controls how many refinement candidates SafeQL will examine. Higher values provide more thorough search but may impact performance. Default is 1000.",
        &MAX_REFINEMENT_NUM,
        10,     // min value
        1000000,  // max value
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "safeql.value_refinement_samples",
        "Number of value samples to consider during value refinement",
        "Controls how many nearest values from the database are considered for value refinement. Higher values provide more options but may impact performance. Default is 1000.",
        &VALUE_REFINEMENT_SAMPLES,
        10,     // min value
        1000000,  // max value
        GucContext::Userset,
        GucFlags::default(),
    );

    // SafeQL top k search 설정
    GucRegistry::define_int_guc(
        "safeql.top_k_expansion",
        "Number of top expansions during SafeQL search",
        "Controls how many refinement suggestions SafeQL will expand. Higher values provide more options but may impact performance. Default is 5.",
        &TOP_K_EXPANSION,
        1,      // min value
        100000,    // max value
        GucContext::Userset,
        GucFlags::default(),
    );

    // SafeQL refinement priority 가중치 설정
    GucRegistry::define_float_guc(
        "safeql.table_refinement_weight",
        "Weight multiplier for table refinement priority (CASE 1: FROM Rel -> FROM Rel')",
        "Higher values make table name corrections less preferred. Default is 1.0.",
        &TABLE_REFINEMENT_WEIGHT,
        0.1,    // min value
        10.0,   // max value
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.column_refinement_weight",
        "Weight multiplier for column refinement priority (CASE 2: SELECT Att -> SELECT Att')",
        "Higher values make column name corrections less preferred. Default is 1.0.",
        &COLUMN_REFINEMENT_WEIGHT,
        0.1,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.table_for_column_weight",
        "Weight multiplier for table-for-column refinement priority (CASE 3: FROM Rel -> FROM Rel')",
        "Higher values make table changes for column errors less preferred. Default is 1.0.",
        &TABLE_FOR_COLUMN_WEIGHT,
        0.1,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.column_table_reference_weight",
        "Weight multiplier for column table reference refinement priority (CASE 4: SELECT R.Att -> SELECT S.Att)",
        "Higher values make column table reference changes less preferred. Default is 1.0.",
        &COLUMN_TABLE_REFERENCE_WEIGHT,
        0.1,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.join_refinement_weight",
        "Weight multiplier for join refinement priority (CASE 5: FROM R -> FROM R JOIN S)",
        "Higher values make table joins less preferred. Default is 2.0.",
        &JOIN_REFINEMENT_WEIGHT,
        0.0,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.operand_refinement_weight",
        "Weight multiplier for operand refinement priority (CASE 6: R.a = C -> R.b = C)",
        "Higher values make operand change less preferred. Default is 1.0.",
        &OPERAND_REFINEMENT_WEIGHT,
        0.1,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.argument_refinement_weight",
        "Weight multiplier for argument refinement priority (CASE 6: Func(a) -> Func(b))",
        "Higher values make function argument changes less preferred. Default is 1.0.",
        &ARGUMENT_REFINEMENT_WEIGHT,
        0.1,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.typecast_refinement_weight",
        "Weight multiplier for type casting refinement priority (CASE 6b: R.a = C -> CAST(R.a AS type) = C)",
        "Higher values make type casting less preferred. Default is 0.0 (highest priority).",
        &TYPECAST_REFINEMENT_WEIGHT,
        0.0,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.function_name_refinement_weight",
        "Weight multiplier for function name refinement priority (CASE 6-4: func() -> similar_func())",
        "Higher values make function name corrections less preferred. Default is 1.0.",
        &FUNCTION_NAME_REFINEMENT_WEIGHT,
        0.0,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.column_ambiguity_refinement_weight",
        "Weight multiplier for column ambiguity refinement priority (CASE 9: col -> table.col)",
        "Higher values make column ambiguity resolution less preferred. Default is 0.0.",
        &COLUMN_AMBIGUITY_REFINEMENT_WEIGHT,
        0.0,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        "safeql.value_refinement_weight",
        "Weight multiplier for value refinement priority (CASE 10: literal -> nearest DB value)",
        "Higher values make value refinements less preferred based on distance. Default is 1.0.",
        &VALUE_REFINEMENT_WEIGHT,
        0.0,
        10.0,
        GucContext::Userset,
        GucFlags::default(),
    );
}