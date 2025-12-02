# Parameters

SafeQL exposes a set of PostgreSQL GUC parameters that control refinement behavior, pruning, search limits, caching, and priority weighting.

---

## 1. Refinement Enable/Disable Flags

These switches enable or disable each refinement step in SafeQL’s search space.  
Useful for debugging, ablation studies, or tuning refinement behavior.

| Parameter | Default | Description |
|----------|---------|-------------|
| enable_safeql_refinement | true | Global toggle for SafeQL refinement. |
| enable_table_refinement | true | Repairs incorrect table names. |
| enable_column_refinement | true | Repairs incorrect column names. |
| enable_table_for_column | true | Infers missing table when column exists. |
| enable_column_table_reference | true | Fixes column/table qualifier mismatches. |
| enable_join_refinement | true | Repairs or inserts JOIN operations. |
| enable_operand_column_refinement | true | Repairs operand column expressions. |
| enable_operand_table_for_column_refinement | true | Infers missing table for operand columns. |
| enable_operand_column_table_reference_refinement | true | Fixes operand qualifier mismatches. |
| enable_operand_typecast_refinement | false | Repairs type mismatches in operands. |
| enable_argument_column_refinement | true | Repairs function argument columns. |
| enable_argument_table_for_column_refinement | true | Infers missing table for function arguments. |
| enable_argument_column_table_reference_refinement | true | Fixes column qualifiers in arguments. |
| enable_argument_typecast_refinement | true | Repairs function argument type mismatches. |
| enable_function_name_refinement | true | Repairs incorrect SQL function names. |
| enable_column_ambiguity_refinement | true | Resolves ambiguous column references. |
| enable_value_refinement | true | Repairs literal values (e.g., category corrections). |

Example:

```sql
SET safeql.enable_join_refinement TO false;
SET safeql.enable_value_refinement TO false;
```

---

## 2. Pruning Parameters

Pruning helps SafeQL discard irrelevant or invalid refinements before deeper search.

### 2.1 Type-based Pruning

| Parameter | Default | Description |
|----------|---------|-------------|
| enable_type_based_refinement | true | Removes refinements with incompatible SQL types. |

Example:

```sql
SET safeql.enable_type_based_refinement TO false;
```

---

### 2.2 Top-K Semantic Pruning

SafeQL uses embedding similarity to rank refinements and keeps only **K** candidates per category.

| Parameter | Default | Description |
|----------|---------|-------------|
| top_k_expansion | 3 | Number of candidates to keep per refinement group. |

Example:

```sql
SET safeql.top_k_expansion TO 5;
```

---

## 3. Optimization Parameters

These parameters influence search limits, caching behavior, and performance characteristics.

### 3.1 Search Cache

SafeQL integrates with the pgvecto.rs search cache for major speedups in repeated refinements.

| Parameter | Default | Description |
|----------|---------|-------------|
| enable_search_cache | true | When enabled, search results are cached for improved performance. |

Example:

```sql
SET vectors.enable_search_cache TO false;
```


### 3.2 Search Limits

| Parameter | Default | Description |
|----------|---------|-------------|
| max_refinement_hop | 5 | Maximum refinement depth. |
| max_refinement_num | 300 | Maximum total refinement candidates. |
| value_refinement_samples | 1000000 | Max number of values sampled in value refinement. |

Example:

```sql
SET safeql.max_refinement_num TO 150;
SET safeql.max_refinement_hop TO 3;
```

---

## 4. Refinement Priority Weights

Priority weights determine the order in which SafeQL explores refinements.  
Lower weight → Higher priority.

| Parameter                         | Default |
|-----------------------------------|---------|
| table_refinement_weight           | 1.0     |
| column_refinement_weight          | 1.0     |
| table_for_column_weight           | 1.0     |
| column_table_reference_weight     | 1.0     |
| join_refinement_weight            | 2.0     |
| operand_refinement_weight         | 1.0     |
| argument_refinement_weight        | 1.0     |
| typecast_refinement_weight        | 1.0     |
| function_name_refinement_weight   | 1.0     |
| column_ambiguity_refinement_weight| 1.0     |
| value_refinement_weight           | 1.0     |

Example:

```sql
SET safeql.join_refinement_weight TO 3.0;
SET safeql.typecast_refinement_weight TO 0.5;
```
