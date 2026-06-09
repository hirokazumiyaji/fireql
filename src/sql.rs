use crate::error::{FireqlError, Result};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderByExpr, OrderByKind, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone)]
pub enum StatementAst {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    InsertSelect(InsertSelectStatement),
}

#[derive(Debug, Clone)]
pub struct CollectionSpec {
    pub collection_id: String,
    pub parent_path: Option<String>,
    pub is_group: bool,
}

#[derive(Debug, Clone)]
pub enum Projection {
    All,
    Fields(Vec<String>),
}

pub(crate) const FIREQL_REF_KEY: &str = "__fireql_ref";
pub(crate) const FIREQL_TS_KEY: &str = "__fireql_ts";
pub(crate) const FIREQL_CURRENT_TS_KEY: &str = "__fireql_current_ts";

fn sentinel_object(key: &str, value: JsonValue) -> JsonValue {
    JsonValue::Object([(key.to_string(), value)].into_iter().collect())
}

fn reject_function_modifiers(function: &sqlparser::ast::Function, context: &str) -> Result<()> {
    let has_distinct = matches!(
        &function.args,
        FunctionArguments::List(list) if list.duplicate_treatment.is_some()
    );
    if has_distinct || function.filter.is_some() || function.over.is_some() {
        return Err(FireqlError::Unsupported(format!(
            "{context} modifiers are not supported"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub enum SelectProjection {
    Fields(Projection),
    Aggregations(Vec<AggregationExpr>),
}

#[derive(Debug, Clone)]
pub struct AggregationExpr {
    pub func: AggregationFunc,
    pub field: Option<String>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy)]
pub enum AggregationFunc {
    Count,
    Sum,
    Avg,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub collection: CollectionSpec,
    pub alias: Option<String>,
    pub projection: SelectProjection,
    pub filter: Option<FilterExpr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u32>,
    pub joins: Option<Vec<JoinSpec>>,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub collection: CollectionSpec,
    pub assignments: Vec<(String, JsonValue)>,
    pub filter: FilterExpr,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub collection: CollectionSpec,
    pub filter: FilterExpr,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct InsertSelectStatement {
    pub collection: CollectionSpec,
    pub columns: Option<Vec<String>>,
    pub source: SelectStatement,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub field: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub join_type: JoinType,
    pub collection: CollectionSpec,
    pub left_field: String,
    pub right_field: String,
    pub left_alias: Option<String>,
    pub right_alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum FilterExpr {
    Compare {
        field: String,
        op: CompareOp,
        value: JsonValue,
    },
    ArrayContains {
        field: String,
        value: JsonValue,
    },
    ArrayContainsAny {
        field: String,
        values: Vec<JsonValue>,
    },
    InList {
        field: String,
        values: Vec<JsonValue>,
        negated: bool,
    },
    Unary {
        field: String,
        op: UnaryOp,
    },
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    IsNull,
    IsNotNull,
}

pub fn parse_sql(input: &str) -> Result<StatementAst> {
    if let Some(stmt) = try_parse_delete_table_function(input)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_insert_collection_function(input)? {
        return Ok(stmt);
    }

    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, input).map_err(|e| FireqlError::SqlParse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(FireqlError::Unsupported(
            "Only a single SQL statement is supported".to_string(),
        ));
    }

    let stmt = statements.remove(0);
    match stmt {
        Statement::Query(query) => parse_query(*query),
        Statement::Update(update) => {
            let collection = parse_table_with_joins(&update.table)?;
            let filter = update
                .selection
                .map(|expr| parse_filter_expr(&expr))
                .transpose()?
                .ok_or(FireqlError::MissingWhere)?;
            let assignments = parse_assignments(update.assignments)?;
            Ok(StatementAst::Update(UpdateStatement {
                collection,
                assignments,
                filter,
                order_by: vec![],
                limit: None,
            }))
        }
        Statement::Delete(delete) => {
            let from = match delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            if from.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "Only one FROM source is supported in DELETE".to_string(),
                ));
            }
            let collection = parse_table_with_joins(&from[0])?;
            let filter = delete
                .selection
                .map(|expr| parse_filter_expr(&expr))
                .transpose()?
                .ok_or(FireqlError::MissingWhere)?;
            let (order_by, limit) =
                parse_order_and_limit_from_query_parts(Some(delete.order_by), delete.limit)?;
            Ok(StatementAst::Delete(DeleteStatement {
                collection,
                filter,
                order_by,
                limit,
            }))
        }
        Statement::Insert(insert) => parse_insert_select(insert, None),
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported statement: {other}"
        ))),
    }
}

fn try_parse_insert_collection_function(input: &str) -> Result<Option<StatementAst>> {
    let Some(after_insert) = strip_keyword(input, "insert") else {
        return Ok(None);
    };
    let Some(after_into) = strip_keyword(after_insert, "into") else {
        return Ok(None);
    };
    let Some(after_collection) = strip_keyword(after_into, "collection") else {
        return Ok(None);
    };

    let after_collection = after_collection.trim_start();
    if !after_collection.starts_with('(') {
        return Ok(None);
    }
    let Some(first_arg_char) = after_collection[1..].trim_start().chars().next() else {
        return Ok(None);
    };
    if first_arg_char != '\'' {
        return Ok(None);
    }

    let close = find_matching_paren(after_collection, 0)
        .ok_or_else(|| FireqlError::SqlParse("Unclosed collection() target".to_string()))?;
    let target_expr = format!("collection{}", &after_collection[..=close]);
    let target = parse_collection_target_expr(&target_expr)?;
    let remainder = after_collection[close + 1..].trim_start();
    let rewritten = format!("INSERT INTO __fireql_insert_target {remainder}");

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, &rewritten)
        .map_err(|e| FireqlError::SqlParse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(FireqlError::Unsupported(
            "Only a single SQL statement is supported".to_string(),
        ));
    }

    match statements.remove(0) {
        Statement::Insert(insert) => parse_insert_select(insert, Some(target)).map(Some),
        _ => Err(FireqlError::Unsupported(
            "INSERT rewrite produced unsupported statement".to_string(),
        )),
    }
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    let prefix = trimmed.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &trimmed[keyword.len()..];
    match rest.chars().next() {
        Some(c) if c.is_ascii_alphanumeric() || c == '_' => None,
        _ => Some(rest),
    }
}

fn find_matching_paren(input: &str, open_idx: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut idx = open_idx;

    while idx < bytes.len() {
        let b = bytes[idx];
        if let Some(q) = quote {
            if b == q {
                if bytes.get(idx + 1) == Some(&q) {
                    idx += 2;
                    continue;
                }
                quote = None;
            }
            idx += 1;
            continue;
        }

        match b {
            b'\'' | b'"' => quote = Some(b),
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }

    None
}

fn parse_collection_target_expr(target_expr: &str) -> Result<CollectionSpec> {
    let sql = format!("SELECT * FROM {target_expr}");
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, &sql).map_err(|e| FireqlError::SqlParse(e.to_string()))?;
    match statements.remove(0) {
        Statement::Query(query) => match parse_query(*query)? {
            StatementAst::Select(select) => Ok(select.collection),
            _ => Err(FireqlError::Unsupported(
                "INSERT target rewrite produced unsupported statement".to_string(),
            )),
        },
        _ => Err(FireqlError::Unsupported(
            "INSERT target rewrite produced unsupported statement".to_string(),
        )),
    }
}

fn parse_insert_select(
    insert: sqlparser::ast::Insert,
    collection_override: Option<CollectionSpec>,
) -> Result<StatementAst> {
    if !insert.into
        || insert.optimizer_hint.is_some()
        || insert.or.is_some()
        || insert.ignore
        || insert.table_alias.is_some()
        || insert.overwrite
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.replace_into
        || insert.priority.is_some()
        || insert.insert_alias.is_some()
        || insert.settings.is_some()
        || insert.format_clause.is_some()
        || insert.has_table_keyword
    {
        return Err(FireqlError::Unsupported(
            "Only INSERT INTO ... SELECT is supported".to_string(),
        ));
    }

    let collection = match collection_override {
        Some(collection) => collection,
        None => parse_insert_target(&insert.table)?,
    };
    if collection.is_group {
        return Err(FireqlError::Unsupported(
            "collection_group() is not supported as INSERT target".to_string(),
        ));
    }

    let source = insert.source.ok_or_else(|| {
        FireqlError::Unsupported("Only INSERT INTO ... SELECT is supported".to_string())
    })?;
    let source = match parse_query(*source)? {
        StatementAst::Select(select) => select,
        _ => {
            return Err(FireqlError::Unsupported(
                "INSERT source must be a SELECT query".to_string(),
            ))
        }
    };

    if source.collection.is_group {
        return Err(FireqlError::Unsupported(
            "collection_group() is not supported in INSERT SELECT".to_string(),
        ));
    }
    if source.joins.is_some() {
        return Err(FireqlError::Unsupported(
            "JOIN is not supported in INSERT SELECT".to_string(),
        ));
    }
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(
            insert
                .columns
                .into_iter()
                .map(|c| c.value)
                .collect::<Vec<_>>(),
        )
    };
    validate_insert_select_projection(columns.as_deref(), &source.projection)?;

    Ok(StatementAst::InsertSelect(InsertSelectStatement {
        collection,
        columns,
        source,
    }))
}

fn parse_insert_target(target: &TableObject) -> Result<CollectionSpec> {
    match target {
        TableObject::TableName(name) => parse_object_name(name),
        TableObject::TableFunction(function) => {
            let name = object_name_to_string(&function.name);
            if !name.eq_ignore_ascii_case("collection") {
                return Err(FireqlError::Unsupported(format!(
                    "Unsupported INSERT target function: {name}"
                )));
            }
            let args = extract_function_arg_list(&function.args)?;
            parse_collection_args(args)
        }
    }
}

fn validate_insert_select_projection(
    columns: Option<&[String]>,
    projection: &SelectProjection,
) -> Result<()> {
    match (columns, projection) {
        (None, SelectProjection::Fields(Projection::All)) => Ok(()),
        (None, SelectProjection::Fields(Projection::Fields(_))) => Err(FireqlError::Unsupported(
            "INSERT SELECT without destination columns requires SELECT *".to_string(),
        )),
        (None, SelectProjection::Aggregations(_))
        | (Some(_), SelectProjection::Aggregations(_)) => Err(FireqlError::Unsupported(
            "Aggregation is not supported in INSERT SELECT".to_string(),
        )),
        (Some(columns), SelectProjection::Fields(Projection::All)) => {
            if columns.is_empty() {
                return Err(FireqlError::Unsupported(
                    "INSERT destination columns cannot be empty".to_string(),
                ));
            }
            Err(FireqlError::Unsupported(
                "INSERT SELECT with destination columns requires explicit SELECT fields"
                    .to_string(),
            ))
        }
        (Some(columns), SelectProjection::Fields(Projection::Fields(fields))) => {
            if columns.is_empty() {
                return Err(FireqlError::Unsupported(
                    "INSERT destination columns cannot be empty".to_string(),
                ));
            }
            if columns.len() != fields.len() {
                return Err(FireqlError::Unsupported(
                    "INSERT destination columns must match SELECT field count".to_string(),
                ));
            }
            for (idx, column) in columns.iter().enumerate() {
                if column == "__name__" && fields.get(idx).map(String::as_str) != Some("__name__") {
                    return Err(FireqlError::Unsupported(
                        "__name__ destination column requires __name__ at the same SELECT field position"
                            .to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

fn try_parse_delete_table_function(input: &str) -> Result<Option<StatementAst>> {
    let trimmed = input.trim_start();

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    if !parts.next().unwrap_or("").eq_ignore_ascii_case("delete") {
        return Ok(None);
    }

    let rest_trimmed = parts.next().unwrap_or("").trim_start();
    let mut words = rest_trimmed.splitn(2, char::is_whitespace);
    if !words.next().unwrap_or("").eq_ignore_ascii_case("from") {
        return Ok(None);
    }
    let after_from = words.next().unwrap_or("").trim_start();
    if !after_from.split_once('(').is_some_and(|(name, _)| {
        let n = name.trim();
        n.eq_ignore_ascii_case("collection_group") || n.eq_ignore_ascii_case("collection")
    }) {
        return Ok(None);
    }

    let select_sql = format!("SELECT * {rest_trimmed}");

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, &select_sql)
        .map_err(|e| FireqlError::SqlParse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(FireqlError::Unsupported(
            "Only a single SQL statement is supported".to_string(),
        ));
    }

    match statements.remove(0) {
        Statement::Query(query) => {
            let stmt = match parse_query(*query)? {
                StatementAst::Select(select) => select,
                _ => {
                    return Err(FireqlError::Unsupported(
                        "DELETE rewrite produced unsupported statement".to_string(),
                    ))
                }
            };

            let filter = stmt.filter.ok_or(FireqlError::MissingWhere)?;
            Ok(Some(StatementAst::Delete(DeleteStatement {
                collection: stmt.collection,
                filter,
                order_by: stmt.order_by,
                limit: stmt.limit,
            })))
        }
        _ => Err(FireqlError::Unsupported(
            "DELETE rewrite produced unsupported statement".to_string(),
        )),
    }
}

fn parse_query(query: Query) -> Result<StatementAst> {
    let order_by_exprs = match query.order_by {
        Some(order_by) => match order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => {
                return Err(FireqlError::Unsupported(
                    "ORDER BY ALL is not supported".to_string(),
                ))
            }
        },
        None => vec![],
    };

    let limit_expr = match query.limit_clause {
        Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) => {
            if offset.is_some() {
                return Err(FireqlError::Unsupported(
                    "OFFSET is not supported".to_string(),
                ));
            }
            limit
        }
        Some(sqlparser::ast::LimitClause::OffsetCommaLimit { .. }) => {
            return Err(FireqlError::Unsupported(
                "OFFSET is not supported".to_string(),
            ));
        }
        None => None,
    };

    match *query.body {
        SetExpr::Select(select) => parse_select(*select, order_by_exprs, limit_expr),
        other => Err(FireqlError::Unsupported(format!(
            "Only SELECT is supported in queries. Found: {other}"
        ))),
    }
}

fn parse_select(
    select: Select,
    order_by_exprs: Vec<OrderByExpr>,
    limit_expr: Option<Expr>,
) -> Result<StatementAst> {
    // Reject clauses that sqlparser accepts but fireql does not translate, so they
    // can never be silently dropped (e.g. TOP/QUALIFY would otherwise change the
    // result set without warning).
    let unsupported: &[(bool, &str)] = &[
        (select.distinct.is_some(), "DISTINCT"),
        (select.top.is_some(), "TOP"),
        (select.having.is_some(), "HAVING"),
        (select.qualify.is_some(), "QUALIFY"),
        (select.prewhere.is_some(), "PREWHERE"),
        (select.into.is_some(), "SELECT INTO"),
        (select.exclude.is_some(), "EXCLUDE"),
        (select.optimizer_hint.is_some(), "optimizer hints"),
        (select.select_modifiers.is_some(), "SELECT modifiers"),
        (select.value_table_mode.is_some(), "SELECT AS STRUCT/VALUE"),
        (!select.lateral_views.is_empty(), "LATERAL VIEW"),
        (!select.connect_by.is_empty(), "CONNECT BY"),
        (!select.cluster_by.is_empty(), "CLUSTER BY"),
        (!select.distribute_by.is_empty(), "DISTRIBUTE BY"),
        (!select.sort_by.is_empty(), "SORT BY"),
        (!select.named_window.is_empty(), "WINDOW"),
    ];
    if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
        return Err(FireqlError::Unsupported(format!(
            "{clause} is not supported"
        )));
    }
    if !matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty())
    {
        return Err(FireqlError::Unsupported(
            "GROUP BY is not supported".to_string(),
        ));
    }

    if select.from.len() != 1 {
        return Err(FireqlError::Unsupported(
            "Only one FROM source is supported".to_string(),
        ));
    }

    let (collection, alias, joins) = parse_table_with_joins_for_select(&select.from[0])?;
    let projection = parse_projection(&select.projection)?;

    if joins.is_some() && matches!(projection, SelectProjection::Aggregations(_)) {
        return Err(FireqlError::Unsupported(
            "Aggregation with JOIN is not supported".to_string(),
        ));
    }

    let filter = select
        .selection
        .map(|expr| parse_filter_expr(&expr))
        .transpose()?;
    let (order_by, limit) =
        parse_order_and_limit_from_query_parts(Some(order_by_exprs), limit_expr)?;

    if joins.is_some() && !order_by.is_empty() {
        return Err(FireqlError::Unsupported(
            "ORDER BY is not supported with JOIN".to_string(),
        ));
    }
    if joins.is_some() && limit.is_some() {
        return Err(FireqlError::Unsupported(
            "LIMIT is not supported with JOIN".to_string(),
        ));
    }

    if let (Some(joins), Some(filter)) = (&joins, &filter) {
        let right_names: Vec<&str> = joins
            .iter()
            .map(|j| {
                j.right_alias
                    .as_deref()
                    .unwrap_or(j.collection.collection_id.as_str())
            })
            .collect();
        validate_join_filter_aliases(filter, &right_names)?;
    }

    Ok(StatementAst::Select(SelectStatement {
        collection,
        alias,
        projection,
        filter,
        order_by,
        limit,
        joins,
    }))
}

/// In a JOIN query the WHERE filter is pushed to the left (FROM) collection
/// query only, so a field qualified by a joined table's alias (e.g. `o.amount`)
/// would otherwise be sent to Firestore as a left-side map path and silently
/// match nothing. Reject those up front. Unqualified or left-qualified fields,
/// and nested map paths (whose prefix is not a join alias), are left untouched.
fn validate_join_filter_aliases(filter: &FilterExpr, right_names: &[&str]) -> Result<()> {
    match filter {
        FilterExpr::Compare { field, .. }
        | FilterExpr::ArrayContains { field, .. }
        | FilterExpr::ArrayContainsAny { field, .. }
        | FilterExpr::InList { field, .. }
        | FilterExpr::Unary { field, .. } => {
            if let Some((prefix, _)) = field.split_once('.') {
                if right_names.contains(&prefix) {
                    return Err(FireqlError::Unsupported(format!(
                        "WHERE cannot reference the joined table `{prefix}`; filters apply to the left (FROM) table only"
                    )));
                }
            }
            Ok(())
        }
        FilterExpr::And(filters) | FilterExpr::Or(filters) => {
            for f in filters {
                validate_join_filter_aliases(f, right_names)?;
            }
            Ok(())
        }
    }
}

fn parse_table_with_joins(table: &TableWithJoins) -> Result<CollectionSpec> {
    if !table.joins.is_empty() {
        return Err(FireqlError::Unsupported(
            "JOIN is not supported".to_string(),
        ));
    }
    parse_table_factor(&table.relation)
}

fn parse_table_with_joins_for_select(
    table: &TableWithJoins,
) -> Result<(CollectionSpec, Option<String>, Option<Vec<JoinSpec>>)> {
    if table.joins.is_empty() {
        let (collection, alias) = parse_table_factor_with_alias(&table.relation)?;
        return Ok((collection, alias, None));
    }

    let (collection, alias) = parse_table_factor_with_alias(&table.relation)?;
    let mut join_specs = Vec::with_capacity(table.joins.len());

    for join in &table.joins {
        let join_type = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(on_expr)) => (JoinType::Inner, on_expr),
            JoinOperator::LeftOuter(JoinConstraint::On(on_expr)) => (JoinType::Left, on_expr),
            JoinOperator::Left(JoinConstraint::On(on_expr)) => (JoinType::Left, on_expr),
            _ => {
                return Err(FireqlError::Unsupported(
                    "Only INNER JOIN and LEFT JOIN are supported".to_string(),
                ))
            }
        };

        let (right_collection, right_alias) = parse_table_factor_with_alias(&join.relation)?;
        let (first_qualifier, first_field, second_qualifier, second_field) =
            parse_join_on_expr(join_type.1)?;

        let left_name = alias.as_deref().unwrap_or(&collection.collection_id);
        let right_name = right_alias
            .as_deref()
            .unwrap_or(&right_collection.collection_id);

        let (left_alias_on, left_field, right_alias_on, right_field) =
            match (&first_qualifier, &second_qualifier) {
                (Some(fq), Some(sq)) if fq == right_name && sq == left_name => {
                    (second_qualifier, second_field, first_qualifier, first_field)
                }
                (Some(fq), None) if fq == right_name => {
                    (second_qualifier, second_field, first_qualifier, first_field)
                }
                (None, Some(sq)) if sq == left_name => {
                    (second_qualifier, second_field, first_qualifier, first_field)
                }
                _ => (first_qualifier, first_field, second_qualifier, second_field),
            };

        join_specs.push(JoinSpec {
            join_type: join_type.0,
            collection: right_collection,
            left_field,
            right_field,
            left_alias: left_alias_on.or_else(|| alias.clone()),
            right_alias: right_alias_on.or(right_alias),
        });
    }

    Ok((collection, alias, Some(join_specs)))
}

fn parse_table_factor_with_alias(factor: &TableFactor) -> Result<(CollectionSpec, Option<String>)> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            sample,
            ..
        } => {
            if sample.is_some() {
                return Err(FireqlError::Unsupported(
                    "TABLESAMPLE is not supported".to_string(),
                ));
            }
            if let Some(tfa) = args {
                let func_name = object_name_to_string(name);
                if func_name.eq_ignore_ascii_case("collection_group") {
                    let spec = parse_collection_group_args(&tfa.args)?;
                    let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                    return Ok((spec, alias_str));
                }
                if func_name.eq_ignore_ascii_case("collection") {
                    let spec = parse_collection_args(&tfa.args)?;
                    let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                    return Ok((spec, alias_str));
                }
                return Err(FireqlError::Unsupported(format!(
                    "Table-valued functions are not supported: {func_name}"
                )));
            }

            let collection = parse_object_name(name)?;
            let alias_str = alias.as_ref().map(|a| a.name.value.clone());
            Ok((collection, alias_str))
        }
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported FROM source: {other}"
        ))),
    }
}

fn parse_join_on_expr(expr: &Expr) -> Result<(Option<String>, String, Option<String>, String)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if !matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                return Err(FireqlError::Unsupported(
                    "Only equality conditions are supported in JOIN ON clause".to_string(),
                ));
            }
            let (left_table, left_field) = parse_compound_ident_expr(left)?;
            let (right_table, right_field) = parse_compound_ident_expr(right)?;
            Ok((left_table, left_field, right_table, right_field))
        }
        _ => Err(FireqlError::Unsupported(
            "Only equality conditions are supported in JOIN ON clause".to_string(),
        )),
    }
}

fn parse_compound_ident_expr(expr: &Expr) -> Result<(Option<String>, String)> {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            Ok((Some(idents[0].value.clone()), idents[1].value.clone()))
        }
        Expr::Identifier(ident) => Ok((None, ident.value.clone())),
        _ => Err(FireqlError::Unsupported(
            "JOIN ON clause requires field references in the form table.field or field".to_string(),
        )),
    }
}

fn parse_table_factor(factor: &TableFactor) -> Result<CollectionSpec> {
    parse_table_factor_with_alias(factor).map(|(spec, _)| spec)
}

/// Extracts the single string argument of `collection()` / `collection_group()`,
/// accepting either a string literal (`'posts'`) or a bare identifier (`posts`).
fn collection_function_arg(args: &[FunctionArg], context: &str) -> Result<String> {
    if args.len() != 1 {
        return Err(FireqlError::Unsupported(format!(
            "{context} expects exactly one argument"
        )));
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
            Expr::Value(_) => expr_to_string_literal(expr, context),
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            other => Err(FireqlError::Unsupported(format!(
                "{context} expects a string literal or identifier, got: {other}"
            ))),
        },
        _ => Err(FireqlError::Unsupported(format!(
            "{context} expects a single unnamed argument"
        ))),
    }
}

fn parse_collection_group_args(args: &[FunctionArg]) -> Result<CollectionSpec> {
    let collection_id = collection_function_arg(args, "collection_group()")?;
    Ok(CollectionSpec {
        collection_id,
        parent_path: None,
        is_group: true,
    })
}

const COLLECTION_PATH_ERR: &str =
    "collection() expects a relative collection path ending in a collection id";

pub fn parse_collection_relative_path(raw: &str) -> Result<(String, Option<String>)> {
    let segments: Vec<&str> = raw.split('/').collect();
    if segments.iter().any(|s| s.is_empty()) || segments.len().is_multiple_of(2) {
        return Err(FireqlError::Unsupported(COLLECTION_PATH_ERR.to_string()));
    }
    let collection_id = segments.last().unwrap().to_string();
    let parent_path = if segments.len() == 1 {
        None
    } else {
        Some(segments[..segments.len() - 1].join("/"))
    };
    Ok((collection_id, parent_path))
}

fn parse_collection_args(args: &[FunctionArg]) -> Result<CollectionSpec> {
    let raw = collection_function_arg(args, "collection()")?;
    let (collection_id, parent_path) = parse_collection_relative_path(&raw)?;
    Ok(CollectionSpec {
        collection_id,
        parent_path,
        is_group: false,
    })
}

fn parse_object_name(name: &ObjectName) -> Result<CollectionSpec> {
    if name.0.len() != 1 {
        return Err(FireqlError::Unsupported(
            "Only simple collection names are supported".to_string(),
        ));
    }
    let ident = match &name.0[0] {
        ObjectNamePart::Identifier(ident) => ident,
        _ => {
            return Err(FireqlError::Unsupported(
                "Only simple collection names are supported".to_string(),
            ))
        }
    };
    Ok(CollectionSpec {
        collection_id: ident.value.clone(),
        parent_path: None,
        is_group: false,
    })
}

fn parse_projection(items: &[SelectItem]) -> Result<SelectProjection> {
    let mut fields = Vec::new();
    let mut aggregates = Vec::new();
    let mut has_wildcard = false;
    for item in items {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                has_wildcard = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(agg) = parse_aggregate_expr(expr, None)? {
                    aggregates.push(agg);
                } else {
                    let field = parse_field_expr(expr)?;
                    fields.push(field);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(agg) = parse_aggregate_expr(expr, Some(alias.value.clone()))? {
                    aggregates.push(agg);
                } else {
                    return Err(FireqlError::Unsupported(
                        "SELECT field alias is not supported".to_string(),
                    ));
                }
            }
        }
    }

    if !aggregates.is_empty() {
        if has_wildcard || !fields.is_empty() {
            return Err(FireqlError::Unsupported(
                "SELECT cannot mix aggregate functions with normal fields".to_string(),
            ));
        }
        validate_unique_aggregate_aliases(&aggregates)?;
        Ok(SelectProjection::Aggregations(aggregates))
    } else if has_wildcard {
        Ok(SelectProjection::Fields(Projection::All))
    } else if !fields.is_empty() {
        Ok(SelectProjection::Fields(Projection::Fields(fields)))
    } else {
        Ok(SelectProjection::Fields(Projection::All))
    }
}

fn parse_aggregate_expr(expr: &Expr, alias: Option<String>) -> Result<Option<AggregationExpr>> {
    let function = match expr {
        Expr::Function(function) => function,
        _ => return Ok(None),
    };

    reject_function_modifiers(function, "Aggregate")?;

    let name = object_name_to_string(&function.name);
    let name_lower = name.to_ascii_lowercase();
    let alias = alias.unwrap_or_else(|| name_lower.clone());

    match name_lower.as_str() {
        "count" => {
            let field = parse_count_arg(&function.args)?;
            Ok(Some(AggregationExpr {
                func: AggregationFunc::Count,
                field,
                alias,
            }))
        }
        "sum" => {
            let field = parse_single_field_arg(&function.args, "SUM")?;
            Ok(Some(AggregationExpr {
                func: AggregationFunc::Sum,
                field: Some(field),
                alias,
            }))
        }
        "avg" => {
            let field = parse_single_field_arg(&function.args, "AVG")?;
            Ok(Some(AggregationExpr {
                func: AggregationFunc::Avg,
                field: Some(field),
                alias,
            }))
        }
        _ => Ok(None),
    }
}

fn expr_to_string_literal(expr: &Expr, context: &str) -> Result<String> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
            _ => Err(FireqlError::Unsupported(format!(
                "{context} expects a string literal"
            ))),
        },
        _ => Err(FireqlError::Unsupported(format!(
            "{context} expects a string literal"
        ))),
    }
}

fn extract_function_arg_list(args: &FunctionArguments) -> Result<&[FunctionArg]> {
    match args {
        FunctionArguments::List(list) => Ok(&list.args),
        FunctionArguments::None => Ok(&[]),
        _ => Err(FireqlError::Unsupported(
            "Subquery function arguments are not supported".to_string(),
        )),
    }
}

fn parse_count_arg(args: &FunctionArguments) -> Result<Option<String>> {
    let args = extract_function_arg_list(args)?;
    if args.len() != 1 {
        return Err(FireqlError::Unsupported(
            "COUNT expects exactly one argument".to_string(),
        ));
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => Ok(None),
            _ => Err(FireqlError::Unsupported(
                "COUNT supports field, literal, or *".to_string(),
            )),
        },
        _ => Err(FireqlError::Unsupported(
            "COUNT supports only unnamed arguments".to_string(),
        )),
    }
}

fn parse_single_field_arg(args: &FunctionArguments, label: &str) -> Result<String> {
    let args = extract_function_arg_list(args)?;
    if args.len() != 1 {
        return Err(FireqlError::Unsupported(format!(
            "{label} expects exactly one argument"
        )));
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => parse_field_expr(expr),
        _ => Err(FireqlError::Unsupported(format!(
            "{label} supports only field arguments"
        ))),
    }
}

fn validate_unique_aggregate_aliases(aggregates: &[AggregationExpr]) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for agg in aggregates {
        if !seen.insert(agg.alias.as_str()) {
            return Err(FireqlError::Unsupported(format!(
                "Duplicate aggregation alias: {}",
                agg.alias
            )));
        }
    }
    Ok(())
}

fn parse_order_and_limit_from_query_parts(
    order_by_exprs: Option<Vec<OrderByExpr>>,
    limit_expr: Option<Expr>,
) -> Result<(Vec<OrderBy>, Option<u32>)> {
    let mut order_by = Vec::new();
    for expr in order_by_exprs.unwrap_or_default() {
        order_by.push(parse_order_by_expr(&expr)?);
    }

    let limit = match limit_expr {
        Some(expr) => parse_limit_expr(&expr)?,
        None => None,
    };

    Ok((order_by, limit))
}

fn parse_order_by_expr(expr: &OrderByExpr) -> Result<OrderBy> {
    let field = parse_field_expr(&expr.expr)?;
    let direction = match expr.options.asc {
        Some(true) | None => OrderDirection::Asc,
        Some(false) => OrderDirection::Desc,
    };
    Ok(OrderBy { field, direction })
}

fn parse_limit_expr(expr: &Expr) -> Result<Option<u32>> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(value, _) => value.parse::<u32>().map(Some).map_err(|_| {
                FireqlError::Unsupported("LIMIT must be a non-negative integer".to_string())
            }),
            _ => Err(FireqlError::Unsupported(
                "LIMIT must be a numeric literal".to_string(),
            )),
        },
        _ => Err(FireqlError::Unsupported(
            "LIMIT must be a numeric literal".to_string(),
        )),
    }
}

fn parse_assignments(
    assignments: Vec<sqlparser::ast::Assignment>,
) -> Result<Vec<(String, JsonValue)>> {
    let mut result = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let field = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name_to_string(name),
            AssignmentTarget::Tuple(_) => {
                return Err(FireqlError::Unsupported(
                    "Tuple assignment is not supported".to_string(),
                ))
            }
        };
        let value = parse_value_expr(&assignment.value)?;
        result.push((field, value));
    }
    Ok(result)
}

fn parse_filter_expr(expr: &Expr) -> Result<FilterExpr> {
    match expr {
        Expr::Function(function) => parse_filter_function(function),
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator;
            match op {
                BinaryOperator::And => {
                    let left = parse_filter_expr(left)?;
                    let right = parse_filter_expr(right)?;
                    Ok(merge_filters(FilterExpr::And(vec![left, right])))
                }
                BinaryOperator::Or => {
                    let left = parse_filter_expr(left)?;
                    let right = parse_filter_expr(right)?;
                    Ok(merge_filters(FilterExpr::Or(vec![left, right])))
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let field = parse_field_expr(left)?;
                    let value = parse_value_expr(right)?;
                    let op = match op {
                        BinaryOperator::Eq => CompareOp::Eq,
                        BinaryOperator::NotEq => CompareOp::NotEq,
                        BinaryOperator::Lt => CompareOp::Lt,
                        BinaryOperator::LtEq => CompareOp::LtEq,
                        BinaryOperator::Gt => CompareOp::Gt,
                        BinaryOperator::GtEq => CompareOp::GtEq,
                        _ => unreachable!(),
                    };
                    Ok(FilterExpr::Compare { field, op, value })
                }
                _ => Err(FireqlError::Unsupported(format!(
                    "Unsupported binary operator: {op}"
                ))),
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let field = parse_field_expr(expr)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                values.push(parse_value_expr(item)?);
            }
            Ok(FilterExpr::InList {
                field,
                values,
                negated: *negated,
            })
        }
        Expr::IsNull(expr) => {
            let field = parse_field_expr(expr)?;
            Ok(FilterExpr::Unary {
                field,
                op: UnaryOp::IsNull,
            })
        }
        Expr::IsNotNull(expr) => {
            let field = parse_field_expr(expr)?;
            Ok(FilterExpr::Unary {
                field,
                op: UnaryOp::IsNotNull,
            })
        }
        Expr::Nested(expr) => parse_filter_expr(expr),
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported WHERE expression: {other}"
        ))),
    }
}

fn parse_filter_function(function: &sqlparser::ast::Function) -> Result<FilterExpr> {
    reject_function_modifiers(function, "WHERE function")?;

    let name = object_name_to_string(&function.name);
    let name_lower = name.to_ascii_lowercase();
    let args = parse_function_args(&function.args)?;

    match name_lower.as_str() {
        "array_contains" => {
            if args.len() != 2 {
                return Err(FireqlError::Unsupported(
                    "array_contains(field, value) expects 2 arguments".to_string(),
                ));
            }
            let field = parse_field_expr(&args[0])?;
            let value = parse_value_expr(&args[1])?;
            Ok(FilterExpr::ArrayContains { field, value })
        }
        "array_contains_any" => {
            if args.len() < 2 {
                return Err(FireqlError::Unsupported(
                    "array_contains_any(field, values...) expects at least 2 arguments".to_string(),
                ));
            }
            let field = parse_field_expr(&args[0])?;
            let values = if args.len() == 2 {
                parse_value_list_expr(&args[1])?
            } else {
                args[1..]
                    .iter()
                    .map(parse_value_expr)
                    .collect::<Result<Vec<_>>>()?
            };
            Ok(FilterExpr::ArrayContainsAny { field, values })
        }
        _ => Err(FireqlError::Unsupported(format!(
            "Unsupported function in WHERE: {name}"
        ))),
    }
}

fn parse_function_args(args: &FunctionArguments) -> Result<Vec<Expr>> {
    let arg_list = extract_function_arg_list(args)?;
    let mut exprs = Vec::with_capacity(arg_list.len());
    for arg in arg_list {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => exprs.push(expr.clone()),
            _ => {
                return Err(FireqlError::Unsupported(
                    "Only unnamed function arguments are supported".to_string(),
                ))
            }
        }
    }
    Ok(exprs)
}

fn parse_value_list_expr(expr: &Expr) -> Result<Vec<JsonValue>> {
    match expr {
        Expr::Array(array) => array
            .elem
            .iter()
            .map(parse_value_expr)
            .collect::<Result<Vec<_>>>(),
        Expr::Tuple(items) => items
            .iter()
            .map(parse_value_expr)
            .collect::<Result<Vec<_>>>(),
        other => Ok(vec![parse_value_expr(other)?]),
    }
}

fn merge_filters(expr: FilterExpr) -> FilterExpr {
    match expr {
        FilterExpr::And(filters) => {
            let mut merged = Vec::new();
            for f in filters {
                match f {
                    FilterExpr::And(inner) => merged.extend(inner),
                    other => merged.push(other),
                }
            }
            FilterExpr::And(merged)
        }
        FilterExpr::Or(filters) => {
            let mut merged = Vec::new();
            for f in filters {
                match f {
                    FilterExpr::Or(inner) => merged.extend(inner),
                    other => merged.push(other),
                }
            }
            FilterExpr::Or(merged)
        }
        other => other,
    }
}

fn parse_field_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => Ok(idents
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported field expression: {other}"
        ))),
    }
}

fn parse_value_expr(expr: &Expr) -> Result<JsonValue> {
    match expr {
        Expr::Value(vws) => parse_value(&vws.value),
        Expr::Function(function) => parse_value_function(function),
        Expr::Identifier(ident) => {
            if ident.value.eq_ignore_ascii_case("current_timestamp") {
                Ok(sentinel_object(
                    FIREQL_CURRENT_TS_KEY,
                    JsonValue::Bool(true),
                ))
            } else {
                Err(FireqlError::Unsupported(format!(
                    "Unsupported identifier in value expression: {ident}"
                )))
            }
        }
        Expr::UnaryOp { op, expr } => match op {
            sqlparser::ast::UnaryOperator::Minus => match &**expr {
                Expr::Value(vws) => match &vws.value {
                    Value::Number(num, _) => {
                        let with_sign = format!("-{num}");
                        parse_numeric(&with_sign)
                    }
                    _ => Err(FireqlError::Unsupported(
                        "Unary minus only supported for numeric literals".to_string(),
                    )),
                },
                _ => Err(FireqlError::Unsupported(
                    "Unary minus only supported for numeric literals".to_string(),
                )),
            },
            _ => Err(FireqlError::Unsupported(
                "Only unary minus is supported for values".to_string(),
            )),
        },
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported value expression: {other}"
        ))),
    }
}

fn parse_value_function(function: &sqlparser::ast::Function) -> Result<JsonValue> {
    let name = object_name_to_string(&function.name);
    let name_lower = name.to_ascii_lowercase();
    let args = parse_function_args(&function.args)?;

    match name_lower.as_str() {
        "ref" | "reference" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "ref(path) expects exactly one argument".to_string(),
                ));
            }
            let path = expr_to_string_literal(&args[0], "ref(path)")?;
            Ok(sentinel_object(FIREQL_REF_KEY, JsonValue::String(path)))
        }
        "timestamp" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "timestamp(value) expects exactly one argument".to_string(),
                ));
            }
            let value = expr_to_string_literal(&args[0], "timestamp(value)")?;
            Ok(sentinel_object(FIREQL_TS_KEY, JsonValue::String(value)))
        }
        "current_timestamp" => {
            if !args.is_empty() {
                return Err(FireqlError::Unsupported(
                    "CURRENT_TIMESTAMP expects no arguments".to_string(),
                ));
            }
            Ok(sentinel_object(
                FIREQL_CURRENT_TS_KEY,
                JsonValue::Bool(true),
            ))
        }
        _ => Err(FireqlError::Unsupported(format!(
            "Unsupported function in value expression: {name}"
        ))),
    }
}

fn parse_value(value: &Value) -> Result<JsonValue> {
    match value {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(JsonValue::String(s.clone()))
        }
        Value::Number(num, _) => parse_numeric(num),
        Value::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Value::Null => Ok(JsonValue::Null),
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported literal: {other}"
        ))),
    }
}

fn parse_numeric(input: &str) -> Result<JsonValue> {
    if let Ok(int) = input.parse::<i64>() {
        Ok(JsonValue::Number(int.into()))
    } else if let Ok(float) = input.parse::<f64>() {
        serde_json::Number::from_f64(float)
            .map(JsonValue::Number)
            .ok_or_else(|| FireqlError::Unsupported("Invalid float literal".to_string()))
    } else {
        Err(FireqlError::Unsupported(
            "Numeric literal must be int or float".to_string(),
        ))
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_with_filter_order_limit() {
        let stmt =
            parse_sql("SELECT * FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "users");
                assert!(select.collection.parent_path.is_none());
                assert!(!select.collection.is_group);
                assert!(matches!(
                    select.projection,
                    SelectProjection::Fields(Projection::All)
                ));
                assert!(select.filter.is_some());
                assert_eq!(select.order_by.len(), 1);
                assert_eq!(select.limit, Some(10));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_collection_group_select() {
        let stmt =
            parse_sql("SELECT name FROM collection_group('profiles') WHERE active = true").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "profiles");
                assert!(select.collection.parent_path.is_none());
                assert!(select.collection.is_group);
                assert!(matches!(
                    select.projection,
                    SelectProjection::Fields(Projection::Fields(_))
                ));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn update_requires_where() {
        let err = parse_sql("UPDATE users SET status = 'active'").unwrap_err();
        assert!(matches!(err, FireqlError::MissingWhere));
    }

    #[test]
    fn delete_collection_group_requires_where() {
        let err = parse_sql("DELETE FROM collection_group('logs')").unwrap_err();
        assert!(matches!(err, FireqlError::MissingWhere));
    }

    #[test]
    fn parse_delete_collection_group_with_where() {
        let stmt =
            parse_sql("DELETE FROM collection_group('logs') WHERE created_at < '2023-01-01'")
                .unwrap();
        match stmt {
            StatementAst::Delete(delete) => {
                assert_eq!(delete.collection.collection_id, "logs");
                assert!(delete.collection.parent_path.is_none());
                assert!(delete.collection.is_group);
            }
            _ => panic!("expected delete"),
        }
    }

    #[test]
    fn parse_collection_shorthand() {
        let stmt = parse_sql("SELECT * FROM collection('posts') WHERE draft = false").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "posts");
                assert!(select.collection.parent_path.is_none());
                assert!(!select.collection.is_group);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_collection_subcollection() {
        let stmt =
            parse_sql("SELECT * FROM collection('users/user1/posts') WHERE author = 'x'").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "posts");
                assert_eq!(
                    select.collection.parent_path.as_deref(),
                    Some("users/user1")
                );
                assert!(!select.collection.is_group);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_collection_bare_identifier() {
        let stmt = parse_sql("SELECT * FROM collection(posts) WHERE draft = false").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "posts");
                assert!(select.collection.parent_path.is_none());
                assert!(!select.collection.is_group);
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn unsupported_query_error_is_concise_sql() {
        let err = parse_sql("SELECT * FROM users WHERE a BETWEEN 1 AND 5").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("a BETWEEN 1 AND 5"), "got: {msg}");
        assert!(!msg.contains("Span"), "error leaks AST debug noise: {msg}");
        assert!(
            !msg.contains("Location"),
            "error leaks AST debug noise: {msg}"
        );
    }

    #[test]
    fn parse_update_delete_collection_subcollection() {
        let u =
            parse_sql("UPDATE collection('users/user1/posts') SET ok = true WHERE n = 1").unwrap();
        match u {
            StatementAst::Update(up) => {
                assert_eq!(up.collection.collection_id, "posts");
                assert_eq!(up.collection.parent_path.as_deref(), Some("users/user1"));
            }
            _ => panic!("expected update"),
        }
        let d = parse_sql("DELETE FROM collection('users/user1/posts') WHERE n = 1").unwrap();
        match d {
            StatementAst::Delete(del) => {
                assert_eq!(del.collection.collection_id, "posts");
                assert_eq!(del.collection.parent_path.as_deref(), Some("users/user1"));
            }
            _ => panic!("expected delete"),
        }
    }

    #[test]
    fn parse_insert_select_auto_id_copy() {
        let stmt =
            parse_sql("INSERT INTO archived_users SELECT * FROM users WHERE disabled = true")
                .unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(insert.collection.collection_id, "archived_users");
                assert!(insert.columns.is_none());
                assert_eq!(insert.source.collection.collection_id, "users");
                assert!(matches!(
                    insert.source.projection,
                    SelectProjection::Fields(Projection::All)
                ));
                assert!(insert.source.filter.is_some());
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_select_subcollections() {
        let stmt = parse_sql(
            "INSERT INTO collection('users/u1/archive') \
             SELECT * FROM collection('users/u1/posts') WHERE published = false",
        )
        .unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(insert.collection.collection_id, "archive");
                assert_eq!(insert.collection.parent_path.as_deref(), Some("users/u1"));
                assert_eq!(insert.source.collection.collection_id, "posts");
                assert_eq!(
                    insert.source.collection.parent_path.as_deref(),
                    Some("users/u1")
                );
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_select_with_id_preservation_columns() {
        let stmt = parse_sql(
            "INSERT INTO archived_users (__name__, name, age) \
             SELECT __name__, name, age FROM users WHERE disabled = true",
        )
        .unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(
                    insert.columns.as_ref().expect("columns"),
                    &vec![
                        "__name__".to_string(),
                        "name".to_string(),
                        "age".to_string()
                    ]
                );
                assert!(matches!(
                    insert.source.projection,
                    SelectProjection::Fields(Projection::Fields(_))
                ));
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_select_allows_collection_named_collection_with_columns() {
        let stmt = parse_sql("INSERT INTO collection (name) SELECT name FROM users").unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(insert.collection.collection_id, "collection");
                assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn insert_select_name_destination_requires_name_source() {
        let err =
            parse_sql("INSERT INTO archived_users (__name__, name) SELECT id, name FROM users")
                .unwrap_err();
        assert!(err.to_string().contains("__name__"));
    }

    #[test]
    fn insert_select_name_destination_requires_positional_name_source() {
        let err = parse_sql(
            "INSERT INTO archived_users (__name__, name) SELECT name, __name__ FROM users",
        )
        .unwrap_err();
        assert!(err.to_string().contains("same SELECT field position"));
    }

    #[test]
    fn insert_select_rejects_aggregation() {
        let err = parse_sql("INSERT INTO archived_users SELECT COUNT(*) FROM users").unwrap_err();
        assert!(err.to_string().contains("Aggregation is not supported"));
    }

    #[test]
    fn insert_select_rejects_collection_group_source() {
        let err = parse_sql("INSERT INTO archived_users SELECT * FROM collection_group('users')")
            .unwrap_err();
        assert!(err.to_string().contains("collection_group"));
    }

    #[test]
    fn parse_insert_select_collection_named_collection_without_space_before_columns() {
        let stmt = parse_sql("INSERT INTO collection(name) SELECT name FROM users").unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(insert.collection.collection_id, "collection");
                assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_select_collection_named_collection_with_quoted_column() {
        let stmt = parse_sql("INSERT INTO collection(\"name\") SELECT name FROM users").unwrap();

        match stmt {
            StatementAst::InsertSelect(insert) => {
                assert_eq!(insert.collection.collection_id, "collection");
                assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
            }
            other => panic!("expected insert select, got {other:?}"),
        }
    }

    #[test]
    fn parse_join_collection_subcollection() {
        let sql = "SELECT * FROM collection('users/user1/posts') p INNER JOIN users u ON u.id = p.author_id";
        parse_sql(sql).unwrap();
    }

    #[test]
    fn collection_path_rejects_invalid() {
        for bad in [
            "",
            "/users/u1/posts",
            "users/u1/posts/",
            "users//u1/posts",
            "users/u1",
        ] {
            let err =
                parse_sql(&format!("SELECT * FROM collection('{bad}') WHERE x = 1")).unwrap_err();
            assert!(
                err.to_string().contains(super::COLLECTION_PATH_ERR),
                "unexpected err for {bad:?}: {err}"
            );
        }
    }

    #[test]
    fn parse_array_contains() {
        let stmt = parse_sql("SELECT * FROM users WHERE array_contains(tags, 'a')").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let filter = select.filter.expect("filter");
                match filter {
                    FilterExpr::ArrayContains { field, value } => {
                        assert_eq!(field, "tags");
                        assert_eq!(value, JsonValue::from("a"));
                    }
                    _ => panic!("expected array_contains filter"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_array_contains_any() {
        let stmt =
            parse_sql("SELECT * FROM users WHERE array_contains_any(tags, ['a','b'])").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let filter = select.filter.expect("filter");
                match filter {
                    FilterExpr::ArrayContainsAny { field, values } => {
                        assert_eq!(field, "tags");
                        assert_eq!(values.len(), 2);
                    }
                    _ => panic!("expected array_contains_any filter"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_ref_value() {
        let stmt = parse_sql("SELECT * FROM users WHERE owner = ref('users/user1')").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let filter = select.filter.expect("filter");
                match filter {
                    FilterExpr::Compare { value, .. } => {
                        let obj = value.as_object().expect("object");
                        assert_eq!(obj.get(FIREQL_REF_KEY).unwrap(), "users/user1");
                    }
                    _ => panic!("expected compare filter"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_timestamp_value() {
        let stmt =
            parse_sql("SELECT * FROM users WHERE created_at >= timestamp('2024-01-01T00:00:00Z')")
                .unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let filter = select.filter.expect("filter");
                match filter {
                    FilterExpr::Compare { value, .. } => {
                        let obj = value.as_object().expect("object");
                        assert_eq!(obj.get(FIREQL_TS_KEY).unwrap(), "2024-01-01T00:00:00Z");
                    }
                    _ => panic!("expected compare filter"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_current_timestamp_value() {
        let stmt = parse_sql("SELECT * FROM users WHERE created_at >= CURRENT_TIMESTAMP").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let filter = select.filter.expect("filter");
                match filter {
                    FilterExpr::Compare { value, .. } => {
                        let obj = value.as_object().expect("object");
                        assert_eq!(
                            obj.get(FIREQL_CURRENT_TS_KEY).unwrap(),
                            &JsonValue::Bool(true)
                        );
                    }
                    _ => panic!("expected compare filter"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_update_with_current_timestamp_assignment() {
        let stmt =
            parse_sql("UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE status = 'active'")
                .unwrap();
        match stmt {
            StatementAst::Update(update) => {
                assert_eq!(update.assignments.len(), 1);
                let (field, value) = &update.assignments[0];
                assert_eq!(field, "updated_at");
                let obj = value.as_object().expect("object");
                assert_eq!(
                    obj.get(FIREQL_CURRENT_TS_KEY).unwrap(),
                    &JsonValue::Bool(true)
                );
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn parse_count_aggregate() {
        let stmt = parse_sql("SELECT COUNT(*) FROM users").unwrap();
        match stmt {
            StatementAst::Select(select) => match select.projection {
                SelectProjection::Aggregations(aggs) => {
                    assert_eq!(aggs.len(), 1);
                    assert!(matches!(aggs[0].func, AggregationFunc::Count));
                    assert_eq!(aggs[0].alias, "count");
                }
                _ => panic!("expected aggregation"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_count_field_is_count_star() {
        let stmt = parse_sql("SELECT COUNT(age) FROM users").unwrap();
        match stmt {
            StatementAst::Select(select) => match select.projection {
                SelectProjection::Aggregations(aggs) => {
                    assert_eq!(aggs.len(), 1);
                    assert!(matches!(aggs[0].func, AggregationFunc::Count));
                    assert_eq!(aggs[0].alias, "count");
                    assert!(aggs[0].field.is_none());
                }
                _ => panic!("expected aggregation"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_sum_aggregate_with_alias() {
        let stmt = parse_sql("SELECT SUM(score) AS total FROM users").unwrap();
        match stmt {
            StatementAst::Select(select) => match select.projection {
                SelectProjection::Aggregations(aggs) => {
                    assert_eq!(aggs.len(), 1);
                    assert!(matches!(aggs[0].func, AggregationFunc::Sum));
                    assert_eq!(aggs[0].alias, "total");
                }
                _ => panic!("expected aggregation"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn aggregate_cannot_mix_fields() {
        let err = parse_sql("SELECT name, COUNT(*) FROM users").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn distinct_is_rejected() {
        let err = parse_sql("SELECT DISTINCT name FROM users").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn group_by_is_rejected() {
        let err = parse_sql("SELECT COUNT(*) FROM users GROUP BY team").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn having_is_rejected() {
        let err =
            parse_sql("SELECT COUNT(*) FROM users GROUP BY team HAVING COUNT(*) > 1").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn offset_is_rejected() {
        let err = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 20").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn top_is_rejected() {
        let err = parse_sql("SELECT TOP 5 * FROM users").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn qualify_is_rejected() {
        let err = parse_sql("SELECT * FROM users QUALIFY ROW_NUMBER() OVER () = 1").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn prewhere_is_rejected() {
        let err = parse_sql("SELECT * FROM users PREWHERE active = true").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn cluster_by_is_rejected() {
        let err = parse_sql("SELECT * FROM users CLUSTER BY name").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn sort_by_is_rejected() {
        let err = parse_sql("SELECT * FROM users SORT BY name").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn window_clause_is_rejected() {
        let err = parse_sql("SELECT * FROM users WINDOW w AS (PARTITION BY team)").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn select_into_is_rejected() {
        let err = parse_sql("SELECT * INTO archived FROM users").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn tablesample_is_rejected() {
        let err = parse_sql("SELECT * FROM users TABLESAMPLE BERNOULLI (10)").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[test]
    fn parse_inner_join() {
        let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.collection.collection_id, "users");
                let join = select.joins.as_ref().expect("should have join");
                assert_eq!(join.len(), 1);
                assert_eq!(join[0].collection.collection_id, "orders");
                assert!(matches!(join[0].join_type, JoinType::Inner));
                assert_eq!(join[0].left_field, "id");
                assert_eq!(join[0].right_field, "user_id");
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_left_join() {
        let sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let join = select.joins.as_ref().expect("should have join");
                assert!(matches!(join[0].join_type, JoinType::Left));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_with_alias() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert_eq!(select.alias.as_deref(), Some("u"));
                let join = select.joins.as_ref().expect("should have join");
                assert_eq!(join[0].left_alias.as_deref(), Some("u"));
                assert_eq!(join[0].right_alias.as_deref(), Some("o"));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_rejects_unsupported_join_types() {
        let sql = "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id";
        let err = parse_sql(sql).unwrap_err();
        assert!(err
            .to_string()
            .contains("Only INNER JOIN and LEFT JOIN are supported"));
    }

    #[test]
    fn parse_join_rejects_non_equality_on() {
        let sql = "SELECT * FROM users INNER JOIN orders ON users.id > orders.user_id";
        let err = parse_sql(sql).unwrap_err();
        assert!(err.to_string().contains("Only equality conditions"));
    }

    #[test]
    fn parse_join_rejects_aggregation_with_join() {
        let sql = "SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id";
        let err = parse_sql(sql).unwrap_err();
        assert!(err.to_string().contains("Aggregation"));
    }

    #[test]
    fn parse_join_with_qualified_fields() {
        let sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => match &select.projection {
                SelectProjection::Fields(Projection::Fields(fields)) => {
                    assert_eq!(fields, &["u.name", "o.amount"]);
                }
                _ => panic!("expected fields projection"),
            },
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_with_where() {
        let sql =
            "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert!(select.joins.is_some());
                assert!(select.filter.is_some());
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_with_order_by_and_limit() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY u.name LIMIT 10";
        let err = parse_sql(sql).unwrap_err();
        assert!(err
            .to_string()
            .contains("ORDER BY is not supported with JOIN"));
    }

    #[test]
    fn parse_join_rejects_order_by_with_join() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY u.name";
        let err = parse_sql(sql).unwrap_err();
        assert!(err
            .to_string()
            .contains("ORDER BY is not supported with JOIN"));
    }

    #[test]
    fn parse_join_rejects_limit_with_join() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LIMIT 10";
        let err = parse_sql(sql).unwrap_err();
        assert!(err.to_string().contains("LIMIT is not supported with JOIN"));
    }

    #[test]
    fn join_where_referencing_right_alias_is_rejected() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE o.amount > 100";
        let err = parse_sql(sql).unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)), "got: {err:?}");
    }

    #[test]
    fn join_where_referencing_left_alias_is_allowed() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE u.active = true";
        assert!(parse_sql(sql).is_ok());
    }

    #[test]
    fn join_where_nested_field_is_allowed() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE profile.age > 18";
        assert!(parse_sql(sql).is_ok());
    }

    #[test]
    fn parse_left_outer_join() {
        let sql = "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                let join = select.joins.as_ref().expect("should have join");
                assert!(matches!(join[0].join_type, JoinType::Left));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_without_alias() {
        let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert!(select.alias.is_none());
                let join = select.joins.as_ref().expect("should have join");
                assert_eq!(join[0].left_alias.as_deref(), Some("users"));
                assert_eq!(join[0].right_alias.as_deref(), Some("orders"));
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn parse_join_using_clause_rejected() {
        let sql = "SELECT * FROM users INNER JOIN orders USING (id)";
        let err = parse_sql(sql).unwrap_err();
        assert!(err
            .to_string()
            .contains("Only INNER JOIN and LEFT JOIN are supported"));
    }

    #[test]
    fn select_wildcard_with_fields_is_all() {
        let stmt = parse_sql("SELECT *, name FROM users").unwrap();
        match stmt {
            StatementAst::Select(select) => {
                assert!(matches!(
                    select.projection,
                    SelectProjection::Fields(Projection::All)
                ));
            }
            _ => panic!("expected select"),
        }
    }
}
