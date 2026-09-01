//! SQL 文の解析エントリポイント。ステートメントレベルの構造（SELECT / UPDATE /
//! DELETE / INSERT SELECT）と射影・ORDER BY・LIMIT を扱い、フィルタ/値式は
//! `filter` モジュール、FROM/JOIN は `table` モジュールに委譲する。

mod filter;
mod table;

use super::{
    AggregationExpr, AggregationFunc, CollectionSpec, DeleteStatement, FilterExpr,
    InsertSelectStatement, OrderBy, OrderDirection, Projection, SelectProjection, SelectStatement,
    SqlValue, StatementAst, UpdateStatement,
};
use crate::error::{FireqlError, Result};
use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, FunctionArgExpr, FunctionArguments, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use table::object_name_to_string;

pub(super) use filter::{parse_field_expr, parse_value_expr};
pub(super) use table::{
    parse_insert_target, parse_table_with_joins, parse_table_with_joins_for_select,
};

pub(super) fn reject_function_modifiers(
    function: &sqlparser::ast::Function,
    context: &str,
) -> Result<()> {
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

/// Rejects clauses that sqlparser accepts but fireql does not translate, so
/// they can never be silently dropped (e.g. DELETE USING or TOP would
/// otherwise change the statement's semantics without warning).
fn reject_unsupported_clauses(clauses: &[(bool, &str)]) -> Result<()> {
    if let Some((_, clause)) = clauses.iter().find(|(present, _)| *present) {
        return Err(FireqlError::Unsupported(format!(
            "{clause} is not supported"
        )));
    }
    Ok(())
}

pub(super) fn expr_to_string_literal(expr: &Expr, context: &str) -> Result<String> {
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

pub(super) fn extract_function_arg_list(
    args: &FunctionArguments,
) -> Result<&[sqlparser::ast::FunctionArg]> {
    match args {
        FunctionArguments::List(list) => Ok(&list.args),
        FunctionArguments::None => Ok(&[]),
        _ => Err(FireqlError::Unsupported(
            "Subquery function arguments are not supported".to_string(),
        )),
    }
}

pub fn parse_sql(input: &str) -> Result<StatementAst> {
    if let Some(stmt) = super::rewrite::try_parse_insert_collection_function(input)? {
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
            reject_unsupported_clauses(&[
                (!update.optimizer_hints.is_empty(), "optimizer hints"),
                (update.from.is_some(), "UPDATE ... FROM"),
                (update.returning.is_some(), "RETURNING"),
                (update.output.is_some(), "OUTPUT"),
                (update.or.is_some(), "UPDATE OR ..."),
            ])?;
            let collection = parse_table_with_joins(&update.table)?;
            let filter = update
                .selection
                .map(|expr| filter::parse_filter_expr(&expr))
                .transpose()?
                .ok_or(FireqlError::MissingWhere)?;
            let assignments = parse_assignments(update.assignments)?;
            let (order_by, limit) =
                parse_order_and_limit_from_query_parts(Some(update.order_by), update.limit)?;
            Ok(StatementAst::Update(UpdateStatement {
                collection,
                assignments,
                filter,
                order_by,
                limit,
            }))
        }
        Statement::Delete(delete) => {
            reject_unsupported_clauses(&[
                (!delete.optimizer_hints.is_empty(), "optimizer hints"),
                (!delete.tables.is_empty(), "Multi-table DELETE"),
                (delete.using.is_some(), "USING"),
                (delete.returning.is_some(), "RETURNING"),
                (delete.output.is_some(), "OUTPUT"),
            ])?;
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
                .map(|expr| filter::parse_filter_expr(&expr))
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

pub(super) fn parse_insert_select(
    insert: sqlparser::ast::Insert,
    collection_override: Option<CollectionSpec>,
) -> Result<StatementAst> {
    if !insert.into || insert.has_table_keyword {
        return Err(FireqlError::Unsupported(
            "Only INSERT INTO ... SELECT is supported".to_string(),
        ));
    }
    reject_unsupported_clauses(&[
        (!insert.optimizer_hints.is_empty(), "optimizer hints"),
        (insert.or.is_some(), "INSERT OR ..."),
        (insert.ignore, "INSERT IGNORE"),
        (insert.table_alias.is_some(), "INSERT target alias"),
        (insert.overwrite, "INSERT OVERWRITE"),
        (insert.partitioned.is_some(), "PARTITION"),
        (!insert.after_columns.is_empty(), "AFTER columns"),
        (!insert.assignments.is_empty(), "INSERT ... SET"),
        (insert.on.is_some(), "ON CONFLICT/ON DUPLICATE KEY"),
        (insert.returning.is_some(), "RETURNING"),
        (insert.output.is_some(), "OUTPUT"),
        (insert.replace_into, "REPLACE INTO"),
        (insert.priority.is_some(), "insert priority"),
        (insert.insert_alias.is_some(), "insert alias"),
        (insert.settings.is_some(), "SETTINGS"),
        (insert.format_clause.is_some(), "FORMAT"),
    ])?;

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
                .iter()
                .map(object_name_to_string)
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

pub(super) fn parse_query(query: Query) -> Result<StatementAst> {
    reject_unsupported_clauses(&[
        (query.with.is_some(), "WITH (CTE)"),
        (query.fetch.is_some(), "FETCH"),
        (!query.locks.is_empty(), "FOR UPDATE/FOR SHARE"),
        (query.for_clause.is_some(), "FOR XML/JSON/BROWSE"),
        (query.settings.is_some(), "SETTINGS"),
        (query.format_clause.is_some(), "FORMAT"),
        (!query.pipe_operators.is_empty(), "Pipe operators"),
    ])?;

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
    reject_unsupported_clauses(&[
        (select.distinct.is_some(), "DISTINCT"),
        (select.top.is_some(), "TOP"),
        (select.having.is_some(), "HAVING"),
        (select.qualify.is_some(), "QUALIFY"),
        (select.prewhere.is_some(), "PREWHERE"),
        (select.into.is_some(), "SELECT INTO"),
        (select.exclude.is_some(), "EXCLUDE"),
        (!select.optimizer_hints.is_empty(), "optimizer hints"),
        (select.select_modifiers.is_some(), "SELECT modifiers"),
        (select.value_table_mode.is_some(), "SELECT AS STRUCT/VALUE"),
        (!select.lateral_views.is_empty(), "LATERAL VIEW"),
        (!select.connect_by.is_empty(), "CONNECT BY"),
        (!select.cluster_by.is_empty(), "CLUSTER BY"),
        (!select.distribute_by.is_empty(), "DISTRIBUTE BY"),
        (!select.sort_by.is_empty(), "SORT BY"),
        (!select.named_window.is_empty(), "WINDOW"),
    ])?;
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
        .map(|expr| filter::parse_filter_expr(&expr))
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
            SelectItem::ExprWithAliases { .. } => {
                return Err(FireqlError::Unsupported(
                    "SELECT field alias is not supported".to_string(),
                ));
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

fn parse_count_arg(args: &FunctionArguments) -> Result<Option<String>> {
    let args = extract_function_arg_list(args)?;
    if args.len() != 1 {
        return Err(FireqlError::Unsupported(
            "COUNT expects exactly one argument".to_string(),
        ));
    }
    match &args[0] {
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
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
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => parse_field_expr(expr),
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
) -> Result<Vec<(String, SqlValue)>> {
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
