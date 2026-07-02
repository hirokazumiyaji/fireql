use super::{
    AggregationExpr, AggregationFunc, CollectionSpec, CompareOp, DeleteStatement, FilterExpr,
    InsertSelectStatement, JoinSpec, JoinType, OrderBy, OrderDirection, Projection,
    SelectProjection, SelectStatement, StatementAst, UnaryOp, UpdateStatement,
    FIREQL_CURRENT_TS_KEY, FIREQL_REF_KEY, FIREQL_TS_KEY,
};
use crate::error::{FireqlError, Result};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderByExpr, OrderByKind, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
            let unsupported: &[(bool, &str)] = &[
                (!update.optimizer_hints.is_empty(), "optimizer hints"),
                (update.from.is_some(), "UPDATE ... FROM"),
                (update.returning.is_some(), "RETURNING"),
                (update.output.is_some(), "OUTPUT"),
                (update.or.is_some(), "UPDATE OR ..."),
            ];
            if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
                return Err(FireqlError::Unsupported(format!(
                    "{clause} is not supported"
                )));
            }
            let collection = parse_table_with_joins(&update.table)?;
            let filter = update
                .selection
                .map(|expr| parse_filter_expr(&expr))
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
            let unsupported: &[(bool, &str)] = &[
                (!delete.optimizer_hints.is_empty(), "optimizer hints"),
                (!delete.tables.is_empty(), "Multi-table DELETE"),
                (delete.using.is_some(), "USING"),
                (delete.returning.is_some(), "RETURNING"),
                (delete.output.is_some(), "OUTPUT"),
            ];
            if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
                return Err(FireqlError::Unsupported(format!(
                    "{clause} is not supported"
                )));
            }
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

pub(super) fn parse_insert_select(
    insert: sqlparser::ast::Insert,
    collection_override: Option<CollectionSpec>,
) -> Result<StatementAst> {
    if !insert.into
        || !insert.optimizer_hints.is_empty()
        || insert.or.is_some()
        || insert.ignore
        || insert.table_alias.is_some()
        || insert.overwrite
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.output.is_some()
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
        TableObject::TableQuery(_) => Err(FireqlError::Unsupported(
            "INSERT target sub-query is not supported".to_string(),
        )),
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

pub(super) fn parse_query(query: Query) -> Result<StatementAst> {
    // Reject query-shell clauses that sqlparser accepts but fireql does not
    // translate, so they can never be silently dropped (same principle as the
    // clause table in parse_select).
    let unsupported: &[(bool, &str)] = &[
        (query.with.is_some(), "WITH (CTE)"),
        (query.fetch.is_some(), "FETCH"),
        (!query.locks.is_empty(), "FOR UPDATE/FOR SHARE"),
        (query.for_clause.is_some(), "FOR XML/JSON/BROWSE"),
        (query.settings.is_some(), "SETTINGS"),
        (query.format_clause.is_some(), "FORMAT"),
        (!query.pipe_operators.is_empty(), "Pipe operators"),
    ];
    if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
        return Err(FireqlError::Unsupported(format!(
            "{clause} is not supported"
        )));
    }

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
        (!select.optimizer_hints.is_empty(), "optimizer hints"),
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
fn parse_collection_args(args: &[FunctionArg]) -> Result<CollectionSpec> {
    let raw = collection_function_arg(args, "collection()")?;
    let (collection_id, parent_path) = super::parse_collection_relative_path(&raw)?;
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
