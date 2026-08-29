//! FROM 句の解析。コレクション指定（`collection()` / `collection_group()` /
//! 単純名・サブコレクションパス）と JOIN ON 句を CollectionSpec / JoinSpec へ
//! 変換する。

use super::super::{CollectionSpec, JoinSpec, JoinType};
use crate::error::{FireqlError, Result};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, ObjectName, ObjectNamePart, TableFactor, TableObject,
    TableWithJoins,
};

pub(in crate::sql) fn parse_table_with_joins(table: &TableWithJoins) -> Result<CollectionSpec> {
    if !table.joins.is_empty() {
        return Err(FireqlError::Unsupported(
            "JOIN is not supported".to_string(),
        ));
    }
    parse_table_factor(&table.relation)
}

pub(in crate::sql) fn parse_table_with_joins_for_select(
    table: &TableWithJoins,
) -> Result<(CollectionSpec, Option<String>, Option<Vec<JoinSpec>>)> {
    if table.joins.is_empty() {
        let (collection, alias) = parse_table_factor_with_alias(&table.relation)?;
        return Ok((collection, alias, None));
    }

    let (collection, alias) = parse_table_factor_with_alias(&table.relation)?;
    let mut join_specs = Vec::with_capacity(table.joins.len());
    // Chained JOIN では 2 つ目以降の JOIN の ON 句が、先頭テーブルだけでなく
    // それまでに結合した右側テーブル (別名またはコレクション名) を参照し得る。
    // 左右の入れ替え判定に使うため、先行テーブル名の一覧を保持する。
    let mut preceding_names: Vec<String> = Vec::with_capacity(table.joins.len() + 1);
    preceding_names.push(
        alias
            .clone()
            .unwrap_or_else(|| collection.collection_id.clone()),
    );

    for join in &table.joins {
        let join_type = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(on_expr)) => {
                (JoinType::Inner, on_expr)
            }
            sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(
                on_expr,
            )) => (JoinType::Left, on_expr),
            sqlparser::ast::JoinOperator::Left(sqlparser::ast::JoinConstraint::On(on_expr)) => {
                (JoinType::Left, on_expr)
            }
            _ => {
                return Err(FireqlError::Unsupported(
                    "Only INNER JOIN and LEFT JOIN are supported".to_string(),
                ))
            }
        };

        let (right_collection, right_alias) = parse_table_factor_with_alias(&join.relation)?;
        let (first_qualifier, first_field, second_qualifier, second_field) =
            parse_join_on_expr(join_type.1)?;

        // JoinSpec 構築時に right_collection がムーブされるため、先行テーブル
        // 一覧へ登録する名前は先に所有権を持たせておく。
        let right_name = right_alias
            .as_deref()
            .unwrap_or(&right_collection.collection_id)
            .to_string();
        let is_preceding_name = |name: &str| preceding_names.iter().any(|p| p == name);

        let (left_alias_on, left_field, right_alias_on, right_field) =
            match (&first_qualifier, &second_qualifier) {
                // ON 句の左辺が右側テーブルで右辺が先行テーブルを指す場合は入れ替える。
                // (例: `... JOIN t3 ON t3.t2_id = t2.id`)
                (Some(fq), Some(sq)) if fq.as_str() == right_name && is_preceding_name(sq) => {
                    (second_qualifier, second_field, first_qualifier, first_field)
                }
                (Some(fq), None) if fq.as_str() == right_name => {
                    (second_qualifier, second_field, first_qualifier, first_field)
                }
                (None, Some(sq)) if is_preceding_name(sq) => {
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

        preceding_names.push(right_name);
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
            Expr::Value(_) => super::expr_to_string_literal(expr, context),
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

pub(in crate::sql) fn parse_collection_args(args: &[FunctionArg]) -> Result<CollectionSpec> {
    let raw = collection_function_arg(args, "collection()")?;
    let (collection_id, parent_path) = super::super::parse_collection_relative_path(&raw)?;
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

pub(in crate::sql) fn parse_insert_target(target: &TableObject) -> Result<CollectionSpec> {
    match target {
        TableObject::TableName(name) => parse_object_name(name),
        TableObject::TableFunction(function) => {
            let name = object_name_to_string(&function.name);
            if !name.eq_ignore_ascii_case("collection") {
                return Err(FireqlError::Unsupported(format!(
                    "Unsupported INSERT target function: {name}"
                )));
            }
            let args = super::extract_function_arg_list(&function.args)?;
            parse_collection_args(args)
        }
        TableObject::TableQuery(_) => Err(FireqlError::Unsupported(
            "INSERT target sub-query is not supported".to_string(),
        )),
    }
}

pub(in crate::sql) fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}
