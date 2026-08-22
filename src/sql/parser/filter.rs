//! WHERE 句・値式の解析。フィルタ式、配列関数フィルタ、リテラル、
//! ref()/timestamp()/CURRENT_TIMESTAMP などの値関数を扱う。

use super::super::{CompareOp, FilterExpr, SqlValue, UnaryOp};
use crate::error::{FireqlError, Result};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value};

pub(in crate::sql) fn parse_filter_expr(expr: &Expr) -> Result<FilterExpr> {
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
    super::reject_function_modifiers(function, "WHERE function")?;

    let name = super::object_name_to_string(&function.name);
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
    let arg_list = super::extract_function_arg_list(args)?;
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

fn parse_value_list_expr(expr: &Expr) -> Result<Vec<SqlValue>> {
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

/// Flattens nested same-operator composites produced by left-associative
/// parsing (`a AND b AND c` becomes `And([a, b, c])`, not `And([And([a,b]),c])`).
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

pub(in crate::sql) fn parse_field_expr(expr: &Expr) -> Result<String> {
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

pub(in crate::sql) fn parse_value_expr(expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Value(vws) => Ok(SqlValue::Literal(parse_value(&vws.value)?)),
        Expr::Function(function) => parse_value_function(function),
        Expr::Identifier(ident) => {
            if ident.value.eq_ignore_ascii_case("current_timestamp") {
                Ok(SqlValue::CurrentTimestamp)
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
                        Ok(SqlValue::Literal(parse_numeric(&with_sign)?))
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

fn parse_value_function(function: &sqlparser::ast::Function) -> Result<SqlValue> {
    let name = super::object_name_to_string(&function.name);
    let name_lower = name.to_ascii_lowercase();
    let args = parse_function_args(&function.args)?;

    match name_lower.as_str() {
        "ref" | "reference" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "ref(path) expects exactly one argument".to_string(),
                ));
            }
            let path = super::expr_to_string_literal(&args[0], "ref(path)")?;
            Ok(SqlValue::Reference(path))
        }
        "timestamp" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "timestamp(value) expects exactly one argument".to_string(),
                ));
            }
            let value = super::expr_to_string_literal(&args[0], "timestamp(value)")?;
            let parsed = DateTime::parse_from_rfc3339(&value)
                .map_err(|e| FireqlError::InvalidQuery(format!("Invalid timestamp: {e}")))?;
            Ok(SqlValue::Timestamp(parsed.with_timezone(&Utc)))
        }
        "current_timestamp" => {
            if !args.is_empty() {
                return Err(FireqlError::Unsupported(
                    "CURRENT_TIMESTAMP expects no arguments".to_string(),
                ));
            }
            Ok(SqlValue::CurrentTimestamp)
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

pub(in crate::sql) fn parse_numeric(input: &str) -> Result<JsonValue> {
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
