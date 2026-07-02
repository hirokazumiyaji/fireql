use super::parser::{parse_insert_select, parse_query};
use super::{CollectionSpec, StatementAst};
use crate::error::{FireqlError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub(super) fn try_parse_insert_collection_function(input: &str) -> Result<Option<StatementAst>> {
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
