mod parser;
mod rewrite;
#[cfg(test)]
mod tests;

pub use parser::parse_sql;

use crate::error::{FireqlError, Result};
use serde_json::Value as JsonValue;

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
