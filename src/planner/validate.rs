//! Firestore クエリ制約の検証。IN / NOT IN / array-contains 系の disjunction
//! 上限、不等号フィルタと ORDER BY の整合、フィルタ種別の相互排他をチェックする。

use super::{MAX_ARRAY_CONTAINS_ANY_VALUES, MAX_IN_VALUES, MAX_NOT_IN_VALUES};
use crate::error::{FireqlError, Result};
use crate::sql::{CompareOp, FilterExpr, OrderBy};
use std::collections::BTreeSet;

pub(in crate::planner) fn validate_query_constraints(
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
) -> Result<()> {
    let stats = FilterStats::collect(filter)?;
    stats.validate(order_by)
}

/// Per-branch statistics about disjunctive filters. Firestore's limits on IN /
/// NOT IN / array filters are per query branch, so OR children are collected
/// independently and validated separately from the AND-level combination.
#[derive(Default)]
struct FilterStats {
    inequality_fields: BTreeSet<String>,
    in_fields: Vec<String>,
    not_in_fields: Vec<String>,
    not_eq_fields: Vec<String>,
    in_lengths: Vec<usize>,
    not_in_lengths: Vec<usize>,
    array_contains_fields: Vec<String>,
    array_contains_any_fields: Vec<String>,
    array_contains_any_lengths: Vec<usize>,
}

impl FilterStats {
    /// Collects and validates a whole filter tree. Each OR branch is
    /// validated independently (Firestore disjunction limits are per branch)
    /// and does not contribute to the parent's combination checks.
    fn collect(filter: Option<&FilterExpr>) -> Result<Self> {
        let mut stats = Self::default();
        if let Some(filter) = filter {
            stats.collect_branch(filter)?;
        }
        Ok(stats)
    }

    fn collect_branch(&mut self, filter: &FilterExpr) -> Result<()> {
        match filter {
            FilterExpr::Or(filters) => {
                for f in filters {
                    let mut branch = Self::default();
                    branch.collect_branch(f)?;
                    branch.validate(&[])?;
                }
                Ok(())
            }
            other => {
                self.collect_conjunction(other);
                Ok(())
            }
        }
    }

    fn collect_conjunction(&mut self, filter: &FilterExpr) {
        match filter {
            FilterExpr::And(filters) | FilterExpr::Or(filters) => {
                for f in filters {
                    self.collect_conjunction(f);
                }
            }
            FilterExpr::Compare { field, op, .. } => match op {
                CompareOp::Lt | CompareOp::LtEq | CompareOp::Gt | CompareOp::GtEq => {
                    self.inequality_fields.insert(field.clone());
                }
                CompareOp::NotEq => {
                    self.inequality_fields.insert(field.clone());
                    self.not_eq_fields.push(field.clone());
                }
                CompareOp::Eq => {}
            },
            FilterExpr::ArrayContains { field, .. } => {
                self.array_contains_fields.push(field.clone());
            }
            FilterExpr::ArrayContainsAny { field, values } => {
                self.array_contains_any_fields.push(field.clone());
                self.array_contains_any_lengths.push(values.len());
            }
            FilterExpr::InList {
                field,
                values,
                negated,
            } => {
                if *negated {
                    self.inequality_fields.insert(field.clone());
                    self.not_in_fields.push(field.clone());
                    self.not_in_lengths.push(values.len());
                } else {
                    self.in_fields.push(field.clone());
                    self.in_lengths.push(values.len());
                }
            }
            FilterExpr::Unary { .. } => {}
        }
    }

    fn validate(&self, order_by: &[OrderBy]) -> Result<()> {
        self.validate_inequality_order_by(order_by)?;
        self.validate_combinations()
    }

    fn validate_inequality_order_by(&self, order_by: &[OrderBy]) -> Result<()> {
        if self.inequality_fields.len() > 1 {
            return Err(FireqlError::InvalidQuery(
                "Firestore allows inequality filters on a single field only".to_string(),
            ));
        }

        if let Some(field) = self.inequality_fields.iter().next() {
            if !order_by.is_empty() {
                let first = &order_by[0].field;
                if first != field {
                    return Err(FireqlError::InvalidQuery(format!(
                        "When ORDER BY is used with an inequality filter, the first ORDER BY field must match the inequality field: expected `{field}`, got `{first}`"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_combinations(&self) -> Result<()> {
        if self.in_fields.len() > 1 {
            return Err(FireqlError::InvalidQuery(
                "Firestore allows at most one IN filter".to_string(),
            ));
        }
        if self.in_lengths.contains(&0) {
            return Err(FireqlError::InvalidQuery(
                "IN requires at least one value".to_string(),
            ));
        }
        if self.in_lengths.iter().any(|len| *len > MAX_IN_VALUES) {
            return Err(FireqlError::InvalidQuery(format!(
                "IN supports up to {MAX_IN_VALUES} values"
            )));
        }
        if self.not_in_fields.len() > 1 {
            return Err(FireqlError::InvalidQuery(
                "Firestore allows at most one NOT IN filter".to_string(),
            ));
        }
        if self.not_in_lengths.contains(&0) {
            return Err(FireqlError::InvalidQuery(
                "NOT IN requires at least one value".to_string(),
            ));
        }
        if self
            .not_in_lengths
            .iter()
            .any(|len| *len > MAX_NOT_IN_VALUES)
        {
            return Err(FireqlError::InvalidQuery(format!(
                "NOT IN supports up to {MAX_NOT_IN_VALUES} values"
            )));
        }
        if self.not_eq_fields.len() > 1 {
            return Err(FireqlError::InvalidQuery(
                "Firestore allows at most one != filter".to_string(),
            ));
        }

        if !self.not_in_fields.is_empty()
            && (!self.in_fields.is_empty() || !self.not_eq_fields.is_empty())
        {
            return Err(FireqlError::InvalidQuery(
                "NOT IN cannot be combined with IN or !=".to_string(),
            ));
        }

        if self.array_contains_fields.len() + self.array_contains_any_fields.len() > 1 {
            return Err(FireqlError::InvalidQuery(
                "Firestore allows at most one array-contains / array-contains-any filter"
                    .to_string(),
            ));
        }
        if !self.array_contains_any_fields.is_empty()
            && (!self.in_fields.is_empty() || !self.not_in_fields.is_empty())
        {
            return Err(FireqlError::InvalidQuery(
                "array-contains-any cannot be combined with IN or NOT IN".to_string(),
            ));
        }
        if !self.not_in_fields.is_empty()
            && (!self.array_contains_fields.is_empty()
                || !self.array_contains_any_fields.is_empty())
        {
            return Err(FireqlError::InvalidQuery(
                "NOT IN cannot be combined with array-contains filters".to_string(),
            ));
        }
        if self.array_contains_any_lengths.contains(&0) {
            return Err(FireqlError::InvalidQuery(
                "array-contains-any requires at least one value".to_string(),
            ));
        }
        if self
            .array_contains_any_lengths
            .iter()
            .any(|len| *len > MAX_ARRAY_CONTAINS_ANY_VALUES)
        {
            return Err(FireqlError::InvalidQuery(format!(
                "array-contains-any supports up to {MAX_ARRAY_CONTAINS_ANY_VALUES} values"
            )));
        }

        Ok(())
    }
}
