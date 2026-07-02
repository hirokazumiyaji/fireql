use super::doc_name::docs_to_output;
use crate::error::{FireqlError, Result};
use crate::joiner::{chunk_keys, extract_join_keys, hash_join, JoinParams};
use crate::output::FireqlOutput;
use crate::planner::{build_aggregated_query_params, build_query_params};
use crate::sql::{FilterExpr, JoinSpec, Projection, SelectProjection, SqlValue};
use crate::value::FireqlValue;
use firestore::{FirestoreAggregatedQuerySupport, FirestoreDb, FirestoreQuerySupport};
use std::collections::HashSet;

// Firestore allows up to 30 disjunctions in an `in` filter; keep in sync
// with MAX_IN_VALUES in planner.rs.
const FIRESTORE_IN_LIMIT: usize = 30;

pub(super) async fn execute_select(
    db: &FirestoreDb,
    stmt: crate::sql::SelectStatement,
) -> Result<FireqlOutput> {
    if let Some(ref joins) = stmt.joins {
        return execute_join_select(db, &stmt, joins).await;
    }

    match &stmt.projection {
        SelectProjection::Fields(projection) => {
            let params = build_query_params(
                &stmt.collection,
                stmt.filter.as_ref(),
                &stmt.order_by,
                stmt.limit,
                Some(projection),
                Some(db.get_documents_path().as_str()),
            )?;

            let docs = db.query_doc(params).await?;
            Ok(FireqlOutput::Rows(docs_to_output(docs)?))
        }
        SelectProjection::Aggregations(aggregations) => {
            let params = build_aggregated_query_params(
                &stmt.collection,
                stmt.filter.as_ref(),
                &stmt.order_by,
                stmt.limit,
                aggregations,
                Some(db.get_documents_path().as_str()),
            )?;
            let docs = db.aggregated_query_doc(params).await?;
            let data = docs
                .into_iter()
                .next()
                .map(|doc| FireqlValue::from_document_fields(doc.fields))
                .unwrap_or_default();
            Ok(FireqlOutput::Aggregation(data))
        }
    }
}

fn strip_alias_from_filter(filter: &FilterExpr, alias: &str) -> FilterExpr {
    let strip_field = |field: &str| -> String {
        let prefix = format!("{alias}.");
        if field.starts_with(&prefix) {
            field[prefix.len()..].to_string()
        } else {
            field.to_string()
        }
    };

    match filter {
        FilterExpr::Compare { field, op, value } => FilterExpr::Compare {
            field: strip_field(field),
            op: *op,
            value: value.clone(),
        },
        FilterExpr::ArrayContains { field, value } => FilterExpr::ArrayContains {
            field: strip_field(field),
            value: value.clone(),
        },
        FilterExpr::ArrayContainsAny { field, values } => FilterExpr::ArrayContainsAny {
            field: strip_field(field),
            values: values.clone(),
        },
        FilterExpr::InList {
            field,
            values,
            negated,
        } => FilterExpr::InList {
            field: strip_field(field),
            values: values.clone(),
            negated: *negated,
        },
        FilterExpr::Unary { field, op } => FilterExpr::Unary {
            field: strip_field(field),
            op: *op,
        },
        FilterExpr::And(exprs) => FilterExpr::And(
            exprs
                .iter()
                .map(|e| strip_alias_from_filter(e, alias))
                .collect(),
        ),
        FilterExpr::Or(exprs) => FilterExpr::Or(
            exprs
                .iter()
                .map(|e| strip_alias_from_filter(e, alias))
                .collect(),
        ),
    }
}

/// Resolves the left-side join key for a join step against `current_result`.
///
/// `__name__` always resolves to the leading table's `DocOutput.id`, which is
/// preserved across every join, so it must stay unqualified even on chained
/// joins. Regular fields, by contrast, are prefixed with their alias on chained
/// joins because the left rows are already prefixed (e.g. `u.dept_id`).
/// A previous right table's `__name__` cannot be used: that id is never retained.
fn effective_left_join_field(join: &JoinSpec, is_joined: bool, left_alias: &str) -> Result<String> {
    if join.left_field == "__name__" {
        let qualifier = join.left_alias.as_deref().unwrap_or(left_alias);
        if is_joined && qualifier != left_alias {
            return Err(FireqlError::Unsupported(format!(
                "JOIN on `{qualifier}.__name__` is not supported; only the leading table's document id can be used as a join key"
            )));
        }
        Ok("__name__".to_string())
    } else if is_joined {
        let alias = join.left_alias.as_deref().unwrap_or(left_alias);
        Ok(format!("{alias}.{}", join.left_field))
    } else {
        Ok(join.left_field.clone())
    }
}

async fn execute_join_select(
    db: &FirestoreDb,
    stmt: &crate::sql::SelectStatement,
    joins: &[JoinSpec],
) -> Result<FireqlOutput> {
    let left_alias = stmt
        .alias
        .as_deref()
        .unwrap_or(&stmt.collection.collection_id);
    let stripped_filter = stmt
        .filter
        .as_ref()
        .map(|f| strip_alias_from_filter(f, left_alias));
    let left_params = build_query_params(
        &stmt.collection,
        stripped_filter.as_ref(),
        &stmt.order_by,
        stmt.limit,
        None,
        Some(db.get_documents_path().as_str()),
    )?;
    let left_docs_raw = db.query_doc(left_params).await?;
    let left_docs = docs_to_output(left_docs_raw)?;

    let mut current_result = left_docs;
    let mut is_joined = false;

    for join in joins {
        let effective_left_field = effective_left_join_field(join, is_joined, left_alias)?;

        let keys = extract_join_keys(&current_result, &effective_left_field)?;
        if keys.is_empty() && join.join_type == crate::sql::JoinType::Inner {
            return Ok(FireqlOutput::Rows(vec![]));
        }

        let chunks = chunk_keys(&keys, FIRESTORE_IN_LIMIT);
        let mut right_docs = Vec::new();

        let doc_path = match &join.collection.parent_path {
            Some(pp) => format!(
                "{}/{}/{}",
                db.get_documents_path(),
                pp,
                join.collection.collection_id
            ),
            None => format!(
                "{}/{}",
                db.get_documents_path(),
                join.collection.collection_id
            ),
        };

        for chunk in chunks {
            // The full document path is deliberately sent as a plain string
            // (`SqlValue::Literal`), not `SqlValue::Reference`: the string form
            // is what the emulator e2e join tests validate against `__name__`;
            // a ReferenceValue here would change the wire type untested.
            let in_values: Vec<SqlValue> = if join.right_field == "__name__" {
                chunk
                    .iter()
                    .map(|k| match k {
                        crate::joiner::JoinKey::String(s) => {
                            SqlValue::Literal(serde_json::Value::String(format!("{doc_path}/{s}")))
                        }
                        _ => SqlValue::Literal(k.to_json_value()),
                    })
                    .collect()
            } else {
                chunk
                    .iter()
                    .map(|k| SqlValue::Literal(k.to_json_value()))
                    .collect()
            };

            let in_filter = FilterExpr::InList {
                field: join.right_field.clone(),
                values: in_values,
                negated: false,
            };

            let right_params = build_query_params(
                &join.collection,
                Some(&in_filter),
                &[],
                None,
                None,
                Some(db.get_documents_path().as_str()),
            )?;

            let chunk_docs = db.query_doc(right_params).await?;
            right_docs.extend(docs_to_output(chunk_docs)?);
        }

        let right_prefix = join
            .right_alias
            .as_deref()
            .unwrap_or(&join.collection.collection_id);

        current_result = hash_join(
            &current_result,
            &right_docs,
            &JoinParams {
                left_field: &effective_left_field,
                right_field: &join.right_field,
                join_type: join.join_type,
                left_prefix: left_alias,
                right_prefix,
                prefix_left: !is_joined,
            },
        )?;

        is_joined = true;
    }

    if let SelectProjection::Fields(Projection::Fields(ref fields)) = stmt.projection {
        let available_keys: HashSet<String> = current_result
            .iter()
            .flat_map(|doc| doc.data.keys().cloned())
            .collect();

        let mut retained_keys: HashSet<String> = HashSet::new();
        for field in fields {
            if available_keys.contains(field) {
                retained_keys.insert(field.clone());
            }
            if !field.contains('.') {
                let suffix = format!(".{field}");
                for key in &available_keys {
                    if key.ends_with(&suffix) {
                        retained_keys.insert(key.clone());
                    }
                }
            }
        }

        for doc in &mut current_result {
            doc.data.retain(|k, _| retained_keys.contains(k));
        }
    }

    Ok(FireqlOutput::Rows(current_result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::CollectionSpec;

    fn join_spec(left_field: &str, right_field: &str, left_alias: Option<&str>) -> JoinSpec {
        JoinSpec {
            join_type: crate::sql::JoinType::Inner,
            collection: CollectionSpec {
                collection_id: "right".to_string(),
                parent_path: None,
                is_group: false,
            },
            left_field: left_field.to_string(),
            right_field: right_field.to_string(),
            left_alias: left_alias.map(|s| s.to_string()),
            right_alias: None,
        }
    }

    #[test]
    fn effective_left_field_first_join_uses_field_as_is() {
        let name_join = join_spec("__name__", "user_id", Some("u"));
        assert_eq!(
            effective_left_join_field(&name_join, false, "u").unwrap(),
            "__name__"
        );
        let field_join = join_spec("dept_id", "__name__", Some("u"));
        assert_eq!(
            effective_left_join_field(&field_join, false, "u").unwrap(),
            "dept_id"
        );
    }

    #[test]
    fn effective_left_field_chained_name_resolves_to_leading_id() {
        let join = join_spec("__name__", "user_id", Some("u"));
        assert_eq!(
            effective_left_join_field(&join, true, "u").unwrap(),
            "__name__"
        );
    }

    #[test]
    fn effective_left_field_chained_regular_field_is_prefixed() {
        let join = join_spec("dept_id", "__name__", Some("u"));
        assert_eq!(
            effective_left_join_field(&join, true, "u").unwrap(),
            "u.dept_id"
        );
    }

    #[test]
    fn effective_left_field_chained_prior_right_name_is_rejected() {
        let join = join_spec("__name__", "order_id", Some("o"));
        let err = effective_left_join_field(&join, true, "u").unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }
}
