use super::doc_name::doc_to_output;
use crate::error::{FireqlError, Result};
use crate::joiner::{chunk_keys, extract_join_keys, hash_join, JoinParams};
use crate::output::{DocOutput, FireqlOutput};
use crate::planner::{plan_aggregation, plan_select, PlannedSelect, MAX_IN_VALUES};
use crate::sql::{FilterExpr, JoinSpec, Projection, SelectProjection, SqlValue};
use crate::value::FireqlValue;
use firestore::FirestoreDb;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use gcloud_sdk::google::firestore::v1::Document;
use std::collections::HashSet;

pub(super) async fn stream_planned_select<'b>(
    db: &'b FirestoreDb,
    planned: PlannedSelect,
) -> Result<BoxStream<'b, firestore::FirestoreResult<Document>>> {
    let mut q = db.fluent().select();
    if let Some(fields) = planned.return_only_fields {
        q = q.fields(fields);
    }
    let mut q = q.from(planned.collection_id);
    if let Some(parent) = planned.parent {
        q = q.parent(parent);
    }
    if planned.all_descendants {
        q = q.all_descendants();
    }
    if let Some(filter) = planned.filter {
        q = q.filter(move |_| Some(filter.clone()));
    }
    if !planned.order_by.is_empty() {
        q = q.order_by(planned.order_by);
    }
    if let Some(limit) = planned.limit {
        q = q.limit(limit);
    }
    Ok(q.stream_query_with_errors().await?)
}

pub(super) async fn execute_select(
    db: &FirestoreDb,
    stmt: crate::sql::SelectStatement,
) -> Result<FireqlOutput> {
    if let Some(ref joins) = stmt.joins {
        return execute_join_select(db, &stmt, joins).await;
    }

    match &stmt.projection {
        SelectProjection::Fields(projection) => {
            let planned = plan_select(
                &stmt.collection,
                stmt.filter.as_ref(),
                &stmt.order_by,
                stmt.limit,
                Some(projection),
                Some(db.get_documents_path().as_str()),
            )?;

            // Stream document bodies so callers that later write row-by-row
            // (JSON) do not force an intermediate all-at-once Firestore fetch
            // API; CSV/Table still buffer via FireqlOutput::Rows today (#28).
            let rows: Vec<DocOutput> = stream_planned_select(db, planned)
                .await?
                .map_err(FireqlError::from)
                .and_then(|doc| async move { doc_to_output(doc) })
                .try_collect()
                .await?;
            Ok(FireqlOutput::Rows(rows))
        }
        SelectProjection::Aggregations(aggregations) => {
            let planned = plan_aggregation(
                &stmt.collection,
                stmt.filter.as_ref(),
                &stmt.order_by,
                stmt.limit,
                aggregations,
                Some(db.get_documents_path().as_str()),
            )?;
            let mut q = db.fluent().select().from(planned.select.collection_id);
            if let Some(parent) = planned.select.parent {
                q = q.parent(parent);
            }
            if planned.select.all_descendants {
                q = q.all_descendants();
            }
            if let Some(filter) = planned.select.filter {
                q = q.filter(move |_| Some(filter.clone()));
            }
            let docs = q
                .aggregate(|agg| {
                    agg.fields(planned.aggregations.iter().map(|a| match a.func {
                        crate::sql::AggregationFunc::Count => agg.field(&a.alias).count(),
                        crate::sql::AggregationFunc::Sum => {
                            let field = a.field.as_deref().unwrap_or_default();
                            agg.field(&a.alias).sum(field)
                        }
                        crate::sql::AggregationFunc::Avg => {
                            let field = a.field.as_deref().unwrap_or_default();
                            agg.field(&a.alias).avg(field)
                        }
                    }))
                })
                .query()
                .await?;
            let data = docs
                .into_iter()
                .next()
                .map(|doc| FireqlValue::from_document_fields(doc.fields))
                .unwrap_or_default();
            Ok(FireqlOutput::Aggregation(data))
        }
    }
}

/// Rewrites every field reference in `filter` via `f`, recursing into
/// AND/OR composites. Shared by alias stripping and future field rewrites.
fn map_filter_fields(filter: &FilterExpr, f: &mut impl FnMut(&str) -> String) -> FilterExpr {
    match filter {
        FilterExpr::Compare { field, op, value } => FilterExpr::Compare {
            field: f(field),
            op: *op,
            value: value.clone(),
        },
        FilterExpr::ArrayContains { field, value } => FilterExpr::ArrayContains {
            field: f(field),
            value: value.clone(),
        },
        FilterExpr::ArrayContainsAny { field, values } => FilterExpr::ArrayContainsAny {
            field: f(field),
            values: values.clone(),
        },
        FilterExpr::InList {
            field,
            values,
            negated,
        } => FilterExpr::InList {
            field: f(field),
            values: values.clone(),
            negated: *negated,
        },
        FilterExpr::Unary { field, op } => FilterExpr::Unary {
            field: f(field),
            op: *op,
        },
        FilterExpr::And(exprs) => {
            FilterExpr::And(exprs.iter().map(|e| map_filter_fields(e, f)).collect())
        }
        FilterExpr::Or(exprs) => {
            FilterExpr::Or(exprs.iter().map(|e| map_filter_fields(e, f)).collect())
        }
    }
}

fn strip_alias_from_filter(filter: &FilterExpr, alias: &str) -> FilterExpr {
    let prefix = format!("{alias}.");
    map_filter_fields(filter, &mut |field| {
        field
            .strip_prefix(prefix.as_str())
            .unwrap_or(field)
            .to_string()
    })
}

/// Resolves the left-side join key for a join step against `current_result`.
///
/// `__name__` resolves to the leading table's `DocOutput.id`, which is
/// preserved across every join, so it stays unqualified when the ON clause
/// references the leading alias even on chained joins. When it references a
/// previously joined right table (e.g. `o.__name__`), `hash_join` preserves
/// that table's document id under `{alias}.__name__`, so the key resolves to
/// the prefixed data field. A qualifier that refers to neither the leading
/// table nor a joined table is rejected, since no row data can supply it.
/// Regular fields, by contrast, are prefixed with their alias on chained
/// joins because the left rows are already prefixed (e.g. `u.dept_id`).
fn effective_left_join_field(
    join: &JoinSpec,
    is_joined: bool,
    left_alias: &str,
    joined_names: &[String],
) -> Result<String> {
    if join.left_field == "__name__" {
        let qualifier = join.left_alias.as_deref().unwrap_or(left_alias);
        if is_joined && qualifier != left_alias {
            if joined_names.iter().any(|name| name == qualifier) {
                return Ok(format!("{qualifier}.__name__"));
            }
            return Err(FireqlError::Unsupported(format!(
                "JOIN on `{qualifier}.__name__` is not supported; `{qualifier}` refers to neither the leading table nor a previously joined table"
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
    let left_planned = plan_select(
        &stmt.collection,
        stripped_filter.as_ref(),
        &stmt.order_by,
        stmt.limit,
        None,
        Some(db.get_documents_path().as_str()),
    )?;
    let left_docs: Vec<DocOutput> = stream_planned_select(db, left_planned)
        .await?
        .map_err(FireqlError::from)
        .and_then(|doc| async move { doc_to_output(doc) })
        .try_collect()
        .await?;

    let mut current_result = left_docs;
    let mut is_joined = false;
    // これまでに結合した右側テーブルの別名 (別名がなければコレクション名)。
    // 後続の JOIN の ON 句が先行する右側テーブルの `__name__` を参照できるかの
    // 判定に使う。
    let mut joined_names: Vec<String> = Vec::with_capacity(joins.len());

    for join in joins {
        let effective_left_field =
            effective_left_join_field(join, is_joined, left_alias, &joined_names)?;

        let keys = extract_join_keys(&current_result, &effective_left_field)?;
        if keys.is_empty() && join.join_type == crate::sql::JoinType::Inner {
            return Ok(FireqlOutput::Rows(vec![]));
        }

        let right_docs = fetch_right_docs(db, join, &keys).await?;

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
        joined_names.push(right_prefix.to_string());
    }

    retain_projected_fields(&mut current_result, &stmt.projection);

    Ok(FireqlOutput::Rows(current_result))
}

/// Fetches the right-side documents matching any of `keys` for one join step,
/// querying Firestore in `MAX_IN_VALUES`-sized IN chunks.
async fn fetch_right_docs(
    db: &FirestoreDb,
    join: &JoinSpec,
    keys: &[crate::joiner::JoinKey],
) -> Result<Vec<DocOutput>> {
    let chunks = chunk_keys(keys, MAX_IN_VALUES);
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
        // `__name__` / document-key filters require a ReferenceValue on the
        // wire; a plain string literal is rejected by Firestore as
        // "__key__ filter value must be a Key".
        let in_values: Vec<SqlValue> = if join.right_field == "__name__" {
            chunk
                .iter()
                .map(|k| match k {
                    crate::joiner::JoinKey::String(s) => {
                        SqlValue::Reference(format!("{doc_path}/{s}"))
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

        let right_planned = plan_select(
            &join.collection,
            Some(&in_filter),
            &[],
            None,
            None,
            Some(db.get_documents_path().as_str()),
        )?;

        let chunk_docs: Vec<DocOutput> = stream_planned_select(db, right_planned)
            .await?
            .map_err(FireqlError::from)
            .and_then(|doc| async move { doc_to_output(doc) })
            .try_collect()
            .await?;
        right_docs.extend(chunk_docs);
    }

    Ok(right_docs)
}

/// Narrows joined rows to the explicitly projected fields. A requested field
/// matches either an exact (possibly alias-prefixed) key or the suffix of a
/// prefixed key, so `SELECT name` keeps both `name` and `u.name`. A no-op for
/// wildcard projections.
fn retain_projected_fields(rows: &mut [DocOutput], projection: &SelectProjection) {
    let SelectProjection::Fields(Projection::Fields(fields)) = projection else {
        return;
    };

    let available_keys: HashSet<String> = rows
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

    for doc in rows {
        doc.data.retain(|k, _| retained_keys.contains(k));
    }
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
            effective_left_join_field(&name_join, false, "u", &[]).unwrap(),
            "__name__"
        );
        let field_join = join_spec("dept_id", "__name__", Some("u"));
        assert_eq!(
            effective_left_join_field(&field_join, false, "u", &[]).unwrap(),
            "dept_id"
        );
    }

    #[test]
    fn effective_left_field_chained_name_resolves_to_leading_id() {
        let join = join_spec("__name__", "user_id", Some("u"));
        assert_eq!(
            effective_left_join_field(&join, true, "u", &[]).unwrap(),
            "__name__"
        );
    }

    #[test]
    fn effective_left_field_chained_regular_field_is_prefixed() {
        let join = join_spec("dept_id", "__name__", Some("u"));
        assert_eq!(
            effective_left_join_field(&join, true, "u", &[]).unwrap(),
            "u.dept_id"
        );
    }

    #[test]
    fn effective_left_field_chained_prior_right_name_resolves_to_prefixed_key() {
        let join = join_spec("__name__", "order_id", Some("o"));
        let joined = vec!["o".to_string()];
        assert_eq!(
            effective_left_join_field(&join, true, "u", &joined).unwrap(),
            "o.__name__"
        );
    }

    #[test]
    fn effective_left_field_chained_unknown_name_qualifier_is_rejected() {
        let join = join_spec("__name__", "order_id", Some("x"));
        let joined = vec!["o".to_string()];
        let err = effective_left_join_field(&join, true, "u", &joined).unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    fn row(fields: &[(&str, FireqlValue)]) -> DocOutput {
        DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    #[test]
    fn retain_projected_fields_keeps_exact_and_alias_prefixed_matches() {
        let mut rows = vec![row(&[
            ("name", FireqlValue::String("Alice".into())),
            ("u.name", FireqlValue::String("Bob".into())),
            ("o.total", FireqlValue::Integer(1)),
        ])];

        retain_projected_fields(
            &mut rows,
            &SelectProjection::Fields(Projection::Fields(vec!["name".to_string()])),
        );

        let mut keys: Vec<_> = rows[0].data.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["name", "u.name"]);
    }

    #[test]
    fn retain_projected_fields_is_noop_for_wildcard() {
        let mut rows = vec![row(&[
            ("a", FireqlValue::Integer(1)),
            ("b", FireqlValue::Integer(2)),
        ])];

        retain_projected_fields(&mut rows, &SelectProjection::Fields(Projection::All));

        assert_eq!(rows[0].data.len(), 2);
    }
}
