use crate::error::{FireqlError, Result};
use crate::joiner::{chunk_keys, extract_join_keys, hash_join, JoinParams};
use crate::output::{DocOutput, FireqlOutput};
use crate::planner::{
    build_aggregated_query_params, build_query_params, json_to_firestore_value_with_context,
};
use crate::sql::{
    CollectionSpec, FilterExpr, JoinSpec, OrderBy, Projection, SelectProjection, StatementAst,
    FIREQL_CURRENT_TS_KEY,
};
use crate::value::FireqlValue;

use firestore::errors::FirestoreError;
use firestore::{
    firestore_document_from_map, FirestoreAggregatedQuerySupport, FirestoreDb,
    FirestoreQuerySupport,
};
use futures::stream::{self, StreamExt};
use gcloud_sdk::google::firestore::v1::{document_transform, write, DocumentMask, Write};
use serde_json::Value as JsonValue;
use std::collections::HashSet;

const BATCH_LIMIT: usize = 500;
const FIRESTORE_IN_LIMIT: usize = 10;

struct FireqlWrite(Write);

impl TryInto<Write> for FireqlWrite {
    type Error = FirestoreError;

    fn try_into(self) -> std::result::Result<Write, Self::Error> {
        Ok(self.0)
    }
}

pub async fn execute(
    db: &FirestoreDb,
    stmt: StatementAst,
    batch_parallelism: usize,
) -> Result<FireqlOutput> {
    match stmt {
        StatementAst::Select(select) => execute_select(db, select).await,
        StatementAst::Update(update) => {
            let op = BatchOp::Update(build_update_parts(
                &update.assignments,
                Some(db.get_documents_path().as_str()),
            )?);
            execute_batch_write(
                db,
                &update.collection,
                &update.filter,
                &update.order_by,
                update.limit,
                batch_parallelism,
                op,
            )
            .await
        }
        StatementAst::Delete(delete) => {
            execute_batch_write(
                db,
                &delete.collection,
                &delete.filter,
                &delete.order_by,
                delete.limit,
                batch_parallelism,
                BatchOp::Delete,
            )
            .await
        }
    }
}

async fn execute_select(
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
            Ok(FireqlOutput::Rows(docs_to_output(&docs)?))
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
                .map(|doc| FireqlValue::from_document_fields(&doc.fields))
                .unwrap_or_default();
            Ok(FireqlOutput::Aggregation(data))
        }
    }
}

async fn execute_join_select(
    db: &FirestoreDb,
    stmt: &crate::sql::SelectStatement,
    joins: &[JoinSpec],
) -> Result<FireqlOutput> {
    let left_params = build_query_params(
        &stmt.collection,
        stmt.filter.as_ref(),
        &stmt.order_by,
        stmt.limit,
        None,
        Some(db.get_documents_path().as_str()),
    )?;
    let left_docs_raw = db.query_doc(left_params).await?;
    let left_docs = docs_to_output(&left_docs_raw)?;

    let left_prefix = stmt.alias.as_deref().unwrap_or(&stmt.collection.name);
    let mut current_result = left_docs;
    let mut is_joined = false;

    for join in joins {
        let effective_left_field = if is_joined {
            let alias = join.left_alias.as_deref().unwrap_or(left_prefix);
            format!("{alias}.{}", join.left_field)
        } else {
            join.left_field.clone()
        };

        let keys = extract_join_keys(&current_result, &effective_left_field);
        if keys.is_empty() && join.join_type == crate::sql::JoinType::Inner {
            return Ok(FireqlOutput::Rows(vec![]));
        }

        let right_field = join.right_field.clone();
        let chunks = chunk_keys(&keys, FIRESTORE_IN_LIMIT);
        let mut right_docs = Vec::new();

        for chunk in chunks {
            let in_values: Vec<serde_json::Value> =
                chunk.iter().map(|k| k.to_json_value()).collect();

            let in_filter = FilterExpr::InList {
                field: right_field.clone(),
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
            right_docs.extend(docs_to_output(&chunk_docs)?);
        }

        let right_prefix = join.right_alias.as_deref().unwrap_or(&join.collection.name);

        current_result = hash_join(
            &current_result,
            &right_docs,
            &JoinParams {
                left_field: &effective_left_field,
                right_field: &join.right_field,
                join_type: join.join_type,
                left_prefix,
                right_prefix,
                prefix_left: !is_joined,
            },
        );

        is_joined = true;
    }

    if let SelectProjection::Fields(Projection::Fields(ref fields)) = stmt.projection {
        let field_set: HashSet<&str> = fields.iter().map(String::as_str).collect();
        for doc in &mut current_result {
            doc.data.retain(|k, _| field_set.contains(k.as_str()));
        }
    }

    Ok(FireqlOutput::Rows(current_result))
}

#[derive(Clone)]
enum BatchOp {
    Update(UpdateParts),
    Delete,
}

async fn execute_batch_write(
    db: &FirestoreDb,
    collection: &CollectionSpec,
    filter: &FilterExpr,
    order_by: &[OrderBy],
    limit: Option<u32>,
    batch_parallelism: usize,
    op: BatchOp,
) -> Result<FireqlOutput> {
    let params = build_query_params(
        collection,
        Some(filter),
        order_by,
        limit,
        None,
        Some(db.get_documents_path().as_str()),
    )?;

    // NOTE: All matching documents are loaded into memory before batching.
    // For large result sets, callers should use LIMIT to bound memory usage.
    let docs = db.query_doc(params).await?;
    let doc_names: Vec<String> = docs.into_iter().map(|doc| doc.name).collect();

    let chunks = doc_names
        .chunks(BATCH_LIMIT)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    let mut affected = 0u64;
    let mut first_error: Option<FireqlError> = None;

    let mut stream = stream::iter(chunks.into_iter().map(|chunk| {
        let db = db.clone();
        let op = op.clone();
        async move {
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();
            for name in &chunk {
                let parts = parse_doc_name(name)?;
                let parent = parts.parent_path(db.get_documents_path().as_str());
                match &op {
                    BatchOp::Update(update_parts) => {
                        let doc_path = format!("{parent}/{}/{}", parts.collection, parts.id);
                        let update_doc =
                            firestore_document_from_map(&doc_path, update_parts.fields.clone())?;
                        batch.add(FireqlWrite(Write {
                            update_mask: Some(DocumentMask {
                                field_paths: update_parts.update_mask_fields.clone(),
                            }),
                            update_transforms: update_parts.transforms.clone(),
                            current_document: None,
                            operation: Some(write::Operation::Update(update_doc)),
                        }))?;
                    }
                    BatchOp::Delete => {
                        batch.delete_by_id_at(parent, &parts.collection, &parts.id, None)?;
                    }
                }
            }
            batch.write().await?;
            Ok::<usize, FireqlError>(chunk.len())
        }
    }))
    .buffer_unordered(batch_parallelism);

    while let Some(result) = stream.next().await {
        match result {
            Ok(count) => affected += count as u64,
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if let Some(err) = first_error {
        return Err(FireqlError::PartialFailure {
            affected,
            error: err.to_string(),
        });
    }

    Ok(FireqlOutput::Affected { affected })
}

#[derive(Clone)]
struct UpdateParts {
    update_mask_fields: Vec<String>,
    fields: Vec<(String, firestore::FirestoreValue)>,
    transforms: Vec<document_transform::FieldTransform>,
}

fn build_update_parts(
    assignments: &[(String, JsonValue)],
    base_doc_path: Option<&str>,
) -> Result<UpdateParts> {
    let mut update_mask_fields = Vec::with_capacity(assignments.len());
    let mut fields = Vec::with_capacity(assignments.len());
    let mut transforms = Vec::new();

    for (field, value) in assignments {
        if is_current_timestamp_value(value) {
            transforms.push(document_transform::FieldTransform {
                field_path: field.clone(),
                transform_type: Some(
                    document_transform::field_transform::TransformType::SetToServerValue(
                        document_transform::field_transform::ServerValue::RequestTime as i32,
                    ),
                ),
            });
            continue;
        }

        let fv = json_to_firestore_value_with_context(value, base_doc_path)?;
        fields.push((field.clone(), fv));
        update_mask_fields.push(field.clone());
    }

    Ok(UpdateParts {
        update_mask_fields,
        fields,
        transforms,
    })
}

fn is_current_timestamp_value(value: &JsonValue) -> bool {
    match value {
        JsonValue::Object(map) => map.contains_key(FIREQL_CURRENT_TS_KEY),
        _ => false,
    }
}

fn docs_to_output(docs: &[gcloud_sdk::google::firestore::v1::Document]) -> Result<Vec<DocOutput>> {
    docs.iter().map(doc_to_output).collect()
}

fn doc_to_output(doc: &gcloud_sdk::google::firestore::v1::Document) -> Result<DocOutput> {
    let parts = parse_doc_name(&doc.name)?;
    let data = FireqlValue::from_document_fields(&doc.fields);

    Ok(DocOutput {
        id: parts.id,
        path: parts.path,
        data,
    })
}

struct DocNameParts {
    id: String,
    path: String,
    collection: String,
    parent_full: Option<String>,
}

impl DocNameParts {
    fn parent_path<'a>(&'a self, default: &'a str) -> &'a str {
        self.parent_full.as_deref().unwrap_or(default)
    }
}

fn parse_doc_name(name: &str) -> Result<DocNameParts> {
    if name.is_empty() {
        return Err(FireqlError::InvalidDocName(name.to_string()));
    }

    let (prefix, relative) = name
        .split_once("/documents/")
        .ok_or_else(|| FireqlError::InvalidDocName(name.to_string()))?;

    let segments = relative.split('/').collect::<Vec<_>>();
    if segments.len() < 2 || segments.len() % 2 != 0 {
        return Err(FireqlError::InvalidDocName(name.to_string()));
    }

    let id = segments.last().unwrap().to_string();
    let collection = segments.get(segments.len() - 2).unwrap().to_string();

    let parent_full = if segments.len() == 2 {
        None
    } else {
        let parent_rel = segments[..segments.len() - 2].join("/");
        Some(format!("{prefix}/documents/{parent_rel}"))
    };

    Ok(DocNameParts {
        id,
        path: relative.to_string(),
        collection,
        parent_full,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::document_transform::field_transform::{
        ServerValue, TransformType,
    };
    use gcloud_sdk::google::firestore::v1::value::ValueType;

    #[test]
    fn update_parts_turn_current_timestamp_into_server_timestamp_transform() {
        let assignments = vec![(
            "updated_at".to_string(),
            JsonValue::Object(
                [(FIREQL_CURRENT_TS_KEY.to_string(), JsonValue::Bool(true))]
                    .into_iter()
                    .collect(),
            ),
        )];

        let parts = build_update_parts(&assignments, None).expect("parts");

        assert!(parts.update_mask_fields.is_empty());
        assert!(parts.fields.is_empty());
        assert_eq!(parts.transforms.len(), 1);
        assert_eq!(parts.transforms[0].field_path, "updated_at");
        assert_eq!(
            parts.transforms[0].transform_type,
            Some(TransformType::SetToServerValue(
                ServerValue::RequestTime as i32
            ))
        );
    }

    #[test]
    fn update_parts_mix_normal_fields_and_server_timestamp_transform() {
        let assignments = vec![
            (
                "status".to_string(),
                JsonValue::String("active".to_string()),
            ),
            (
                "updated_at".to_string(),
                JsonValue::Object(
                    [(FIREQL_CURRENT_TS_KEY.to_string(), JsonValue::Bool(true))]
                        .into_iter()
                        .collect(),
                ),
            ),
        ];

        let parts = build_update_parts(&assignments, None).expect("parts");

        assert_eq!(parts.update_mask_fields, vec!["status".to_string()]);
        assert_eq!(parts.fields.len(), 1);
        assert_eq!(parts.fields[0].0, "status");
        assert_eq!(
            parts.fields[0].1.value.value_type,
            Some(ValueType::StringValue("active".to_string()))
        );
        assert_eq!(parts.transforms.len(), 1);
        assert_eq!(parts.transforms[0].field_path, "updated_at");
        assert_eq!(
            parts.transforms[0].transform_type,
            Some(TransformType::SetToServerValue(
                ServerValue::RequestTime as i32
            ))
        );
    }
}
