use crate::error::{FireqlError, Result};
use crate::joiner::{chunk_keys, extract_join_keys, hash_join, JoinParams};
use crate::output::{DocOutput, FireqlOutput};
use crate::planner::{
    build_aggregated_query_params, build_query_params, json_to_firestore_value_with_context,
};
use crate::sql::{
    CollectionSpec, FilterExpr, InsertSelectStatement, JoinSpec, OrderBy, Projection,
    SelectProjection, StatementAst, FIREQL_CURRENT_TS_KEY,
};
use crate::value::FireqlValue;

use firestore::errors::FirestoreError;
use firestore::{
    firestore_document_from_map, FirestoreAggregatedQuerySupport, FirestoreDb,
    FirestoreQuerySupport,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::{
    document_transform, precondition, write, Document, DocumentMask, Precondition, Write,
};
use gcloud_sdk::google::rpc::Status;
use rand::distr::{Alphanumeric, SampleString};
use serde_json::Value as JsonValue;
use std::collections::HashSet;

const BATCH_LIMIT: usize = 500;
// Firestore allows up to 30 disjunctions in an `in` filter; keep in sync
// with MAX_IN_VALUES in planner.rs.
const FIRESTORE_IN_LIMIT: usize = 30;

/// Split owned items into `BATCH_LIMIT`-sized batches, moving each item into
/// its batch rather than cloning.
fn into_batches<T>(items: Vec<T>) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut iter = items.into_iter().peekable();
    while iter.peek().is_some() {
        batches.push(iter.by_ref().take(BATCH_LIMIT).collect());
    }
    batches
}

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
        StatementAst::InsertSelect(insert) => {
            execute_insert_select(db, insert, batch_parallelism).await
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

        let keys = extract_join_keys(&current_result, &effective_left_field)
            .map_err(FireqlError::Unsupported)?;
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
            let in_values: Vec<serde_json::Value> = if join.right_field == "__name__" {
                chunk
                    .iter()
                    .map(|k| match k {
                        crate::joiner::JoinKey::String(s) => {
                            serde_json::Value::String(format!("{doc_path}/{s}"))
                        }
                        _ => k.to_json_value(),
                    })
                    .collect()
            } else {
                chunk.iter().map(|k| k.to_json_value()).collect()
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
        )
        .map_err(FireqlError::Unsupported)?;

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

#[derive(Clone)]
enum BatchOp {
    Update(UpdateParts),
    Delete,
}

async fn execute_insert_select(
    db: &FirestoreDb,
    stmt: InsertSelectStatement,
    batch_parallelism: usize,
) -> Result<FireqlOutput> {
    let SelectProjection::Fields(projection) = &stmt.source.projection else {
        return Err(FireqlError::Unsupported(
            "Aggregation is not supported in INSERT SELECT".to_string(),
        ));
    };
    let query_projection = insert_select_query_projection(projection);
    let params = build_query_params(
        &stmt.source.collection,
        stmt.source.filter.as_ref(),
        &stmt.source.order_by,
        stmt.source.limit,
        query_projection.as_ref(),
        Some(db.get_documents_path().as_str()),
    )?;

    let docs = db.query_doc(params).await?;
    if docs.is_empty() {
        return Ok(FireqlOutput::Affected { affected: 0 });
    }

    let chunks = into_batches(docs);
    let stream = stream::iter(chunks.into_iter().map(|chunk| {
        let db = db.clone();
        let collection = stmt.collection.clone();
        let columns = stmt.columns.clone();
        let projection = projection.clone();
        async move {
            let parent = insert_parent_path(&db, &collection);
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();
            let chunk_len = chunk.len();

            for doc in chunk {
                let parts = build_insert_select_parts(doc, columns.as_deref(), &projection)?;
                let id = parts.id.unwrap_or_else(generate_document_id);
                let doc_path = format!("{parent}/{}/{}", collection.collection_id, id);
                let insert_doc = firestore_document_from_map(&doc_path, parts.fields)?;
                batch.add(FireqlWrite(Write {
                    update_mask: None,
                    update_transforms: vec![],
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(false)),
                    }),
                    operation: Some(write::Operation::Update(insert_doc)),
                }))?;
            }

            let response = batch.write().await?;
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk_len,
            ))
        }
    }))
    .buffer_unordered(batch_parallelism);

    drain_batch_results(stream).await
}

fn insert_select_query_projection(projection: &Projection) -> Option<Projection> {
    match projection {
        Projection::All => Some(Projection::All),
        Projection::Fields(fields) => {
            let fields = fields
                .iter()
                .filter(|field| field.as_str() != "__name__")
                .cloned()
                .collect::<Vec<_>>();
            if fields.is_empty() {
                None
            } else {
                Some(Projection::Fields(fields))
            }
        }
    }
}

fn insert_parent_path(db: &FirestoreDb, collection: &CollectionSpec) -> String {
    match &collection.parent_path {
        Some(parent) => format!("{}/{parent}", db.get_documents_path()),
        None => db.get_documents_path().to_string(),
    }
}

struct InsertSelectParts {
    id: Option<String>,
    fields: Vec<(String, firestore::FirestoreValue)>,
}

fn build_insert_select_parts(
    doc: Document,
    columns: Option<&[String]>,
    projection: &Projection,
) -> Result<InsertSelectParts> {
    match (columns, projection) {
        (None, Projection::All) => Ok(InsertSelectParts {
            id: None,
            fields: doc
                .fields
                .into_iter()
                .map(|(field, value)| (field, firestore::FirestoreValue::from(value)))
                .collect(),
        }),
        (Some(columns), Projection::Fields(source_fields)) => {
            let doc_id = parse_doc_name(&doc.name)?.id;
            let mut id = None;
            let mut fields = Vec::new();

            for (target, source) in columns.iter().zip(source_fields) {
                if target == "__name__" {
                    if source != "__name__" {
                        return Err(FireqlError::InvalidQuery(
                            "__name__ destination column requires __name__ source field"
                                .to_string(),
                        ));
                    }
                    id = Some(doc_id.clone());
                    continue;
                }

                if source == "__name__" {
                    let value: firestore::FirestoreValue = JsonValue::String(doc_id.clone()).into();
                    fields.push((target.clone(), value));
                } else if let Some(value) = doc.fields.get(source) {
                    fields.push((
                        target.clone(),
                        firestore::FirestoreValue::from(value.clone()),
                    ));
                }
            }

            Ok(InsertSelectParts { id, fields })
        }
        _ => Err(FireqlError::InvalidQuery(
            "Invalid INSERT SELECT projection".to_string(),
        )),
    }
}

fn generate_document_id() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 20)
}

/// The Firestore `BatchWrite` RPC is non-atomic: it returns `Ok` even when
/// individual writes fail, reporting each result in `statuses` (code 0 = OK).
/// Count only the writes that actually succeeded and surface the first failure,
/// so e.g. an INSERT colliding with an existing id (FAILED_PRECONDITION) is not
/// silently reported as affected.
fn count_batch_outcome(statuses: &[Status], chunk_len: usize) -> (usize, Option<String>) {
    let mut failures = 0usize;
    let mut first_error = None;
    for status in statuses {
        if status.code != 0 {
            failures += 1;
            if first_error.is_none() {
                first_error = Some(format!(
                    "write failed (code {}): {}",
                    status.code, status.message
                ));
            }
        }
    }
    (chunk_len.saturating_sub(failures), first_error)
}

/// Drains a `buffer_unordered` stream of per-chunk write results, summing the
/// succeeded count and keeping only the first error. Any error downgrades the
/// whole statement to `PartialFailure` so callers never see a partial success
/// reported as a full success.
async fn drain_batch_results(
    mut stream: impl futures::Stream<Item = std::result::Result<(usize, Option<String>), FireqlError>>
        + Unpin,
) -> Result<FireqlOutput> {
    let mut affected = 0u64;
    let mut first_error: Option<String> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok((count, write_error)) => {
                affected += count as u64;
                if first_error.is_none() {
                    first_error = write_error;
                }
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err.to_string());
                }
            }
        }
    }

    if let Some(error) = first_error {
        return Err(FireqlError::PartialFailure { affected, error });
    }

    Ok(FireqlOutput::Affected { affected })
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

    // Stream the query so only document names are kept in memory; the full
    // document bodies are dropped as each result arrives.
    let doc_names: Vec<String> = db
        .stream_query_doc_with_errors(params)
        .await?
        .map_ok(|doc| doc.name)
        .try_collect()
        .await?;

    let chunks = into_batches(doc_names);
    let stream = stream::iter(chunks.into_iter().map(|chunk| {
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
            let response = batch.write().await?;
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk.len(),
            ))
        }
    }))
    .buffer_unordered(batch_parallelism);

    drain_batch_results(stream).await
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

fn docs_to_output(
    docs: Vec<gcloud_sdk::google::firestore::v1::Document>,
) -> Result<Vec<DocOutput>> {
    docs.into_iter().map(doc_to_output).collect()
}

fn doc_to_output(doc: gcloud_sdk::google::firestore::v1::Document) -> Result<DocOutput> {
    let parts = parse_doc_name(&doc.name)?;
    let data = FireqlValue::from_document_fields(doc.fields);

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
    use gcloud_sdk::google::firestore::v1::{Document, Value};
    use std::collections::HashMap;

    fn document(name: &str, fields: Vec<(&str, Value)>) -> Document {
        Document {
            name: name.to_string(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>(),
            create_time: None,
            update_time: None,
        }
    }

    fn string_value(value: &str) -> Value {
        Value {
            value_type: Some(ValueType::StringValue(value.to_string())),
        }
    }

    fn integer_value(value: i64) -> Value {
        Value {
            value_type: Some(ValueType::IntegerValue(value)),
        }
    }

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

    #[test]
    fn insert_select_parts_copy_all_fields_with_auto_id() {
        let doc = document(
            "projects/p/databases/(default)/documents/users/u1",
            vec![
                ("name", string_value("Alice")),
                ("score", integer_value(10)),
            ],
        );

        let parts = build_insert_select_parts(doc, None, &Projection::All).expect("parts");

        assert!(parts.id.is_none());
        assert_eq!(parts.fields.len(), 2);
        assert!(parts.fields.iter().any(|(field, value)| {
            field == "name"
                && value.value.value_type == Some(ValueType::StringValue("Alice".into()))
        }));
        assert!(parts.fields.iter().any(|(field, value)| {
            field == "score" && value.value.value_type == Some(ValueType::IntegerValue(10))
        }));
    }

    #[test]
    fn insert_select_parts_preserve_document_id_from_name_column() {
        let doc = document(
            "projects/p/databases/(default)/documents/users/u1",
            vec![("name", string_value("Alice"))],
        );
        let columns = vec!["__name__".to_string(), "name".to_string()];
        let source_fields = vec!["__name__".to_string(), "name".to_string()];

        let parts =
            build_insert_select_parts(doc, Some(&columns), &Projection::Fields(source_fields))
                .expect("parts");

        assert_eq!(parts.id.as_deref(), Some("u1"));
        assert_eq!(parts.fields.len(), 1);
        assert_eq!(parts.fields[0].0, "name");
        assert_eq!(
            parts.fields[0].1.value.value_type,
            Some(ValueType::StringValue("Alice".into()))
        );
    }

    #[test]
    fn insert_select_parts_can_rename_explicit_fields() {
        let doc = document(
            "projects/p/databases/(default)/documents/users/u1",
            vec![("name", string_value("Alice"))],
        );
        let columns = vec!["archived_name".to_string()];
        let source_fields = vec!["name".to_string()];

        let parts =
            build_insert_select_parts(doc, Some(&columns), &Projection::Fields(source_fields))
                .expect("parts");

        assert!(parts.id.is_none());
        assert_eq!(parts.fields.len(), 1);
        assert_eq!(parts.fields[0].0, "archived_name");
        assert_eq!(
            parts.fields[0].1.value.value_type,
            Some(ValueType::StringValue("Alice".into()))
        );
    }

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

    fn status(code: i32, message: &str) -> Status {
        Status {
            code,
            message: message.to_string(),
            details: vec![],
        }
    }

    #[test]
    fn batch_outcome_all_success() {
        let statuses = vec![status(0, ""), status(0, ""), status(0, "")];
        let (succeeded, err) = count_batch_outcome(&statuses, 3);
        assert_eq!(succeeded, 3);
        assert!(err.is_none());
    }

    #[test]
    fn batch_outcome_partial_failure_counts_only_success() {
        let statuses = vec![status(0, ""), status(6, "already exists"), status(0, "")];
        let (succeeded, err) = count_batch_outcome(&statuses, 3);
        assert_eq!(succeeded, 2);
        assert!(err.unwrap().contains("already exists"));
    }

    #[test]
    fn batch_outcome_empty_statuses_assumes_success() {
        let (succeeded, err) = count_batch_outcome(&[], 5);
        assert_eq!(succeeded, 5);
        assert!(err.is_none());
    }

    #[tokio::test]
    async fn drain_batch_results_sums_affected_on_success() {
        let stream = stream::iter(vec![Ok::<_, FireqlError>((2, None)), Ok((3, None))]);
        let output = drain_batch_results(stream).await.unwrap();
        match output {
            FireqlOutput::Affected { affected } => assert_eq!(affected, 5),
            other => panic!("expected affected output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drain_batch_results_reports_first_error_as_partial_failure() {
        let stream = stream::iter(vec![
            Ok::<_, FireqlError>((2, None)),
            Ok((1, Some("boom".to_string()))),
            Err(FireqlError::Format("io".to_string())),
        ]);
        let err = drain_batch_results(stream).await.unwrap_err();
        match err {
            FireqlError::PartialFailure { affected, error } => {
                assert_eq!(affected, 3);
                assert_eq!(error, "boom");
            }
            other => panic!("expected partial failure, got {other:?}"),
        }
    }
}
