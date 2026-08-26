use super::doc_name::parse_doc_name;
use crate::error::{FireqlError, Result};
use crate::output::FireqlOutput;
use crate::planner::{build_query_params, sql_value_to_firestore};
use crate::sql::{CollectionSpec, FilterExpr, OrderBy, SqlValue};
use firestore::errors::FirestoreError;
use firestore::{firestore_document_from_map, FirestoreDb, FirestoreQuerySupport};
use futures::stream::{self, StreamExt, TryStreamExt};
use gcloud_sdk::google::firestore::v1::{document_transform, write, DocumentMask, Write};
use gcloud_sdk::google::rpc::Status;

pub(super) const BATCH_LIMIT: usize = 500;

/// Split owned items into `BATCH_LIMIT`-sized batches, moving each item into
/// its batch rather than cloning.
pub(super) fn into_batches<T>(items: Vec<T>) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut iter = items.into_iter().peekable();
    while iter.peek().is_some() {
        batches.push(iter.by_ref().take(BATCH_LIMIT).collect());
    }
    batches
}

pub(super) struct FireqlWrite(pub(super) Write);

impl TryInto<Write> for FireqlWrite {
    type Error = FirestoreError;

    fn try_into(self) -> std::result::Result<Write, Self::Error> {
        Ok(self.0)
    }
}

#[derive(Clone)]
pub(super) enum BatchOp {
    Update(UpdateParts),
    Delete,
}

#[derive(Clone)]
pub(super) struct UpdateParts {
    update_mask_fields: Vec<String>,
    fields: Vec<(String, firestore::FirestoreValue)>,
    transforms: Vec<document_transform::FieldTransform>,
}

pub(super) fn build_update_parts(
    assignments: &[(String, SqlValue)],
    base_doc_path: Option<&str>,
) -> Result<UpdateParts> {
    let mut update_mask_fields = Vec::with_capacity(assignments.len());
    let mut fields = Vec::with_capacity(assignments.len());
    let mut transforms = Vec::new();

    for (field, value) in assignments {
        if matches!(value, SqlValue::CurrentTimestamp) {
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

        let fv = sql_value_to_firestore(value, base_doc_path)?;
        fields.push((field.clone(), fv));
        update_mask_fields.push(field.clone());
    }

    Ok(UpdateParts {
        update_mask_fields,
        fields,
        transforms,
    })
}

/// The Firestore `BatchWrite` RPC is non-atomic: it returns `Ok` even when
/// individual writes fail, reporting each result in `statuses` (code 0 = OK).
/// Count only the writes that actually succeeded and surface the first failure,
/// so e.g. an INSERT colliding with an existing id (FAILED_PRECONDITION) is not
/// silently reported as affected.
pub(super) fn count_batch_outcome(
    statuses: &[Status],
    chunk_len: usize,
) -> (usize, Option<String>) {
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
pub(super) async fn drain_batch_results(
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

pub(super) async fn execute_batch_write(
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

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::document_transform::field_transform::{
        ServerValue, TransformType,
    };
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use serde_json::Value as JsonValue;

    #[test]
    fn update_parts_turn_current_timestamp_into_server_timestamp_transform() {
        let assignments = vec![("updated_at".to_string(), SqlValue::CurrentTimestamp)];

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
                SqlValue::Literal(JsonValue::String("active".to_string())),
            ),
            ("updated_at".to_string(), SqlValue::CurrentTimestamp),
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
