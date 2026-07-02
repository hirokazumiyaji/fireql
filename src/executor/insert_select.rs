use super::batch::{count_batch_outcome, drain_batch_results, into_batches, FireqlWrite};
use super::doc_name::parse_doc_name;
use crate::error::{FireqlError, Result};
use crate::output::FireqlOutput;
use crate::planner::build_query_params;
use crate::sql::{CollectionSpec, InsertSelectStatement, Projection, SelectProjection};
use firestore::{firestore_document_from_map, FirestoreDb, FirestoreQuerySupport};
use futures::stream::{self, StreamExt};
use gcloud_sdk::google::firestore::v1::{precondition, write, Document, Precondition, Write};
use rand::distr::{Alphanumeric, SampleString};
use serde_json::Value as JsonValue;

pub(super) async fn execute_insert_select(
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
