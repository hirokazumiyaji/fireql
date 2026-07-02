use crate::error::{FireqlError, Result};
use crate::output::DocOutput;
use crate::value::FireqlValue;

pub(super) struct DocNameParts {
    pub id: String,
    pub path: String,
    pub collection: String,
    parent_full: Option<String>,
}

impl DocNameParts {
    pub(super) fn parent_path<'a>(&'a self, default: &'a str) -> &'a str {
        self.parent_full.as_deref().unwrap_or(default)
    }
}

pub(super) fn parse_doc_name(name: &str) -> Result<DocNameParts> {
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

pub(super) fn docs_to_output(
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
