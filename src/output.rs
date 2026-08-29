use crate::value::FireqlValue;
use futures::stream::BoxStream;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct DocOutput {
    pub id: String,
    pub path: String,
    pub data: HashMap<String, FireqlValue>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum FireqlOutput {
    Rows(Vec<DocOutput>),
    Affected { affected: u64 },
    Aggregation(HashMap<String, FireqlValue>),
}

/// Result of a streaming execution ([`crate::Fireql::execute_stream`]).
///
/// Plain SELECT statements (no JOIN / aggregation) stream their rows as
/// documents arrive from Firestore, keeping the memory footprint constant
/// regardless of result size (#55). Every other statement kind produces its
/// output as a single value and is returned as [`FireqlStream::Completed`].
pub enum FireqlStream<'a> {
    /// Rows of a SELECT statement, streamed as documents arrive.
    Rows(BoxStream<'a, crate::error::Result<DocOutput>>),
    /// A single, already materialized output (aggregations, JOINed SELECTs,
    /// UPDATE/DELETE/INSERT SELECT affected counts).
    Completed(FireqlOutput),
}

impl<'a> From<FireqlOutput> for FireqlStream<'a> {
    fn from(output: FireqlOutput) -> Self {
        FireqlStream::Completed(output)
    }
}
