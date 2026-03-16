use crate::value::FireqlValue;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
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
