use crate::output::DocOutput;
use crate::sql::JoinType;
use crate::value::FireqlValue;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinKey {
    String(String),
    Integer(i64),
    Boolean(bool),
    Null,
}

impl JoinKey {
    pub fn from_fireql_value(value: &FireqlValue) -> Self {
        match value {
            FireqlValue::String(s) => JoinKey::String(s.clone()),
            FireqlValue::Integer(i) => JoinKey::Integer(*i),
            FireqlValue::Boolean(b) => JoinKey::Boolean(*b),
            FireqlValue::Null => JoinKey::Null,
            other => JoinKey::String(format!("{other:?}")),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            JoinKey::String(s) => serde_json::Value::String(s.clone()),
            JoinKey::Integer(i) => serde_json::Value::Number((*i).into()),
            JoinKey::Boolean(b) => serde_json::Value::Bool(*b),
            JoinKey::Null => serde_json::Value::Null,
        }
    }
}

fn doc_key(doc: &DocOutput, field: &str) -> JoinKey {
    if field == "__name__" {
        JoinKey::String(doc.id.clone())
    } else {
        match doc.data.get(field) {
            Some(v) => JoinKey::from_fireql_value(v),
            None => JoinKey::Null,
        }
    }
}

pub fn extract_join_keys(docs: &[DocOutput], field: &str) -> Vec<JoinKey> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for doc in docs {
        let key = doc_key(doc, field);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}

pub fn chunk_keys(keys: &[JoinKey], chunk_size: usize) -> Vec<&[JoinKey]> {
    keys.chunks(chunk_size).collect()
}

fn prefix_fields(
    data: &HashMap<String, FireqlValue>,
    prefix: &str,
) -> HashMap<String, FireqlValue> {
    data.iter()
        .map(|(k, v)| (format!("{prefix}.{k}"), v.clone()))
        .collect()
}

pub fn hash_join(
    left_docs: &[DocOutput],
    right_docs: &[DocOutput],
    left_field: &str,
    right_field: &str,
    join_type: JoinType,
    left_prefix: &str,
    right_prefix: &str,
) -> Vec<DocOutput> {
    let mut right_map: HashMap<JoinKey, Vec<&DocOutput>> = HashMap::new();
    for doc in right_docs {
        right_map.entry(doc_key(doc, right_field)).or_default().push(doc);
    }

    let mut result = Vec::new();
    for left_doc in left_docs {
        let left_key = doc_key(left_doc, left_field);

        match right_map.get(&left_key) {
            Some(matches) => {
                let left_prefixed = prefix_fields(&left_doc.data, left_prefix);
                for right_doc in matches {
                    let mut merged = left_prefixed.clone();
                    merged.extend(prefix_fields(&right_doc.data, right_prefix));
                    result.push(DocOutput {
                        id: left_doc.id.clone(),
                        path: left_doc.path.clone(),
                        data: merged,
                    });
                }
            }
            None if join_type == JoinType::Left => {
                result.push(DocOutput {
                    id: left_doc.id.clone(),
                    path: left_doc.path.clone(),
                    data: prefix_fields(&left_doc.data, left_prefix),
                });
            }
            None => {}
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::FireqlValue;

    fn make_doc(id: &str, data: Vec<(&str, FireqlValue)>) -> DocOutput {
        DocOutput {
            id: id.to_string(),
            path: format!("collection/{id}"),
            data: data.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    #[test]
    fn extract_join_keys_by_name() {
        let docs = vec![
            make_doc("u1", vec![("name", FireqlValue::String("Alice".into()))]),
            make_doc("u2", vec![("name", FireqlValue::String("Bob".into()))]),
        ];
        let keys = extract_join_keys(&docs, "__name__");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&JoinKey::String("u1".to_string())));
        assert!(keys.contains(&JoinKey::String("u2".to_string())));
    }

    #[test]
    fn extract_join_keys_by_field() {
        let docs = vec![
            make_doc("u1", vec![("dept", FireqlValue::String("eng".into()))]),
            make_doc("u2", vec![("dept", FireqlValue::String("eng".into()))]),
            make_doc("u3", vec![("dept", FireqlValue::String("sales".into()))]),
        ];
        let keys = extract_join_keys(&docs, "dept");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn chunk_keys_splits_by_10() {
        let keys: Vec<JoinKey> = (0..25).map(|i| JoinKey::String(format!("v{i}"))).collect();
        let chunks = chunk_keys(&keys, 10);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 10);
        assert_eq!(chunks[1].len(), 10);
        assert_eq!(chunks[2].len(), 5);
    }

    #[test]
    fn hash_join_inner() {
        let left = vec![
            make_doc("u1", vec![
                ("name", FireqlValue::String("Alice".into())),
                ("dept_id", FireqlValue::String("d1".into())),
            ]),
            make_doc("u2", vec![
                ("name", FireqlValue::String("Bob".into())),
                ("dept_id", FireqlValue::String("d2".into())),
            ]),
            make_doc("u3", vec![
                ("name", FireqlValue::String("Charlie".into())),
                ("dept_id", FireqlValue::String("d999".into())),
            ]),
        ];
        let right = vec![
            make_doc("d1", vec![("dept_name", FireqlValue::String("Engineering".into()))]),
            make_doc("d2", vec![("dept_name", FireqlValue::String("Sales".into()))]),
        ];

        let result = hash_join(&left, &right, "dept_id", "__name__", JoinType::Inner, "users", "departments");
        assert_eq!(result.len(), 2);
        assert!(result[0].data.contains_key("users.name"));
        assert!(result[0].data.contains_key("departments.dept_name"));
        assert_eq!(result[0].id, "u1");
    }

    #[test]
    fn hash_join_left() {
        let left = vec![
            make_doc("u1", vec![("dept_id", FireqlValue::String("d1".into()))]),
            make_doc("u2", vec![("dept_id", FireqlValue::String("d999".into()))]),
        ];
        let right = vec![
            make_doc("d1", vec![("dept_name", FireqlValue::String("Engineering".into()))]),
        ];

        let result = hash_join(&left, &right, "dept_id", "__name__", JoinType::Left, "users", "departments");
        assert_eq!(result.len(), 2);
        assert!(result[0].data.contains_key("departments.dept_name"));
        assert!(!result[1].data.contains_key("departments.dept_name"));
        assert_eq!(result[1].id, "u2");
    }

    #[test]
    fn hash_join_by_document_id() {
        let left = vec![
            make_doc("u1", vec![("order_id", FireqlValue::String("o1".into()))]),
            make_doc("u2", vec![("order_id", FireqlValue::String("o2".into()))]),
        ];
        let right = vec![
            make_doc("o1", vec![("amount", FireqlValue::Integer(100))]),
            make_doc("o2", vec![("amount", FireqlValue::Integer(200))]),
        ];

        let result = hash_join(&left, &right, "order_id", "__name__", JoinType::Inner, "users", "orders");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data.get("orders.amount"), Some(&FireqlValue::Integer(100)));
    }

    #[test]
    fn hash_join_left_side_by_document_id() {
        let left = vec![
            make_doc("u1", vec![("name", FireqlValue::String("Alice".into()))]),
        ];
        let right = vec![
            make_doc("r1", vec![
                ("user_id", FireqlValue::String("u1".into())),
                ("score", FireqlValue::Integer(95)),
            ]),
        ];

        let result = hash_join(&left, &right, "__name__", "user_id", JoinType::Inner, "users", "reviews");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].data.get("reviews.score"), Some(&FireqlValue::Integer(95)));
    }

    #[test]
    fn extract_join_keys_with_missing_field() {
        let docs = vec![
            make_doc("u1", vec![("name", FireqlValue::String("Alice".into()))]),
            make_doc("u2", vec![("dept", FireqlValue::String("eng".into()))]),
        ];
        let keys = extract_join_keys(&docs, "dept");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&JoinKey::Null));
        assert!(keys.contains(&JoinKey::String("eng".to_string())));
    }

    #[test]
    fn extract_join_keys_empty_docs() {
        let docs: Vec<DocOutput> = vec![];
        let keys = extract_join_keys(&docs, "field");
        assert!(keys.is_empty());
    }

    #[test]
    fn hash_join_inner_no_matches() {
        let left = vec![
            make_doc("u1", vec![("dept_id", FireqlValue::String("d999".into()))]),
        ];
        let right = vec![
            make_doc("d1", vec![("name", FireqlValue::String("Engineering".into()))]),
        ];
        let result = hash_join(&left, &right, "dept_id", "__name__", JoinType::Inner, "u", "d");
        assert!(result.is_empty());
    }

    #[test]
    fn hash_join_left_all_unmatched() {
        let left = vec![
            make_doc("u1", vec![("dept_id", FireqlValue::String("d999".into()))]),
            make_doc("u2", vec![("dept_id", FireqlValue::String("d998".into()))]),
        ];
        let right: Vec<DocOutput> = vec![];
        let result = hash_join(&left, &right, "dept_id", "__name__", JoinType::Left, "u", "d");
        assert_eq!(result.len(), 2);
        assert!(!result[0].data.contains_key("d.name"));
    }

    #[test]
    fn join_key_to_json_roundtrip() {
        assert_eq!(
            JoinKey::String("hello".into()).to_json_value(),
            serde_json::Value::String("hello".into())
        );
        assert_eq!(JoinKey::Integer(42).to_json_value(), serde_json::json!(42));
        assert_eq!(
            JoinKey::Boolean(true).to_json_value(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(JoinKey::Null.to_json_value(), serde_json::Value::Null);
    }

    #[test]
    fn chunk_keys_empty() {
        let keys: Vec<JoinKey> = vec![];
        let chunks = chunk_keys(&keys, 10);
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_keys_exact_multiple() {
        let keys: Vec<JoinKey> = (0..20).map(|i| JoinKey::String(format!("v{i}"))).collect();
        let chunks = chunk_keys(&keys, 10);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 10);
        assert_eq!(chunks[1].len(), 10);
    }

    #[test]
    fn hash_join_one_to_many() {
        let left = vec![
            make_doc("u1", vec![("name", FireqlValue::String("Alice".into()))]),
        ];
        let right = vec![
            make_doc("o1", vec![("user_id", FireqlValue::String("u1".into())), ("amount", FireqlValue::Integer(100))]),
            make_doc("o2", vec![("user_id", FireqlValue::String("u1".into())), ("amount", FireqlValue::Integer(200))]),
        ];

        let result = hash_join(&left, &right, "__name__", "user_id", JoinType::Inner, "users", "orders");
        assert_eq!(result.len(), 2);
    }
}
