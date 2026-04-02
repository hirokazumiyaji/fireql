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
            _ => JoinKey::Null,
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
        if key == JoinKey::Null {
            continue;
        }
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

pub struct JoinParams<'a> {
    pub left_field: &'a str,
    pub right_field: &'a str,
    pub join_type: JoinType,
    pub left_prefix: &'a str,
    pub right_prefix: &'a str,
    pub prefix_left: bool,
}

pub fn hash_join(
    left_docs: &[DocOutput],
    right_docs: &[DocOutput],
    params: &JoinParams<'_>,
) -> Vec<DocOutput> {
    let JoinParams { left_field, right_field, join_type, left_prefix, right_prefix, prefix_left } = params;
    let mut right_map: HashMap<JoinKey, Vec<&DocOutput>> = HashMap::new();
    for doc in right_docs {
        let key = doc_key(doc, right_field);
        if key == JoinKey::Null {
            continue;
        }
        right_map.entry(key).or_default().push(doc);
    }

    let left_data = |doc: &DocOutput| -> HashMap<String, FireqlValue> {
        if *prefix_left {
            prefix_fields(&doc.data, left_prefix)
        } else {
            doc.data.clone()
        }
    };

    let emit_left = |doc: &DocOutput, data: HashMap<String, FireqlValue>| DocOutput {
        id: doc.id.clone(),
        path: doc.path.clone(),
        data,
    };

    let mut result = Vec::new();
    for left_doc in left_docs {
        let left_key = doc_key(left_doc, left_field);

        if left_key == JoinKey::Null {
            if *join_type == JoinType::Left {
                result.push(emit_left(left_doc, left_data(left_doc)));
            }
            continue;
        }

        match right_map.get(&left_key) {
            Some(matches) => {
                let left_prefixed = left_data(left_doc);
                for right_doc in matches {
                    let mut merged = left_prefixed.clone();
                    merged.extend(
                        right_doc.data.iter().map(|(k, v)| (format!("{right_prefix}.{k}"), v.clone()))
                    );
                    result.push(emit_left(left_doc, merged));
                }
            }
            None if *join_type == JoinType::Left => {
                result.push(emit_left(left_doc, left_data(left_doc)));
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

    fn jp<'a>(
        left_field: &'a str,
        right_field: &'a str,
        join_type: JoinType,
        left_prefix: &'a str,
        right_prefix: &'a str,
        prefix_left: bool,
    ) -> JoinParams<'a> {
        JoinParams { left_field, right_field, join_type, left_prefix, right_prefix, prefix_left }
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

        let result = hash_join(&left, &right, &jp("dept_id", "__name__", JoinType::Inner, "users", "departments", true));
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

        let result = hash_join(&left, &right, &jp("dept_id", "__name__", JoinType::Left, "users", "departments", true));
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

        let result = hash_join(&left, &right, &jp("order_id", "__name__", JoinType::Inner, "users", "orders", true));
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

        let result = hash_join(&left, &right, &jp("__name__", "user_id", JoinType::Inner, "users", "reviews", true));
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
        assert_eq!(keys.len(), 1);
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
        let result = hash_join(&left, &right, &jp("dept_id", "__name__", JoinType::Inner, "u", "d", true));
        assert!(result.is_empty());
    }

    #[test]
    fn hash_join_left_all_unmatched() {
        let left = vec![
            make_doc("u1", vec![("dept_id", FireqlValue::String("d999".into()))]),
            make_doc("u2", vec![("dept_id", FireqlValue::String("d998".into()))]),
        ];
        let right: Vec<DocOutput> = vec![];
        let result = hash_join(&left, &right, &jp("dept_id", "__name__", JoinType::Left, "u", "d", true));
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
    fn hash_join_null_keys_do_not_match() {
        let left = vec![
            make_doc("u1", vec![]),
            make_doc("u2", vec![]),
        ];
        let right = vec![
            make_doc("d1", vec![]),
        ];
        let result = hash_join(&left, &right, &jp("dept_id", "id", JoinType::Inner, "l", "r", true));
        assert!(result.is_empty(), "NULL should not match NULL in INNER JOIN");
    }

    #[test]
    fn hash_join_left_null_keys_preserved_without_match() {
        let left = vec![
            make_doc("u1", vec![("dept_id", FireqlValue::String("d1".into()))]),
            make_doc("u2", vec![]),
        ];
        let right = vec![
            make_doc("d1", vec![("name", FireqlValue::String("Eng".into()))]),
        ];
        let result = hash_join(&left, &right, &jp("dept_id", "__name__", JoinType::Left, "l", "r", true));
        assert_eq!(result.len(), 2);
        assert!(result[0].data.contains_key("r.name"));
        assert!(!result[1].data.contains_key("r.name"));
    }

    #[test]
    fn extract_join_keys_excludes_null_from_result() {
        let docs = vec![
            make_doc("u1", vec![("dept", FireqlValue::String("eng".into()))]),
            make_doc("u2", vec![]),
        ];
        let keys = extract_join_keys(&docs, "dept");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], JoinKey::String("eng".to_string()));
    }

    #[test]
    fn join_key_from_unsupported_type_returns_null() {
        let key = JoinKey::from_fireql_value(&FireqlValue::Double(3.14));
        assert_eq!(key, JoinKey::Null);

        let key = JoinKey::from_fireql_value(&FireqlValue::Array(vec![]));
        assert_eq!(key, JoinKey::Null);
    }

    #[test]
    fn hash_join_already_prefixed_data_not_double_prefixed() {
        let left = vec![
            DocOutput {
                id: "u1".to_string(),
                path: "users/u1".to_string(),
                data: vec![
                    ("users.name".to_string(), FireqlValue::String("Alice".into())),
                    ("orders.amount".to_string(), FireqlValue::Integer(100)),
                    ("orders.product_id".to_string(), FireqlValue::String("p1".into())),
                ].into_iter().collect(),
            },
        ];
        let right = vec![
            make_doc("p1", vec![("product_name", FireqlValue::String("Widget".into()))]),
        ];

        let result = hash_join(
            &left, &right,
            &jp("orders.product_id", "__name__", JoinType::Inner, "users", "products", false),
        );

        assert_eq!(result.len(), 1);
        assert!(result[0].data.contains_key("users.name"), "should keep original users.name prefix");
        assert!(result[0].data.contains_key("orders.amount"), "should keep original orders.amount prefix");
        assert!(result[0].data.contains_key("products.product_name"), "right side should get products prefix");
        assert!(!result[0].data.contains_key("users.users.name"), "should NOT double-prefix");
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

        let result = hash_join(&left, &right, &jp("__name__", "user_id", JoinType::Inner, "users", "orders", true));
        assert_eq!(result.len(), 2);
    }
}
