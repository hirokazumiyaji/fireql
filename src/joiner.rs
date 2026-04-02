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

pub fn extract_join_keys(docs: &[DocOutput], field: &str) -> Vec<JoinKey> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for doc in docs {
        let key = if field == "__name__" {
            JoinKey::String(doc.id.clone())
        } else {
            match doc.data.get(field) {
                Some(v) => JoinKey::from_fireql_value(v),
                None => JoinKey::Null,
            }
        };
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
        let key = if right_field == "__name__" {
            JoinKey::String(doc.id.clone())
        } else {
            match doc.data.get(right_field) {
                Some(v) => JoinKey::from_fireql_value(v),
                None => JoinKey::Null,
            }
        };
        right_map.entry(key).or_default().push(doc);
    }

    let mut result = Vec::new();
    for left_doc in left_docs {
        let left_key = if left_field == "__name__" {
            JoinKey::String(left_doc.id.clone())
        } else {
            match left_doc.data.get(left_field) {
                Some(v) => JoinKey::from_fireql_value(v),
                None => JoinKey::Null,
            }
        };

        let left_prefixed = prefix_fields(&left_doc.data, left_prefix);

        match right_map.get(&left_key) {
            Some(matches) => {
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
            None => {
                if join_type == JoinType::Left {
                    result.push(DocOutput {
                        id: left_doc.id.clone(),
                        path: left_doc.path.clone(),
                        data: left_prefixed,
                    });
                }
            }
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
