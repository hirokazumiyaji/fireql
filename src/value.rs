use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::Value;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FireqlValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Timestamp(chrono::DateTime<chrono::Utc>),
    String(String),
    Bytes(Vec<u8>),
    Reference(String),
    GeoPoint { latitude: f64, longitude: f64 },
    Array(Vec<FireqlValue>),
    Map(HashMap<String, FireqlValue>),
}

impl FireqlValue {
    pub(crate) fn from_proto(value: &Value) -> Self {
        match &value.value_type {
            None | Some(ValueType::NullValue(_)) => Self::Null,
            Some(ValueType::BooleanValue(b)) => Self::Boolean(*b),
            Some(ValueType::IntegerValue(i)) => Self::Integer(*i),
            Some(ValueType::DoubleValue(d)) => Self::Double(*d),
            Some(ValueType::TimestampValue(ts)) => {
                let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32)
                    .unwrap_or_default();
                Self::Timestamp(dt)
            }
            Some(ValueType::StringValue(s)) => Self::String(s.clone()),
            Some(ValueType::BytesValue(b)) => Self::Bytes(b.clone()),
            Some(ValueType::ReferenceValue(r)) => Self::Reference(r.clone()),
            Some(ValueType::GeoPointValue(g)) => Self::GeoPoint {
                latitude: g.latitude,
                longitude: g.longitude,
            },
            Some(ValueType::ArrayValue(arr)) => {
                Self::Array(arr.values.iter().map(Self::from_proto).collect())
            }
            Some(ValueType::MapValue(map)) => Self::Map(
                map.fields
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::from_proto(v)))
                    .collect(),
            ),
            _ => Self::Null,
        }
    }

    pub(crate) fn from_document_fields(fields: &HashMap<String, Value>) -> HashMap<String, Self> {
        fields
            .iter()
            .map(|(k, v)| (k.clone(), Self::from_proto(v)))
            .collect()
    }

    pub fn to_plain_string(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Boolean(b) => b.to_string(),
            Self::Integer(i) => i.to_string(),
            Self::Double(d) => d.to_string(),
            Self::String(s) => s.clone(),
            Self::Bytes(b) => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(b)
            }
            Self::Timestamp(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            Self::Reference(r) => r.clone(),
            Self::GeoPoint { .. } | Self::Array(_) | Self::Map(_) => {
                serde_json::to_string(&plain_json_value(self))
                    .expect("structured value serialization cannot fail")
            }
        }
    }
}

fn to_relative_path(full_path: &str) -> &str {
    const MARKER: &str = "/documents/";
    if !full_path.starts_with("projects/") {
        return full_path;
    }
    match full_path.find(MARKER) {
        Some(pos) => &full_path[pos + MARKER.len()..],
        None => full_path,
    }
}

// All types serialize with `_firestore_type`. Most include a `value` key,
// but `Null` omits it and `GeoPoint` uses `latitude`/`longitude` instead.
impl Serialize for FireqlValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("_firestore_type", "null")?;
                map.end()
            }
            Self::Boolean(b) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "boolean")?;
                map.serialize_entry("value", b)?;
                map.end()
            }
            Self::Integer(i) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "integer")?;
                map.serialize_entry("value", i)?;
                map.end()
            }
            Self::Double(d) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "double")?;
                map.serialize_entry("value", d)?;
                map.end()
            }
            Self::String(s) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "string")?;
                map.serialize_entry("value", s)?;
                map.end()
            }
            Self::Timestamp(dt) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "timestamp")?;
                map.serialize_entry(
                    "value",
                    &dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                )?;
                map.end()
            }
            Self::Bytes(b) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "bytes")?;
                map.serialize_entry("value", b)?;
                map.end()
            }
            Self::Reference(r) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "reference")?;
                map.serialize_entry("value", to_relative_path(r))?;
                map.end()
            }
            Self::GeoPoint {
                latitude,
                longitude,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("_firestore_type", "geopoint")?;
                map.serialize_entry("latitude", latitude)?;
                map.serialize_entry("longitude", longitude)?;
                map.end()
            }
            Self::Array(arr) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "array")?;
                map.serialize_entry("value", &TypedArray(arr))?;
                map.end()
            }
            Self::Map(inner) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "map")?;
                map.serialize_entry("value", inner)?;
                map.end()
            }
        }
    }
}

fn plain_json_value(v: &FireqlValue) -> serde_json::Value {
    match v {
        FireqlValue::Null => serde_json::Value::Null,
        FireqlValue::Boolean(b) => serde_json::Value::from(*b),
        FireqlValue::Integer(i) => serde_json::Value::from(*i),
        FireqlValue::Double(d) => {
            if d.is_finite() {
                serde_json::json!(*d)
            } else {
                serde_json::Value::from(d.to_string())
            }
        }
        FireqlValue::String(s) => serde_json::Value::from(s.as_str()),
        FireqlValue::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::from(base64::engine::general_purpose::STANDARD.encode(b))
        }
        FireqlValue::Timestamp(dt) => {
            serde_json::Value::from(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        FireqlValue::Reference(r) => serde_json::Value::from(r.as_str()),
        FireqlValue::GeoPoint {
            latitude,
            longitude,
        } => serde_json::json!({"latitude": latitude, "longitude": longitude}),
        FireqlValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(plain_json_value).collect())
        }
        FireqlValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), plain_json_value(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

struct TypedArray<'a>(&'a [FireqlValue]);

impl Serialize for TypedArray<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for v in self.0 {
            seq.serialize_element(v)?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_relative_path_normal() {
        let full = "projects/p/databases/(default)/documents/users/u1";
        assert_eq!(to_relative_path(full), "users/u1");
    }

    #[test]
    fn test_to_relative_path_nested_collection() {
        let full = "projects/p/databases/(default)/documents/users/u1/posts/p1";
        assert_eq!(to_relative_path(full), "users/u1/posts/p1");
    }

    #[test]
    fn test_to_relative_path_no_documents_prefix() {
        let path = "some/other/path";
        assert_eq!(to_relative_path(path), "some/other/path");
    }

    #[test]
    fn test_serialize_reference_normal() {
        let val =
            FireqlValue::Reference("projects/p/databases/(default)/documents/users/u1".to_string());
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["_firestore_type"], "reference");
        assert_eq!(json["value"], "users/u1");
    }

    #[test]
    fn test_serialize_reference_nested_collection() {
        let val = FireqlValue::Reference(
            "projects/p/databases/(default)/documents/users/u1/posts/p1".to_string(),
        );
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["value"], "users/u1/posts/p1");
    }

    #[test]
    fn test_serialize_reference_fallback() {
        let val = FireqlValue::Reference("some/other/path".to_string());
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["value"], "some/other/path");
    }

    #[test]
    fn test_to_relative_path_non_resource_with_documents_segment() {
        let path = "users/documents/u1";
        assert_eq!(to_relative_path(path), "users/documents/u1");
    }

    #[test]
    fn plain_string_null() {
        assert_eq!(FireqlValue::Null.to_plain_string(), "");
    }

    #[test]
    fn plain_string_boolean() {
        assert_eq!(FireqlValue::Boolean(true).to_plain_string(), "true");
        assert_eq!(FireqlValue::Boolean(false).to_plain_string(), "false");
    }

    #[test]
    fn plain_string_integer() {
        assert_eq!(FireqlValue::Integer(42).to_plain_string(), "42");
        assert_eq!(FireqlValue::Integer(-1).to_plain_string(), "-1");
    }

    #[test]
    fn plain_string_double() {
        assert_eq!(FireqlValue::Double(2.5).to_plain_string(), "2.5");
    }

    #[test]
    fn plain_string_string() {
        assert_eq!(
            FireqlValue::String("hello".to_string()).to_plain_string(),
            "hello"
        );
    }

    #[test]
    fn plain_string_bytes() {
        use base64::Engine;
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let expected = base64::engine::general_purpose::STANDARD.encode(&data);
        assert_eq!(FireqlValue::Bytes(data).to_plain_string(), expected);
    }

    #[test]
    fn plain_string_timestamp() {
        let dt = chrono::DateTime::from_timestamp(1704067200, 0).unwrap();
        assert_eq!(
            FireqlValue::Timestamp(dt).to_plain_string(),
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn plain_string_reference() {
        assert_eq!(
            FireqlValue::Reference("projects/p/databases/d/documents/c/id".to_string())
                .to_plain_string(),
            "projects/p/databases/d/documents/c/id"
        );
    }

    #[test]
    fn plain_string_geopoint() {
        let v = FireqlValue::GeoPoint {
            latitude: 40.7128,
            longitude: -74.006,
        };
        assert_eq!(
            v.to_plain_string(),
            r#"{"latitude":40.7128,"longitude":-74.006}"#
        );
    }

    #[test]
    fn plain_string_array() {
        let v = FireqlValue::Array(vec![
            FireqlValue::String("a".to_string()),
            FireqlValue::Integer(1),
        ]);
        assert_eq!(v.to_plain_string(), r#"["a",1]"#);
    }

    #[test]
    fn plain_string_map() {
        let mut m = HashMap::new();
        m.insert("key".to_string(), FireqlValue::Integer(42));
        let v = FireqlValue::Map(m);
        assert_eq!(v.to_plain_string(), r#"{"key":42}"#);
    }

    #[test]
    fn plain_string_string_with_control_chars() {
        let v = FireqlValue::Array(vec![
            FireqlValue::String("line1\nline2".to_string()),
            FireqlValue::String("tab\there".to_string()),
        ]);
        let result = v.to_plain_string();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed[0], "line1\nline2");
        assert_eq!(parsed[1], "tab\there");
    }
}
