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
}

// All types serialize with a consistent `_firestore_type` + `value` schema.
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
                map.serialize_entry("value", r)?;
                map.end()
            }
            Self::GeoPoint { latitude, longitude } => {
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
