use crate::error::{FireqlError, Result};
use crate::sql::{
    AggregationExpr, AggregationFunc, CollectionSpec, CompareOp, FilterExpr, OrderBy,
    OrderDirection, Projection, UnaryOp, FIREQL_CURRENT_TS_KEY, FIREQL_REF_KEY, FIREQL_TS_KEY,
};
use chrono::{DateTime, Utc};
use firestore::{
    FirestoreAggregatedQueryParams, FirestoreAggregation, FirestoreAggregationOperator,
    FirestoreAggregationOperatorAvg, FirestoreAggregationOperatorCount,
    FirestoreAggregationOperatorSum, FirestoreQueryCollection, FirestoreQueryDirection,
    FirestoreQueryFilter, FirestoreQueryFilterCompare, FirestoreQueryFilterComposite,
    FirestoreQueryFilterCompositeOperator, FirestoreQueryFilterUnary, FirestoreQueryOrder,
    FirestoreQueryParams, FirestoreValue,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

pub fn build_query_params(
    collection: &CollectionSpec,
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
    limit: Option<u32>,
    projection: Option<&Projection>,
    base_doc_path: Option<&str>,
) -> Result<FirestoreQueryParams> {
    validate_query_constraints(filter, order_by)?;

    let mut params = FirestoreQueryParams::new(collection_id(collection));

    if collection.is_group {
        params.all_descendants = Some(true);
    }

    if let Some(filter_expr) = filter {
        params.filter = Some(build_filter(filter_expr, base_doc_path)?);
    }

    if !order_by.is_empty() {
        let mut order = Vec::with_capacity(order_by.len());
        for item in order_by {
            order.push(FirestoreQueryOrder {
                field_name: item.field.clone(),
                direction: match item.direction {
                    OrderDirection::Asc => FirestoreQueryDirection::Ascending,
                    OrderDirection::Desc => FirestoreQueryDirection::Descending,
                },
            });
        }
        params.order_by = Some(order);
    }

    if let Some(limit) = limit {
        params.limit = Some(limit);
    }

    if let Some(Projection::Fields(fields)) = projection {
        params.return_only_fields = Some(fields.clone());
    }

    Ok(params)
}

pub fn build_aggregated_query_params(
    collection: &CollectionSpec,
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
    limit: Option<u32>,
    aggregations: &[AggregationExpr],
    base_doc_path: Option<&str>,
) -> Result<FirestoreAggregatedQueryParams> {
    if !order_by.is_empty() {
        return Err(FireqlError::InvalidQuery(
            "ORDER BY is not supported in aggregation queries".to_string(),
        ));
    }
    if limit.is_some() {
        return Err(FireqlError::InvalidQuery(
            "LIMIT is not supported in aggregation queries".to_string(),
        ));
    }
    let query_params =
        build_query_params(collection, filter, order_by, limit, None, base_doc_path)?;
    let mut aggs = Vec::with_capacity(aggregations.len());
    for agg in aggregations {
        aggs.push(build_aggregation(agg)?);
    }
    Ok(FirestoreAggregatedQueryParams {
        query_params,
        aggregations: aggs,
    })
}

fn build_aggregation(agg: &AggregationExpr) -> Result<FirestoreAggregation> {
    let operator = match agg.func {
        AggregationFunc::Count => {
            FirestoreAggregationOperator::Count(FirestoreAggregationOperatorCount { up_to: None })
        }
        AggregationFunc::Sum => {
            let field = agg
                .field
                .clone()
                .ok_or_else(|| FireqlError::InvalidQuery("SUM requires a field".to_string()))?;
            FirestoreAggregationOperator::Sum(FirestoreAggregationOperatorSum { field_name: field })
        }
        AggregationFunc::Avg => {
            let field = agg
                .field
                .clone()
                .ok_or_else(|| FireqlError::InvalidQuery("AVG requires a field".to_string()))?;
            FirestoreAggregationOperator::Avg(FirestoreAggregationOperatorAvg { field_name: field })
        }
    };

    Ok(FirestoreAggregation {
        alias: agg.alias.clone(),
        operator: Some(operator),
    })
}

fn validate_query_constraints(filter: Option<&FilterExpr>, order_by: &[OrderBy]) -> Result<()> {
    let mut stats = FilterStats::default();
    if let Some(filter) = filter {
        collect_filter_stats(filter, &mut stats);
    }

    if stats.inequality_fields.len() > 1 {
        return Err(FireqlError::InvalidQuery(
            "Firestore allows inequality filters on a single field only".to_string(),
        ));
    }

    if let Some(field) = stats.inequality_fields.iter().next() {
        if order_by.is_empty() {
            return Err(FireqlError::InvalidQuery(format!(
                "Firestore requires ORDER BY on the inequality field: {field}"
            )));
        }
        let first = &order_by[0].field;
        if first != field {
            return Err(FireqlError::InvalidQuery(format!(
                "First ORDER BY field must match inequality field: expected {field}, got {first}"
            )));
        }
    }

    if stats.in_fields.len() > 1 {
        return Err(FireqlError::InvalidQuery(
            "Firestore allows at most one IN filter".to_string(),
        ));
    }
    if stats.in_lengths.contains(&0) {
        return Err(FireqlError::InvalidQuery(
            "IN requires at least one value".to_string(),
        ));
    }
    if stats.in_lengths.iter().any(|len| *len > 10) {
        return Err(FireqlError::InvalidQuery(
            "IN supports up to 10 values".to_string(),
        ));
    }
    if stats.not_in_fields.len() > 1 {
        return Err(FireqlError::InvalidQuery(
            "Firestore allows at most one NOT IN filter".to_string(),
        ));
    }
    if stats.not_in_lengths.contains(&0) {
        return Err(FireqlError::InvalidQuery(
            "NOT IN requires at least one value".to_string(),
        ));
    }
    if stats.not_in_lengths.iter().any(|len| *len > 10) {
        return Err(FireqlError::InvalidQuery(
            "NOT IN supports up to 10 values".to_string(),
        ));
    }
    if stats.not_eq_fields.len() > 1 {
        return Err(FireqlError::InvalidQuery(
            "Firestore allows at most one != filter".to_string(),
        ));
    }

    if !stats.not_in_fields.is_empty()
        && (!stats.in_fields.is_empty() || !stats.not_eq_fields.is_empty())
    {
        return Err(FireqlError::InvalidQuery(
            "NOT IN cannot be combined with IN or !=".to_string(),
        ));
    }

    if stats.array_contains_fields.len() + stats.array_contains_any_fields.len() > 1 {
        return Err(FireqlError::InvalidQuery(
            "Firestore allows at most one array-contains / array-contains-any filter".to_string(),
        ));
    }
    if !stats.array_contains_any_fields.is_empty()
        && (!stats.in_fields.is_empty() || !stats.not_in_fields.is_empty())
    {
        return Err(FireqlError::InvalidQuery(
            "array-contains-any cannot be combined with IN or NOT IN".to_string(),
        ));
    }
    if !stats.not_in_fields.is_empty()
        && (!stats.array_contains_fields.is_empty() || !stats.array_contains_any_fields.is_empty())
    {
        return Err(FireqlError::InvalidQuery(
            "NOT IN cannot be combined with array-contains filters".to_string(),
        ));
    }
    if stats.array_contains_any_lengths.contains(&0) {
        return Err(FireqlError::InvalidQuery(
            "array-contains-any requires at least one value".to_string(),
        ));
    }
    if stats.array_contains_any_lengths.iter().any(|len| *len > 10) {
        return Err(FireqlError::InvalidQuery(
            "array-contains-any supports up to 10 values".to_string(),
        ));
    }

    Ok(())
}

#[derive(Default)]
struct FilterStats {
    inequality_fields: BTreeSet<String>,
    in_fields: Vec<String>,
    not_in_fields: Vec<String>,
    not_eq_fields: Vec<String>,
    in_lengths: Vec<usize>,
    not_in_lengths: Vec<usize>,
    array_contains_fields: Vec<String>,
    array_contains_any_fields: Vec<String>,
    array_contains_any_lengths: Vec<usize>,
}

fn collect_filter_stats(filter: &FilterExpr, stats: &mut FilterStats) {
    match filter {
        FilterExpr::Compare { field, op, .. } => match op {
            CompareOp::Lt | CompareOp::LtEq | CompareOp::Gt | CompareOp::GtEq => {
                stats.inequality_fields.insert(field.clone());
            }
            CompareOp::NotEq => {
                stats.inequality_fields.insert(field.clone());
                stats.not_eq_fields.push(field.clone());
            }
            CompareOp::Eq => {}
        },
        FilterExpr::ArrayContains { field, .. } => {
            stats.array_contains_fields.push(field.clone());
        }
        FilterExpr::ArrayContainsAny { field, values } => {
            stats.array_contains_any_fields.push(field.clone());
            stats.array_contains_any_lengths.push(values.len());
        }
        FilterExpr::InList {
            field,
            values,
            negated,
        } => {
            if *negated {
                stats.inequality_fields.insert(field.clone());
                stats.not_in_fields.push(field.clone());
                stats.not_in_lengths.push(values.len());
            } else {
                stats.in_fields.push(field.clone());
                stats.in_lengths.push(values.len());
            }
        }
        FilterExpr::Unary { .. } => {}
        FilterExpr::And(filters) | FilterExpr::Or(filters) => {
            for f in filters {
                collect_filter_stats(f, stats);
            }
        }
    }
}

pub fn build_filter(
    filter: &FilterExpr,
    base_doc_path: Option<&str>,
) -> Result<FirestoreQueryFilter> {
    match filter {
        FilterExpr::Compare { field, op, value } => Ok(FirestoreQueryFilter::Compare(Some(
            compare_op_to_firestore(field, *op, value, base_doc_path)?,
        ))),
        FilterExpr::ArrayContains { field, value } => Ok(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContains(
                field.clone(),
                json_to_firestore_value_with_context(value, base_doc_path)?,
            ),
        ))),
        FilterExpr::ArrayContainsAny { field, values } => Ok(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContainsAny(
                field.clone(),
                json_array_to_firestore_value_with_context(values, base_doc_path)?,
            ),
        ))),
        FilterExpr::InList {
            field,
            values,
            negated,
        } => {
            let value = json_array_to_firestore_value_with_context(values, base_doc_path)?;
            let filter = if *negated {
                FirestoreQueryFilterCompare::NotIn(field.clone(), value)
            } else {
                FirestoreQueryFilterCompare::In(field.clone(), value)
            };
            Ok(FirestoreQueryFilter::Compare(Some(filter)))
        }
        FilterExpr::Unary { field, op } => Ok(FirestoreQueryFilter::Unary(match op {
            UnaryOp::IsNull => FirestoreQueryFilterUnary::IsNull(field.clone()),
            UnaryOp::IsNotNull => FirestoreQueryFilterUnary::IsNotNull(field.clone()),
        })),
        FilterExpr::And(filters) => Ok(FirestoreQueryFilter::Composite(
            FirestoreQueryFilterComposite {
                operator: FirestoreQueryFilterCompositeOperator::And,
                for_all_filters: filters
                    .iter()
                    .map(|f| build_filter(f, base_doc_path))
                    .collect::<Result<Vec<_>>>()?,
            },
        )),
        FilterExpr::Or(filters) => Ok(FirestoreQueryFilter::Composite(
            FirestoreQueryFilterComposite {
                operator: FirestoreQueryFilterCompositeOperator::Or,
                for_all_filters: filters
                    .iter()
                    .map(|f| build_filter(f, base_doc_path))
                    .collect::<Result<Vec<_>>>()?,
            },
        )),
    }
}

fn compare_op_to_firestore(
    field: &str,
    op: CompareOp,
    value: &JsonValue,
    base_doc_path: Option<&str>,
) -> Result<FirestoreQueryFilterCompare> {
    let firestore_value = json_to_firestore_value_with_context(value, base_doc_path)?;
    Ok(match op {
        CompareOp::Eq => FirestoreQueryFilterCompare::Equal(field.to_string(), firestore_value),
        CompareOp::NotEq => {
            FirestoreQueryFilterCompare::NotEqual(field.to_string(), firestore_value)
        }
        CompareOp::Lt => FirestoreQueryFilterCompare::LessThan(field.to_string(), firestore_value),
        CompareOp::LtEq => {
            FirestoreQueryFilterCompare::LessThanOrEqual(field.to_string(), firestore_value)
        }
        CompareOp::Gt => {
            FirestoreQueryFilterCompare::GreaterThan(field.to_string(), firestore_value)
        }
        CompareOp::GtEq => {
            FirestoreQueryFilterCompare::GreaterThanOrEqual(field.to_string(), firestore_value)
        }
    })
}

pub(crate) fn json_to_firestore_value_with_context(
    value: &JsonValue,
    base_doc_path: Option<&str>,
) -> Result<FirestoreValue> {
    if let JsonValue::Object(map) = value {
        if let Some(JsonValue::String(path)) = map.get(FIREQL_REF_KEY) {
            let full = expand_reference_path(path, base_doc_path)?;
            let fv: FirestoreValue = FirestoreReference(full).into();
            return Ok(fv);
        }
        if let Some(JsonValue::String(ts)) = map.get(FIREQL_TS_KEY) {
            let parsed = DateTime::parse_from_rfc3339(ts)
                .map_err(|e| FireqlError::InvalidQuery(format!("Invalid timestamp: {e}")))?;
            let utc: DateTime<Utc> = parsed.with_timezone(&Utc);
            let fv: FirestoreValue = FirestoreTimestamp(utc).into();
            return Ok(fv);
        }
        if map.contains_key(FIREQL_CURRENT_TS_KEY) {
            let fv: FirestoreValue = FirestoreTimestamp(Utc::now()).into();
            return Ok(fv);
        }
    }
    Ok(value.clone().into())
}

pub(crate) fn json_array_to_firestore_value_with_context(
    values: &[JsonValue],
    base_doc_path: Option<&str>,
) -> Result<FirestoreValue> {
    let mut array_values = Vec::with_capacity(values.len());
    for value in values {
        let fv = json_to_firestore_value_with_context(value, base_doc_path)?;
        array_values.push(fv.value);
    }
    Ok(FirestoreValue::from(
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(
                gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(
                    gcloud_sdk::google::firestore::v1::ArrayValue {
                        values: array_values,
                    },
                ),
            ),
        },
    ))
}

#[derive(Serialize)]
struct FirestoreReference(pub String);

#[derive(Serialize)]
struct FirestoreTimestamp(pub DateTime<Utc>);

fn collection_id(collection: &CollectionSpec) -> FirestoreQueryCollection {
    if collection.is_group {
        FirestoreQueryCollection::Group(vec![collection.name.clone()])
    } else {
        FirestoreQueryCollection::Single(collection.name.clone())
    }
}

fn expand_reference_path(path: &str, base_doc_path: Option<&str>) -> Result<String> {
    if path.contains("/documents/") || path.starts_with("projects/") {
        return Ok(path.to_string());
    }
    let base = base_doc_path
        .ok_or_else(|| FireqlError::InvalidQuery("ref(path) requires absolute path".to_string()))?;
    Ok(format!("{base}/{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{CollectionSpec, OrderDirection};
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use serde_json::json;

    fn collection() -> CollectionSpec {
        CollectionSpec {
            name: "users".to_string(),
            is_group: false,
        }
    }

    #[test]
    fn inequality_requires_order_by() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::Gt,
            value: JsonValue::from(10),
        };
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn inequality_requires_matching_first_order_by() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::GtEq,
            value: JsonValue::from(10),
        };
        let order_by = vec![OrderBy {
            field: "name".to_string(),
            direction: OrderDirection::Asc,
        }];
        let err = build_query_params(&collection(), Some(&filter), &order_by, None, None, None)
            .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn inequality_allows_matching_order_by() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::Lt,
            value: JsonValue::from(10),
        };
        let order_by = vec![OrderBy {
            field: "age".to_string(),
            direction: OrderDirection::Asc,
        }];
        let params = build_query_params(&collection(), Some(&filter), &order_by, None, None, None);
        assert!(params.is_ok());
    }

    #[test]
    fn inequality_single_field_only() {
        let filter = FilterExpr::And(vec![
            FilterExpr::Compare {
                field: "age".to_string(),
                op: CompareOp::Gt,
                value: JsonValue::from(10),
            },
            FilterExpr::Compare {
                field: "score".to_string(),
                op: CompareOp::Lt,
                value: JsonValue::from(5),
            },
        ]);
        let order_by = vec![OrderBy {
            field: "age".to_string(),
            direction: OrderDirection::Asc,
        }];
        let err = build_query_params(&collection(), Some(&filter), &order_by, None, None, None)
            .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn in_values_limit() {
        let filter = FilterExpr::InList {
            field: "age".to_string(),
            values: (0..11).map(JsonValue::from).collect(),
            negated: false,
        };
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn not_in_conflicts_with_not_equal() {
        let filter = FilterExpr::And(vec![
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![JsonValue::from("a")],
                negated: true,
            },
            FilterExpr::Compare {
                field: "score".to_string(),
                op: CompareOp::NotEq,
                value: JsonValue::from(1),
            },
        ]);
        let order_by = vec![OrderBy {
            field: "status".to_string(),
            direction: OrderDirection::Asc,
        }];
        let err = build_query_params(&collection(), Some(&filter), &order_by, None, None, None)
            .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn multiple_in_not_allowed() {
        let filter = FilterExpr::And(vec![
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![JsonValue::from("a")],
                negated: false,
            },
            FilterExpr::InList {
                field: "role".to_string(),
                values: vec![JsonValue::from("b")],
                negated: false,
            },
        ]);
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn array_contains_any_requires_values() {
        let filter = FilterExpr::ArrayContainsAny {
            field: "tags".to_string(),
            values: vec![],
        };
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn array_contains_any_conflicts_with_in() {
        let filter = FilterExpr::And(vec![
            FilterExpr::ArrayContainsAny {
                field: "tags".to_string(),
                values: vec![JsonValue::from("a")],
            },
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![JsonValue::from("b")],
                negated: false,
            },
        ]);
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn aggregation_disallows_order_by_and_limit() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::Gt,
            value: JsonValue::from(10),
        };
        let order_by = vec![OrderBy {
            field: "age".to_string(),
            direction: OrderDirection::Asc,
        }];
        let agg = AggregationExpr {
            func: AggregationFunc::Count,
            field: None,
            alias: "count".to_string(),
        };

        let err = build_aggregated_query_params(
            &collection(),
            Some(&filter),
            &order_by,
            None,
            std::slice::from_ref(&agg),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));

        let err = build_aggregated_query_params(
            &collection(),
            Some(&filter),
            &[],
            Some(10),
            &[agg],
            None,
        )
        .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn reference_value_expands_relative_path() {
        let value = JsonValue::Object(
            [(
                FIREQL_REF_KEY.to_string(),
                JsonValue::String("users/u1".to_string()),
            )]
            .into_iter()
            .collect(),
        );
        let fv = json_to_firestore_value_with_context(
            &value,
            Some("projects/p/databases/(default)/documents"),
        )
        .unwrap();
        match fv.value.value_type {
            Some(ValueType::ReferenceValue(path)) => {
                assert_eq!(path, "projects/p/databases/(default)/documents/users/u1");
            }
            _ => panic!("expected reference value"),
        }
    }

    #[test]
    fn timestamp_value_parses_rfc3339() {
        let value = JsonValue::Object(
            [(
                FIREQL_TS_KEY.to_string(),
                JsonValue::String("2024-01-01T00:00:00Z".to_string()),
            )]
            .into_iter()
            .collect(),
        );
        let fv = json_to_firestore_value_with_context(&value, None).unwrap();
        match fv.value.value_type {
            Some(ValueType::TimestampValue(ts)) => {
                assert_eq!(ts.seconds, 1704067200);
            }
            _ => panic!("expected timestamp value"),
        }
    }

    #[test]
    fn current_timestamp_value_is_now() {
        let value = json!({ FIREQL_CURRENT_TS_KEY: true });
        let fv = json_to_firestore_value_with_context(&value, None).unwrap();
        match fv.value.value_type {
            Some(ValueType::TimestampValue(ts)) => {
                let now = Utc::now().timestamp();
                assert!((ts.seconds - now).abs() <= 10);
            }
            _ => panic!("expected timestamp value"),
        }
    }
}
