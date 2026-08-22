use crate::error::{FireqlError, Result};
use crate::sql::{
    AggregationExpr, AggregationFunc, CollectionSpec, CompareOp, FilterExpr, OrderBy,
    OrderDirection, Projection, SqlValue, UnaryOp,
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

// Firestore disjunction value limits. `in` and `array-contains-any` allow 30,
// but `not-in` is stricter at 10.
pub(crate) const MAX_IN_VALUES: usize = 30;
pub(crate) const MAX_NOT_IN_VALUES: usize = 10;
pub(crate) const MAX_ARRAY_CONTAINS_ANY_VALUES: usize = 30;

pub fn build_query_params(
    collection: &CollectionSpec,
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
    limit: Option<u32>,
    projection: Option<&Projection>,
    documents_path: Option<&str>,
) -> Result<FirestoreQueryParams> {
    validate_query_constraints(filter, order_by)?;

    let mut params = FirestoreQueryParams::new(firestore_query_collection(collection));

    if collection.is_group {
        params.all_descendants = Some(true);
    }

    if let Some(pp) = &collection.parent_path {
        let base = documents_path.ok_or_else(|| {
            FireqlError::InvalidQuery(
                "collection() with a subcollection path requires database context".to_string(),
            )
        })?;
        params.parent = Some(format!("{base}/{pp}"));
    }

    if let Some(filter_expr) = filter {
        params.filter = Some(build_filter(filter_expr, documents_path)?);
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
    documents_path: Option<&str>,
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
        build_query_params(collection, filter, order_by, limit, None, documents_path)?;
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

mod validate;

use validate::validate_query_constraints;

pub fn build_filter(
    filter: &FilterExpr,
    documents_path: Option<&str>,
) -> Result<FirestoreQueryFilter> {
    match filter {
        FilterExpr::Compare { field, op, value } => Ok(FirestoreQueryFilter::Compare(Some(
            compare_op_to_firestore(field, *op, value, documents_path)?,
        ))),
        FilterExpr::ArrayContains { field, value } => Ok(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContains(
                field.clone(),
                sql_value_to_firestore(value, documents_path)?,
            ),
        ))),
        FilterExpr::ArrayContainsAny { field, values } => Ok(FirestoreQueryFilter::Compare(Some(
            FirestoreQueryFilterCompare::ArrayContainsAny(
                field.clone(),
                sql_values_to_firestore_array(values, documents_path)?,
            ),
        ))),
        FilterExpr::InList {
            field,
            values,
            negated,
        } => {
            let value = sql_values_to_firestore_array(values, documents_path)?;
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
                    .map(|f| build_filter(f, documents_path))
                    .collect::<Result<Vec<_>>>()?,
            },
        )),
        FilterExpr::Or(filters) => Ok(FirestoreQueryFilter::Composite(
            FirestoreQueryFilterComposite {
                operator: FirestoreQueryFilterCompositeOperator::Or,
                for_all_filters: filters
                    .iter()
                    .map(|f| build_filter(f, documents_path))
                    .collect::<Result<Vec<_>>>()?,
            },
        )),
    }
}

fn compare_op_to_firestore(
    field: &str,
    op: CompareOp,
    value: &SqlValue,
    documents_path: Option<&str>,
) -> Result<FirestoreQueryFilterCompare> {
    let firestore_value = sql_value_to_firestore(value, documents_path)?;
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

pub(crate) fn sql_value_to_firestore(
    value: &SqlValue,
    documents_path: Option<&str>,
) -> Result<FirestoreValue> {
    match value {
        SqlValue::Literal(json) => Ok(json.clone().into()),
        SqlValue::Reference(path) => {
            let full = expand_reference_path(path, documents_path)?;
            Ok(FirestoreReference(full).into())
        }
        SqlValue::Timestamp(ts) => Ok(FirestoreTimestamp(*ts).into()),
        SqlValue::CurrentTimestamp => Ok(FirestoreTimestamp(Utc::now()).into()),
    }
}

pub(crate) fn sql_values_to_firestore_array(
    values: &[SqlValue],
    documents_path: Option<&str>,
) -> Result<FirestoreValue> {
    let mut array_values = Vec::with_capacity(values.len());
    for value in values {
        let fv = sql_value_to_firestore(value, documents_path)?;
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

fn firestore_query_collection(collection: &CollectionSpec) -> FirestoreQueryCollection {
    if collection.is_group {
        FirestoreQueryCollection::Group(vec![collection.collection_id.clone()])
    } else {
        FirestoreQueryCollection::Single(collection.collection_id.clone())
    }
}

fn is_absolute_resource_name(path: &str) -> bool {
    let mut segments = path.split('/');
    segments.next() == Some("projects")
        && segments.next().is_some()
        && segments.next() == Some("databases")
        && segments.next().is_some()
        && segments.next() == Some("documents")
}

fn expand_reference_path(path: &str, documents_path: Option<&str>) -> Result<String> {
    if is_absolute_resource_name(path) {
        return Ok(path.to_string());
    }
    let base = documents_path
        .ok_or_else(|| FireqlError::InvalidQuery("ref(path) requires absolute path".to_string()))?;
    Ok(format!("{base}/{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{CollectionSpec, OrderDirection};
    use gcloud_sdk::google::firestore::v1::value::ValueType;
    use serde_json::Value as JsonValue;

    fn collection() -> CollectionSpec {
        CollectionSpec {
            collection_id: "users".to_string(),
            parent_path: None,
            is_group: false,
        }
    }

    #[test]
    fn subcollection_query_sets_parent() {
        let col = CollectionSpec {
            collection_id: "posts".to_string(),
            parent_path: Some("users/u1".to_string()),
            is_group: false,
        };
        let params = build_query_params(
            &col,
            None,
            &[],
            None,
            None,
            Some("projects/x/databases/(default)/documents"),
        )
        .unwrap();
        assert_eq!(
            params.parent.as_deref(),
            Some("projects/x/databases/(default)/documents/users/u1")
        );
        assert_eq!(
            params.collection_id,
            FirestoreQueryCollection::Single("posts".to_string())
        );
        assert_eq!(params.all_descendants, None);
    }

    #[test]
    fn collection_group_still_all_descendants() {
        let col = CollectionSpec {
            collection_id: "posts".to_string(),
            parent_path: None,
            is_group: true,
        };
        let params = build_query_params(&col, None, &[], None, None, None).unwrap();
        assert_eq!(params.parent, None);
        assert_eq!(params.all_descendants, Some(true));
    }

    #[test]
    fn inequality_without_order_by_is_allowed() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::Gt,
            value: SqlValue::Literal(JsonValue::from(10)),
        };
        let result = build_query_params(&collection(), Some(&filter), &[], None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn inequality_requires_matching_first_order_by() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::GtEq,
            value: SqlValue::Literal(JsonValue::from(10)),
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
            value: SqlValue::Literal(JsonValue::from(10)),
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
                value: SqlValue::Literal(JsonValue::from(10)),
            },
            FilterExpr::Compare {
                field: "score".to_string(),
                op: CompareOp::Lt,
                value: SqlValue::Literal(JsonValue::from(5)),
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
    fn or_branches_allow_independent_in_filters() {
        // Each OR branch gets its own disjunction budget, so two IN filters
        // on different fields are valid when they live in separate branches.
        let filter = FilterExpr::Or(vec![
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("a"))],
                negated: false,
            },
            FilterExpr::InList {
                field: "role".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("b"))],
                negated: false,
            },
        ]);
        let result = build_query_params(&collection(), Some(&filter), &[], None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn multiple_in_in_same_and_still_rejected() {
        let filter = FilterExpr::And(vec![
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("a"))],
                negated: false,
            },
            FilterExpr::InList {
                field: "role".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("b"))],
                negated: false,
            },
        ]);
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn in_values_limit() {
        let filter = FilterExpr::InList {
            field: "age".to_string(),
            values: (0..31)
                .map(|v| SqlValue::Literal(JsonValue::from(v)))
                .collect(),
            negated: false,
        };
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn in_allows_up_to_thirty_values() {
        let filter = FilterExpr::InList {
            field: "age".to_string(),
            values: (0..30)
                .map(|v| SqlValue::Literal(JsonValue::from(v)))
                .collect(),
            negated: false,
        };
        let result = build_query_params(&collection(), Some(&filter), &[], None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn not_in_still_limited_to_ten_values() {
        let filter = FilterExpr::InList {
            field: "age".to_string(),
            values: (0..11)
                .map(|v| SqlValue::Literal(JsonValue::from(v)))
                .collect(),
            negated: true,
        };
        let err =
            build_query_params(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn array_contains_any_allows_up_to_thirty_values() {
        let filter = FilterExpr::ArrayContainsAny {
            field: "tags".to_string(),
            values: (0..30)
                .map(|v| SqlValue::Literal(JsonValue::from(v)))
                .collect(),
        };
        let result = build_query_params(&collection(), Some(&filter), &[], None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn not_in_conflicts_with_not_equal() {
        let filter = FilterExpr::And(vec![
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("a"))],
                negated: true,
            },
            FilterExpr::Compare {
                field: "score".to_string(),
                op: CompareOp::NotEq,
                value: SqlValue::Literal(JsonValue::from(1)),
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
                values: vec![SqlValue::Literal(JsonValue::from("a"))],
                negated: false,
            },
            FilterExpr::InList {
                field: "role".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("b"))],
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
                values: vec![SqlValue::Literal(JsonValue::from("a"))],
            },
            FilterExpr::InList {
                field: "status".to_string(),
                values: vec![SqlValue::Literal(JsonValue::from("b"))],
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
            value: SqlValue::Literal(JsonValue::from(10)),
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
        let value = SqlValue::Reference("users/u1".to_string());
        let fv = sql_value_to_firestore(&value, Some("projects/p/databases/(default)/documents"))
            .unwrap();
        match fv.value.value_type {
            Some(ValueType::ReferenceValue(path)) => {
                assert_eq!(path, "projects/p/databases/(default)/documents/users/u1");
            }
            _ => panic!("expected reference value"),
        }
    }

    #[test]
    fn reference_relative_path_starting_with_projects_is_expanded() {
        let value = SqlValue::Reference("projects/p1".to_string());
        let fv = sql_value_to_firestore(&value, Some("projects/p/databases/(default)/documents"))
            .unwrap();
        match fv.value.value_type {
            Some(ValueType::ReferenceValue(path)) => {
                assert_eq!(path, "projects/p/databases/(default)/documents/projects/p1");
            }
            _ => panic!("expected reference value"),
        }
    }

    #[test]
    fn reference_absolute_path_is_preserved() {
        let abs = "projects/p/databases/(default)/documents/users/u1";
        let value = SqlValue::Reference(abs.to_string());
        let fv =
            sql_value_to_firestore(&value, Some("projects/other/databases/(default)/documents"))
                .unwrap();
        match fv.value.value_type {
            Some(ValueType::ReferenceValue(path)) => {
                assert_eq!(path, abs);
            }
            _ => panic!("expected reference value"),
        }
    }

    #[test]
    fn timestamp_value_parses_rfc3339() {
        let parsed = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let value = SqlValue::Timestamp(parsed);
        let fv = sql_value_to_firestore(&value, None).unwrap();
        match fv.value.value_type {
            Some(ValueType::TimestampValue(ts)) => {
                assert_eq!(ts.seconds, 1704067200);
            }
            _ => panic!("expected timestamp value"),
        }
    }

    #[test]
    fn current_timestamp_value_is_now() {
        let value = SqlValue::CurrentTimestamp;
        let fv = sql_value_to_firestore(&value, None).unwrap();
        match fv.value.value_type {
            Some(ValueType::TimestampValue(ts)) => {
                let now = Utc::now().timestamp();
                assert!((ts.seconds - now).abs() <= 10);
            }
            _ => panic!("expected timestamp value"),
        }
    }
}
