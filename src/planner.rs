use crate::error::{FireqlError, Result};
use crate::sql::{
    AggregationExpr, AggregationFunc, CollectionSpec, CompareOp, FilterExpr, OrderBy,
    OrderDirection, Projection, SqlValue, UnaryOp,
};
use chrono::{DateTime, Utc};
use firestore::select_filter_builder::FirestoreQueryFilterBuilder;
use firestore::{
    FirestoreQueryCollection, FirestoreQueryDirection, FirestoreQueryFilter, FirestoreQueryOrder,
    FirestoreValue,
};
use serde::Serialize;

// Firestore disjunction value limits. `in` and `array-contains-any` allow 30,
// but `not-in` is stricter at 10.
pub(crate) const MAX_IN_VALUES: usize = 30;
pub(crate) const MAX_NOT_IN_VALUES: usize = 10;
pub(crate) const MAX_ARRAY_CONTAINS_ANY_VALUES: usize = 30;

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedSelect {
    pub collection_id: FirestoreQueryCollection,
    pub parent: Option<String>,
    pub all_descendants: bool,
    pub filter: Option<FirestoreQueryFilter>,
    pub order_by: Vec<FirestoreQueryOrder>,
    pub limit: Option<u32>,
    pub return_only_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedAggregation {
    pub select: PlannedSelect, // order_by/limit empty; enforced in plan_aggregation
    pub aggregations: Vec<AggregationExpr>, // fireql SQL AST; Fluent mapping at execute time
}

pub fn plan_select(
    collection: &CollectionSpec,
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
    limit: Option<u32>,
    projection: Option<&Projection>,
    documents_path: Option<&str>,
) -> Result<PlannedSelect> {
    validate_query_constraints(filter, order_by)?;

    let collection_id = firestore_query_collection(collection);
    let all_descendants = collection.is_group;

    let parent = if let Some(pp) = &collection.parent_path {
        let base = documents_path.ok_or_else(|| {
            FireqlError::InvalidQuery(
                "collection() with a subcollection path requires database context".to_string(),
            )
        })?;
        Some(format!("{base}/{pp}"))
    } else {
        None
    };

    let fb = FirestoreQueryFilterBuilder;
    let filter = if let Some(filter_expr) = filter {
        build_filter(&fb, filter_expr, documents_path)?
    } else {
        None
    };

    let order_by = if !order_by.is_empty() {
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
        order
    } else {
        Vec::new()
    };

    let return_only_fields = match projection {
        Some(Projection::Fields(fields)) => Some(fields.clone()),
        _ => None,
    };

    Ok(PlannedSelect {
        collection_id,
        parent,
        all_descendants,
        filter,
        order_by,
        limit,
        return_only_fields,
    })
}

pub fn plan_aggregation(
    collection: &CollectionSpec,
    filter: Option<&FilterExpr>,
    order_by: &[OrderBy],
    limit: Option<u32>,
    aggregations: &[AggregationExpr],
    documents_path: Option<&str>,
) -> Result<PlannedAggregation> {
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
    for agg in aggregations {
        match agg.func {
            AggregationFunc::Count => {}
            AggregationFunc::Sum => {
                if agg.field.is_none() {
                    return Err(FireqlError::InvalidQuery(
                        "SUM requires a field".to_string(),
                    ));
                }
            }
            AggregationFunc::Avg => {
                if agg.field.is_none() {
                    return Err(FireqlError::InvalidQuery(
                        "AVG requires a field".to_string(),
                    ));
                }
            }
        }
    }
    let select = plan_select(collection, filter, order_by, limit, None, documents_path)?;
    Ok(PlannedAggregation {
        select,
        aggregations: aggregations.to_vec(),
    })
}

mod validate;

use validate::validate_query_constraints;

pub fn build_filter(
    fb: &FirestoreQueryFilterBuilder,
    filter: &FilterExpr,
    documents_path: Option<&str>,
) -> Result<Option<FirestoreQueryFilter>> {
    match filter {
        FilterExpr::Compare { field, op, value } => {
            let firestore_value = sql_value_to_firestore(value, documents_path)?;
            let f = fb.field(field);
            let filter = match op {
                CompareOp::Eq => f.eq(firestore_value),
                CompareOp::NotEq => f.neq(firestore_value),
                CompareOp::Lt => f.less_than(firestore_value),
                CompareOp::LtEq => f.less_than_or_equal(firestore_value),
                CompareOp::Gt => f.greater_than(firestore_value),
                CompareOp::GtEq => f.greater_than_or_equal(firestore_value),
            };
            Ok(filter)
        }
        FilterExpr::ArrayContains { field, value } => {
            let firestore_value = sql_value_to_firestore(value, documents_path)?;
            Ok(fb.field(field).array_contains(firestore_value))
        }
        FilterExpr::ArrayContainsAny { field, values } => {
            let firestore_value = sql_values_to_firestore_array(values, documents_path)?;
            Ok(fb.field(field).array_contains_any(firestore_value))
        }
        FilterExpr::InList {
            field,
            values,
            negated,
        } => {
            let value = sql_values_to_firestore_array(values, documents_path)?;
            let f = fb.field(field);
            let filter = if *negated {
                f.is_not_in(value)
            } else {
                f.is_in(value)
            };
            Ok(filter)
        }
        FilterExpr::Unary { field, op } => {
            let f = fb.field(field);
            let filter = match op {
                UnaryOp::IsNull => f.is_null(),
                UnaryOp::IsNotNull => f.is_not_null(),
            };
            Ok(filter)
        }
        FilterExpr::And(filters) => {
            let built: Vec<Option<FirestoreQueryFilter>> = filters
                .iter()
                .map(|f| build_filter(fb, f, documents_path))
                .collect::<Result<Vec<_>>>()?;
            Ok(fb.for_all(built))
        }
        FilterExpr::Or(filters) => {
            let built: Vec<Option<FirestoreQueryFilter>> = filters
                .iter()
                .map(|f| build_filter(fb, f, documents_path))
                .collect::<Result<Vec<_>>>()?;
            Ok(fb.for_any(built))
        }
    }
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
        let plan = plan_select(
            &col,
            None,
            &[],
            None,
            None,
            Some("projects/x/databases/(default)/documents"),
        )
        .unwrap();
        assert_eq!(
            plan.parent.as_deref(),
            Some("projects/x/databases/(default)/documents/users/u1")
        );
        assert_eq!(
            plan.collection_id,
            FirestoreQueryCollection::Single("posts".to_string())
        );
        assert!(!plan.all_descendants);
    }

    #[test]
    fn collection_group_still_all_descendants() {
        let col = CollectionSpec {
            collection_id: "posts".to_string(),
            parent_path: None,
            is_group: true,
        };
        let plan = plan_select(&col, None, &[], None, None, None).unwrap();
        assert_eq!(plan.parent, None);
        assert!(plan.all_descendants);
    }

    #[test]
    fn inequality_without_order_by_is_allowed() {
        let filter = FilterExpr::Compare {
            field: "age".to_string(),
            op: CompareOp::Gt,
            value: SqlValue::Literal(JsonValue::from(10)),
        };
        let result = plan_select(&collection(), Some(&filter), &[], None, None, None);
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
        let err =
            plan_select(&collection(), Some(&filter), &order_by, None, None, None).unwrap_err();
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
        let plan = plan_select(&collection(), Some(&filter), &order_by, None, None, None);
        assert!(plan.is_ok());
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
        let err =
            plan_select(&collection(), Some(&filter), &order_by, None, None, None).unwrap_err();
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
        let result = plan_select(&collection(), Some(&filter), &[], None, None, None);
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
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
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
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
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
        let result = plan_select(&collection(), Some(&filter), &[], None, None, None);
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
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
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
        let result = plan_select(&collection(), Some(&filter), &[], None, None, None);
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
        let err =
            plan_select(&collection(), Some(&filter), &order_by, None, None, None).unwrap_err();
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
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn array_contains_any_requires_values() {
        let filter = FilterExpr::ArrayContainsAny {
            field: "tags".to_string(),
            values: vec![],
        };
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
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
        let err = plan_select(&collection(), Some(&filter), &[], None, None, None).unwrap_err();
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

        let err = plan_aggregation(
            &collection(),
            Some(&filter),
            &order_by,
            None,
            std::slice::from_ref(&agg),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));

        let err = plan_aggregation(&collection(), Some(&filter), &[], Some(10), &[agg], None)
            .unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));
    }

    #[test]
    fn sum_and_avg_require_field() {
        let sum_agg = AggregationExpr {
            func: AggregationFunc::Sum,
            field: None,
            alias: "s".to_string(),
        };
        let err = plan_aggregation(&collection(), None, &[], None, &[sum_agg], None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidQuery(_)));

        let avg_agg = AggregationExpr {
            func: AggregationFunc::Avg,
            field: None,
            alias: "a".to_string(),
        };
        let err = plan_aggregation(&collection(), None, &[], None, &[avg_agg], None).unwrap_err();
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
