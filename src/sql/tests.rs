use super::*;

#[test]
fn parse_select_with_filter_order_limit() {
    let stmt = parse_sql("SELECT * FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "users");
            assert!(select.collection.parent_path.is_none());
            assert!(!select.collection.is_group);
            assert!(matches!(
                select.projection,
                SelectProjection::Fields(Projection::All)
            ));
            assert!(select.filter.is_some());
            assert_eq!(select.order_by.len(), 1);
            assert_eq!(select.limit, Some(10));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_collection_group_select() {
    let stmt =
        parse_sql("SELECT name FROM collection_group('profiles') WHERE active = true").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "profiles");
            assert!(select.collection.parent_path.is_none());
            assert!(select.collection.is_group);
            assert!(matches!(
                select.projection,
                SelectProjection::Fields(Projection::Fields(_))
            ));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn update_requires_where() {
    let err = parse_sql("UPDATE users SET status = 'active'").unwrap_err();
    assert!(matches!(err, FireqlError::MissingWhere));
}

#[test]
fn parse_update_with_order_by_and_limit() {
    let stmt = parse_sql("UPDATE users SET status = 'active' WHERE age >= 18 ORDER BY age LIMIT 5")
        .unwrap();
    match stmt {
        StatementAst::Update(update) => {
            assert_eq!(update.order_by.len(), 1);
            assert_eq!(update.order_by[0].field, "age");
            assert!(matches!(update.order_by[0].direction, OrderDirection::Asc));
            assert_eq!(update.limit, Some(5));
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn delete_collection_group_requires_where() {
    let err = parse_sql("DELETE FROM collection_group('logs')").unwrap_err();
    assert!(matches!(err, FireqlError::MissingWhere));
}

#[test]
fn parse_delete_collection_group_with_where() {
    let stmt =
        parse_sql("DELETE FROM collection_group('logs') WHERE created_at < '2023-01-01'").unwrap();
    match stmt {
        StatementAst::Delete(delete) => {
            assert_eq!(delete.collection.collection_id, "logs");
            assert!(delete.collection.parent_path.is_none());
            assert!(delete.collection.is_group);
        }
        _ => panic!("expected delete"),
    }
}

#[test]
fn parse_collection_shorthand() {
    let stmt = parse_sql("SELECT * FROM collection('posts') WHERE draft = false").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "posts");
            assert!(select.collection.parent_path.is_none());
            assert!(!select.collection.is_group);
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_collection_subcollection() {
    let stmt =
        parse_sql("SELECT * FROM collection('users/user1/posts') WHERE author = 'x'").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "posts");
            assert_eq!(
                select.collection.parent_path.as_deref(),
                Some("users/user1")
            );
            assert!(!select.collection.is_group);
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_collection_bare_identifier() {
    let stmt = parse_sql("SELECT * FROM collection(posts) WHERE draft = false").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "posts");
            assert!(select.collection.parent_path.is_none());
            assert!(!select.collection.is_group);
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn unsupported_query_error_is_concise_sql() {
    let err = parse_sql("SELECT * FROM users WHERE a BETWEEN 1 AND 5").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("a BETWEEN 1 AND 5"), "got: {msg}");
    assert!(!msg.contains("Span"), "error leaks AST debug noise: {msg}");
    assert!(
        !msg.contains("Location"),
        "error leaks AST debug noise: {msg}"
    );
}

#[test]
fn parse_update_delete_collection_subcollection() {
    let u = parse_sql("UPDATE collection('users/user1/posts') SET ok = true WHERE n = 1").unwrap();
    match u {
        StatementAst::Update(up) => {
            assert_eq!(up.collection.collection_id, "posts");
            assert_eq!(up.collection.parent_path.as_deref(), Some("users/user1"));
        }
        _ => panic!("expected update"),
    }
    let d = parse_sql("DELETE FROM collection('users/user1/posts') WHERE n = 1").unwrap();
    match d {
        StatementAst::Delete(del) => {
            assert_eq!(del.collection.collection_id, "posts");
            assert_eq!(del.collection.parent_path.as_deref(), Some("users/user1"));
        }
        _ => panic!("expected delete"),
    }
}

#[test]
fn parse_insert_select_auto_id_copy() {
    let stmt =
        parse_sql("INSERT INTO archived_users SELECT * FROM users WHERE disabled = true").unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "archived_users");
            assert!(insert.columns.is_none());
            assert_eq!(insert.source.collection.collection_id, "users");
            assert!(matches!(
                insert.source.projection,
                SelectProjection::Fields(Projection::All)
            ));
            assert!(insert.source.filter.is_some());
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_subcollections() {
    let stmt = parse_sql(
        "INSERT INTO collection('users/u1/archive') \
             SELECT * FROM collection('users/u1/posts') WHERE published = false",
    )
    .unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "archive");
            assert_eq!(insert.collection.parent_path.as_deref(), Some("users/u1"));
            assert_eq!(insert.source.collection.collection_id, "posts");
            assert_eq!(
                insert.source.collection.parent_path.as_deref(),
                Some("users/u1")
            );
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_with_id_preservation_columns() {
    let stmt = parse_sql(
        "INSERT INTO archived_users (__name__, name, age) \
             SELECT __name__, name, age FROM users WHERE disabled = true",
    )
    .unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(
                insert.columns.as_ref().expect("columns"),
                &vec![
                    "__name__".to_string(),
                    "name".to_string(),
                    "age".to_string()
                ]
            );
            assert!(matches!(
                insert.source.projection,
                SelectProjection::Fields(Projection::Fields(_))
            ));
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_allows_collection_named_collection_with_columns() {
    let stmt = parse_sql("INSERT INTO collection (name) SELECT name FROM users").unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "collection");
            assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn insert_select_name_destination_requires_name_source() {
    let err = parse_sql("INSERT INTO archived_users (__name__, name) SELECT id, name FROM users")
        .unwrap_err();
    assert!(err.to_string().contains("__name__"));
}

#[test]
fn insert_select_name_destination_requires_positional_name_source() {
    let err =
        parse_sql("INSERT INTO archived_users (__name__, name) SELECT name, __name__ FROM users")
            .unwrap_err();
    assert!(err.to_string().contains("same SELECT field position"));
}

#[test]
fn insert_select_rejects_aggregation() {
    let err = parse_sql("INSERT INTO archived_users SELECT COUNT(*) FROM users").unwrap_err();
    assert!(err.to_string().contains("Aggregation is not supported"));
}

#[test]
fn insert_select_rejects_collection_group_source() {
    let err = parse_sql("INSERT INTO archived_users SELECT * FROM collection_group('users')")
        .unwrap_err();
    assert!(err.to_string().contains("collection_group"));
}

#[test]
fn parse_insert_select_collection_named_collection_without_space_before_columns() {
    let stmt = parse_sql("INSERT INTO collection(name) SELECT name FROM users").unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "collection");
            assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_collection_named_collection_with_quoted_column() {
    let stmt = parse_sql("INSERT INTO collection(\"name\") SELECT name FROM users").unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "collection");
            assert_eq!(insert.columns.as_ref().expect("columns"), &vec!["name"]);
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_join_collection_subcollection() {
    let sql =
        "SELECT * FROM collection('users/user1/posts') p INNER JOIN users u ON u.id = p.author_id";
    parse_sql(sql).unwrap();
}

#[test]
fn collection_path_rejects_invalid() {
    for bad in [
        "",
        "/users/u1/posts",
        "users/u1/posts/",
        "users//u1/posts",
        "users/u1",
    ] {
        let err = parse_sql(&format!("SELECT * FROM collection('{bad}') WHERE x = 1")).unwrap_err();
        assert!(
            err.to_string().contains(super::COLLECTION_PATH_ERR),
            "unexpected err for {bad:?}: {err}"
        );
    }
}

#[test]
fn parse_array_contains() {
    let stmt = parse_sql("SELECT * FROM users WHERE array_contains(tags, 'a')").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let filter = select.filter.expect("filter");
            match filter {
                FilterExpr::ArrayContains { field, value } => {
                    assert_eq!(field, "tags");
                    assert_eq!(value, JsonValue::from("a"));
                }
                _ => panic!("expected array_contains filter"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_array_contains_any() {
    let stmt = parse_sql("SELECT * FROM users WHERE array_contains_any(tags, ['a','b'])").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let filter = select.filter.expect("filter");
            match filter {
                FilterExpr::ArrayContainsAny { field, values } => {
                    assert_eq!(field, "tags");
                    assert_eq!(values.len(), 2);
                }
                _ => panic!("expected array_contains_any filter"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_ref_value() {
    let stmt = parse_sql("SELECT * FROM users WHERE owner = ref('users/user1')").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let filter = select.filter.expect("filter");
            match filter {
                FilterExpr::Compare { value, .. } => {
                    let obj = value.as_object().expect("object");
                    assert_eq!(obj.get(FIREQL_REF_KEY).unwrap(), "users/user1");
                }
                _ => panic!("expected compare filter"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_timestamp_value() {
    let stmt =
        parse_sql("SELECT * FROM users WHERE created_at >= timestamp('2024-01-01T00:00:00Z')")
            .unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let filter = select.filter.expect("filter");
            match filter {
                FilterExpr::Compare { value, .. } => {
                    let obj = value.as_object().expect("object");
                    assert_eq!(obj.get(FIREQL_TS_KEY).unwrap(), "2024-01-01T00:00:00Z");
                }
                _ => panic!("expected compare filter"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_current_timestamp_value() {
    let stmt = parse_sql("SELECT * FROM users WHERE created_at >= CURRENT_TIMESTAMP").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let filter = select.filter.expect("filter");
            match filter {
                FilterExpr::Compare { value, .. } => {
                    let obj = value.as_object().expect("object");
                    assert_eq!(
                        obj.get(FIREQL_CURRENT_TS_KEY).unwrap(),
                        &JsonValue::Bool(true)
                    );
                }
                _ => panic!("expected compare filter"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_update_with_current_timestamp_assignment() {
    let stmt = parse_sql("UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE status = 'active'")
        .unwrap();
    match stmt {
        StatementAst::Update(update) => {
            assert_eq!(update.assignments.len(), 1);
            let (field, value) = &update.assignments[0];
            assert_eq!(field, "updated_at");
            let obj = value.as_object().expect("object");
            assert_eq!(
                obj.get(FIREQL_CURRENT_TS_KEY).unwrap(),
                &JsonValue::Bool(true)
            );
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn parse_count_aggregate() {
    let stmt = parse_sql("SELECT COUNT(*) FROM users").unwrap();
    match stmt {
        StatementAst::Select(select) => match select.projection {
            SelectProjection::Aggregations(aggs) => {
                assert_eq!(aggs.len(), 1);
                assert!(matches!(aggs[0].func, AggregationFunc::Count));
                assert_eq!(aggs[0].alias, "count");
            }
            _ => panic!("expected aggregation"),
        },
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_count_field_is_count_star() {
    let stmt = parse_sql("SELECT COUNT(age) FROM users").unwrap();
    match stmt {
        StatementAst::Select(select) => match select.projection {
            SelectProjection::Aggregations(aggs) => {
                assert_eq!(aggs.len(), 1);
                assert!(matches!(aggs[0].func, AggregationFunc::Count));
                assert_eq!(aggs[0].alias, "count");
                assert!(aggs[0].field.is_none());
            }
            _ => panic!("expected aggregation"),
        },
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_sum_aggregate_with_alias() {
    let stmt = parse_sql("SELECT SUM(score) AS total FROM users").unwrap();
    match stmt {
        StatementAst::Select(select) => match select.projection {
            SelectProjection::Aggregations(aggs) => {
                assert_eq!(aggs.len(), 1);
                assert!(matches!(aggs[0].func, AggregationFunc::Sum));
                assert_eq!(aggs[0].alias, "total");
            }
            _ => panic!("expected aggregation"),
        },
        _ => panic!("expected select"),
    }
}

#[test]
fn aggregate_cannot_mix_fields() {
    let err = parse_sql("SELECT name, COUNT(*) FROM users").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn distinct_is_rejected() {
    let err = parse_sql("SELECT DISTINCT name FROM users").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn group_by_is_rejected() {
    let err = parse_sql("SELECT COUNT(*) FROM users GROUP BY team").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn having_is_rejected() {
    let err =
        parse_sql("SELECT COUNT(*) FROM users GROUP BY team HAVING COUNT(*) > 1").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn offset_is_rejected() {
    let err = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 20").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn top_is_rejected() {
    let err = parse_sql("SELECT TOP 5 * FROM users").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn qualify_is_rejected() {
    let err = parse_sql("SELECT * FROM users QUALIFY ROW_NUMBER() OVER () = 1").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn prewhere_is_rejected() {
    let err = parse_sql("SELECT * FROM users PREWHERE active = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn cluster_by_is_rejected() {
    let err = parse_sql("SELECT * FROM users CLUSTER BY name").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn sort_by_is_rejected() {
    let err = parse_sql("SELECT * FROM users SORT BY name").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn window_clause_is_rejected() {
    let err = parse_sql("SELECT * FROM users WINDOW w AS (PARTITION BY team)").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_into_is_rejected() {
    let err = parse_sql("SELECT * INTO archived FROM users").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn tablesample_is_rejected() {
    let err = parse_sql("SELECT * FROM users TABLESAMPLE BERNOULLI (10)").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn parse_inner_join() {
    let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.collection.collection_id, "users");
            let join = select.joins.as_ref().expect("should have join");
            assert_eq!(join.len(), 1);
            assert_eq!(join[0].collection.collection_id, "orders");
            assert!(matches!(join[0].join_type, JoinType::Inner));
            assert_eq!(join[0].left_field, "id");
            assert_eq!(join[0].right_field, "user_id");
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_left_join() {
    let sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let join = select.joins.as_ref().expect("should have join");
            assert!(matches!(join[0].join_type, JoinType::Left));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_with_alias() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert_eq!(select.alias.as_deref(), Some("u"));
            let join = select.joins.as_ref().expect("should have join");
            assert_eq!(join[0].left_alias.as_deref(), Some("u"));
            assert_eq!(join[0].right_alias.as_deref(), Some("o"));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_rejects_unsupported_join_types() {
    let sql = "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id";
    let err = parse_sql(sql).unwrap_err();
    assert!(err
        .to_string()
        .contains("Only INNER JOIN and LEFT JOIN are supported"));
}

#[test]
fn parse_join_rejects_non_equality_on() {
    let sql = "SELECT * FROM users INNER JOIN orders ON users.id > orders.user_id";
    let err = parse_sql(sql).unwrap_err();
    assert!(err.to_string().contains("Only equality conditions"));
}

#[test]
fn parse_join_rejects_aggregation_with_join() {
    let sql = "SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id";
    let err = parse_sql(sql).unwrap_err();
    assert!(err.to_string().contains("Aggregation"));
}

#[test]
fn parse_join_with_qualified_fields() {
    let sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => match &select.projection {
            SelectProjection::Fields(Projection::Fields(fields)) => {
                assert_eq!(fields, &["u.name", "o.amount"]);
            }
            _ => panic!("expected fields projection"),
        },
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_with_where() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert!(select.joins.is_some());
            assert!(select.filter.is_some());
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_with_order_by_and_limit() {
    let sql =
        "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY u.name LIMIT 10";
    let err = parse_sql(sql).unwrap_err();
    assert!(err
        .to_string()
        .contains("ORDER BY is not supported with JOIN"));
}

#[test]
fn parse_join_rejects_order_by_with_join() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY u.name";
    let err = parse_sql(sql).unwrap_err();
    assert!(err
        .to_string()
        .contains("ORDER BY is not supported with JOIN"));
}

#[test]
fn parse_join_rejects_limit_with_join() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LIMIT 10";
    let err = parse_sql(sql).unwrap_err();
    assert!(err.to_string().contains("LIMIT is not supported with JOIN"));
}

#[test]
fn join_where_referencing_right_alias_is_rejected() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE o.amount > 100";
    let err = parse_sql(sql).unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)), "got: {err:?}");
}

#[test]
fn join_where_referencing_left_alias_is_allowed() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE u.active = true";
    assert!(parse_sql(sql).is_ok());
}

#[test]
fn join_where_nested_field_is_allowed() {
    let sql = "SELECT * FROM users u INNER JOIN orders o ON u.__name__ = o.user_id \
                   WHERE profile.age > 18";
    assert!(parse_sql(sql).is_ok());
}

#[test]
fn parse_left_outer_join() {
    let sql = "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            let join = select.joins.as_ref().expect("should have join");
            assert!(matches!(join[0].join_type, JoinType::Left));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_without_alias() {
    let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
    let stmt = parse_sql(sql).unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert!(select.alias.is_none());
            let join = select.joins.as_ref().expect("should have join");
            assert_eq!(join[0].left_alias.as_deref(), Some("users"));
            assert_eq!(join[0].right_alias.as_deref(), Some("orders"));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn parse_join_using_clause_rejected() {
    let sql = "SELECT * FROM users INNER JOIN orders USING (id)";
    let err = parse_sql(sql).unwrap_err();
    assert!(err
        .to_string()
        .contains("Only INNER JOIN and LEFT JOIN are supported"));
}

#[test]
fn select_wildcard_with_fields_is_all() {
    let stmt = parse_sql("SELECT *, name FROM users").unwrap();
    match stmt {
        StatementAst::Select(select) => {
            assert!(matches!(
                select.projection,
                SelectProjection::Fields(Projection::All)
            ));
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn delete_using_is_rejected() {
    let err = parse_sql("DELETE FROM users USING orders WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn delete_returning_is_rejected() {
    let err = parse_sql("DELETE FROM users WHERE flag = true RETURNING id").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_from_is_rejected() {
    let err = parse_sql("UPDATE users SET a = 1 FROM orders WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_returning_is_rejected() {
    let err = parse_sql("UPDATE users SET a = 1 WHERE flag = true RETURNING id").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_or_conflict_clause_is_rejected() {
    let err = parse_sql("UPDATE OR IGNORE users SET a = 1 WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_with_cte_is_rejected() {
    let err = parse_sql("WITH x AS (SELECT * FROM users) SELECT * FROM x").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_fetch_is_rejected() {
    let err = parse_sql("SELECT * FROM users FETCH FIRST 5 ROWS ONLY").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_for_update_is_rejected() {
    let err = parse_sql("SELECT * FROM users FOR UPDATE").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}
