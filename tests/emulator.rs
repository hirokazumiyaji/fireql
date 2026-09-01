mod support;

use fireql::{DocOutput, FireqlError, FireqlOutput, FireqlStream, FireqlValue};
use futures::TryStreamExt;
use serde_json::json;
use support::{
    open_db, open_fireql, open_fireql_with_access_token, project_id, should_skip, unique_suffix,
};

async fn create_test_doc(
    db: &firestore::FirestoreDb,
    collection: &str,
    doc_id: &str,
    data: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let _: serde_json::Value = db
        .fluent()
        .insert()
        .into(collection)
        .document_id(doc_id)
        .object(data)
        .execute()
        .await?;
    Ok(())
}

async fn create_test_doc_at(
    db: &firestore::FirestoreDb,
    parent: &str,
    collection: &str,
    doc_id: &str,
    data: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let _: serde_json::Value = db
        .fluent()
        .insert()
        .into(collection)
        .document_id(doc_id)
        .parent(parent)
        .object(data)
        .execute()
        .await?;
    Ok(())
}

#[tokio::test]
async fn emulator_select_update_delete() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_users_{}", unique_suffix());
    let doc_id = "user1";

    let data = json!({
        "age": 30,
        "active": true,
    });
    create_test_doc(&db, &collection, doc_id, &data).await?;

    let select_sql = format!("SELECT * FROM {collection} WHERE age = 30 LIMIT 10");
    let output = fireql.execute(&select_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, doc_id);
            assert!(rows[0].path.contains(&collection));
        }
        _ => panic!("expected rows"),
    }

    let update_sql = format!("UPDATE {collection} SET active = false WHERE age = 30");
    let output = fireql.execute(&update_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => {
            assert_eq!(affected, 1);
        }
        _ => panic!("expected affected"),
    }

    let delete_sql = format!("DELETE FROM {collection} WHERE age = 30");
    let output = fireql.execute(&delete_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => {
            assert_eq!(affected, 1);
        }
        _ => panic!("expected affected"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_with_access_token_select_insert() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql_with_access_token(&project_id, "owner").await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_access_token_source_{suffix}");
    let dest = format!("fireql_access_token_dest_{suffix}");
    let doc_id = "doc1";
    let data = json!({
        "name": "access-token-user",
        "score": 42,
    });
    create_test_doc(&db, &source, doc_id, &data).await?;

    let select_sql = format!("SELECT * FROM {source} WHERE score = 42 LIMIT 10");
    let output = fireql.execute(&select_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, doc_id);
        }
        _ => panic!("expected rows"),
    }

    let insert_sql = format!("INSERT INTO {dest} SELECT * FROM {source} WHERE score = 42");
    let output = fireql.execute(&insert_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => {
            assert_eq!(affected, 1);
        }
        _ => panic!("expected affected"),
    }

    let verify_sql = format!("SELECT * FROM {dest} WHERE score = 42 LIMIT 10");
    let output = fireql.execute(&verify_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            match rows[0].data.get("name") {
                Some(FireqlValue::String(name)) => assert_eq!(name, "access-token-user"),
                other => panic!("expected copied name, got {other:?}"),
            }
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_update_order_by_limit() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_update_limit_{}", unique_suffix());

    for (doc_id, priority) in [
        ("d01", 1),
        ("d02", 2),
        ("d03", 3),
        ("d04", 4),
        ("d05", 5),
        ("d06", 6),
        ("d07", 7),
        ("d08", 8),
    ] {
        create_test_doc(
            &db,
            &collection,
            doc_id,
            &json!({"status": "pending", "priority": priority}),
        )
        .await?;
    }

    let update_sql = format!(
        "UPDATE {collection} SET status = 'done' \
         WHERE status = 'pending' ORDER BY priority DESC LIMIT 5"
    );
    let output = fireql.execute(&update_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 5),
        other => panic!("expected affected, got {other:?}"),
    }

    let done_sql =
        format!("SELECT priority FROM {collection} WHERE status = 'done' ORDER BY priority DESC");
    let output = fireql.execute(&done_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 5);
            let priorities: Vec<i64> = rows
                .iter()
                .map(|row| match row.data.get("priority") {
                    Some(FireqlValue::Integer(p)) => *p,
                    other => panic!("expected integer priority, got {other:?}"),
                })
                .collect();
            assert_eq!(priorities, vec![8, 7, 6, 5, 4]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    let pending_sql =
        format!("SELECT priority FROM {collection} WHERE status = 'pending' ORDER BY priority ASC");
    let output = fireql.execute(&pending_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 3);
            let priorities: Vec<i64> = rows
                .iter()
                .map(|row| match row.data.get("priority") {
                    Some(FireqlValue::Integer(p)) => *p,
                    other => panic!("expected integer priority, got {other:?}"),
                })
                .collect();
            assert_eq!(priorities, vec![1, 2, 3]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_insert_select_auto_id_copy() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_source_{suffix}");
    let dest = format!("fireql_insert_dest_{suffix}");

    create_test_doc(
        &db,
        &source,
        "u1",
        &json!({"name": "Alice", "disabled": true, "score": 10}),
    )
    .await?;
    create_test_doc(
        &db,
        &source,
        "u2",
        &json!({"name": "Bob", "disabled": false, "score": 20}),
    )
    .await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} SELECT * FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    let output = fireql
        .execute(&format!("SELECT * FROM {dest} WHERE disabled = true"))
        .await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_ne!(rows[0].id, "u1");
            match rows[0].data.get("name") {
                Some(FireqlValue::String(name)) => assert_eq!(name, "Alice"),
                other => panic!("expected copied name, got {other:?}"),
            }
            match rows[0].data.get("score") {
                Some(FireqlValue::Integer(score)) => assert_eq!(*score, 10),
                other => panic!("expected copied score, got {other:?}"),
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_insert_select_preserves_id_when_name_column_is_used(
) -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_named_source_{suffix}");
    let dest = format!("fireql_insert_named_dest_{suffix}");

    create_test_doc(
        &db,
        &source,
        "preserved_id",
        &json!({"name": "Alice", "disabled": true}),
    )
    .await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} (__name__, name) \
             SELECT __name__, name FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    let output = fireql
        .execute(&format!("SELECT * FROM {dest} WHERE name = 'Alice'"))
        .await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, "preserved_id");
            assert_eq!(rows[0].path, format!("{dest}/preserved_id"));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_insert_select_empty_source_reports_zero() -> Result<(), Box<dyn std::error::Error>>
{
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_empty_source_{suffix}");
    let dest = format!("fireql_insert_empty_dest_{suffix}");

    create_test_doc(&db, &source, "u1", &json!({"disabled": false})).await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} SELECT * FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 0),
        other => panic!("expected affected, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_collection_group_select() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let parent_collection = format!("fireql_parents_{}", unique_suffix());
    let parent_id = "parent1";
    create_test_doc(&db, &parent_collection, parent_id, &json!({"name": "p"})).await?;

    let parent_path = format!(
        "{}/{}/{}",
        db.get_documents_path(),
        parent_collection,
        parent_id
    );
    let post_title = format!("hello-{}", unique_suffix());

    create_test_doc_at(
        &db,
        &parent_path,
        "posts",
        "post1",
        &json!({"title": &post_title, "likes": 1}),
    )
    .await?;

    let select_sql =
        format!("SELECT * FROM collection_group('posts') WHERE title = '{post_title}' LIMIT 5");
    let output = fireql.execute(&select_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0].path.contains("/posts/"));
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_collection_subcollection_queries() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let parents_col = format!("fireql_scoped_parents_{}", unique_suffix());
    let users_col = format!("fireql_scoped_users_{}", unique_suffix());
    let title_a = format!("title-a-{}", unique_suffix());
    let title_b = format!("title-b-{}", unique_suffix());

    create_test_doc(&db, &users_col, "u1", &json!({"name": "JoinUser"})).await?;

    create_test_doc(&db, &parents_col, "a", &json!({"label": "A"})).await?;
    create_test_doc(&db, &parents_col, "b", &json!({"label": "B"})).await?;

    let parent_a = format!("{}/{}/{}", db.get_documents_path(), parents_col, "a");

    create_test_doc_at(
        &db,
        &parent_a,
        "posts",
        "p1",
        &json!({"title": &title_a, "n": 1, "user_id": "u1"}),
    )
    .await?;
    let parent_b = format!("{}/{}/{}", db.get_documents_path(), parents_col, "b");
    create_test_doc_at(
        &db,
        &parent_b,
        "posts",
        "p2",
        &json!({"title": &title_b, "n": 2, "user_id": "u1"}),
    )
    .await?;

    let rel_a_posts = format!("{parents_col}/a/posts");
    let scoped_sql =
        format!("SELECT title, n FROM collection('{rel_a_posts}') WHERE title = '{title_a}'");
    let output = fireql.execute(&scoped_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            match rows[0].data.get("title") {
                Some(FireqlValue::String(s)) => assert_eq!(s, &title_a),
                other => panic!("expected string title, got {other:?}"),
            }
        }
        _ => panic!("expected rows"),
    }

    let join_sql = format!(
        "SELECT p.title, u.name FROM collection('{rel_a_posts}') p \
         INNER JOIN {users_col} u ON u.__name__ = p.user_id WHERE p.title = '{title_a}'"
    );
    let output = fireql.execute(&join_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            match rows[0].data.get("u.name") {
                Some(FireqlValue::String(s)) => assert_eq!(s, "JoinUser"),
                other => panic!("expected u.name, got {other:?}"),
            }
        }
        _ => panic!("expected rows"),
    }

    let group_sql = format!(
        "SELECT title FROM collection_group('posts') WHERE title IN ('{title_a}', '{title_b}') ORDER BY title"
    );
    let output = fireql.execute(&group_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected rows"),
    }

    let update_sql =
        format!("UPDATE collection('{rel_a_posts}') SET n = 99 WHERE title = '{title_a}'");
    let output = fireql.execute(&update_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        _ => panic!("expected affected"),
    }

    let check_b_sql =
        format!("SELECT n FROM collection('{parents_col}/b/posts') WHERE title = '{title_b}'");
    let output = fireql.execute(&check_b_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            match rows[0].data.get("n") {
                Some(FireqlValue::Integer(i)) => assert_eq!(*i, 2),
                other => panic!("expected integer n, got {other:?}"),
            }
        }
        _ => panic!("expected rows"),
    }

    let delete_sql = format!("DELETE FROM collection('{rel_a_posts}') WHERE title = '{title_a}'");
    let output = fireql.execute(&delete_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        _ => panic!("expected affected"),
    }

    let verify_sql = format!("SELECT * FROM collection('{rel_a_posts}') WHERE title = '{title_a}'");
    let output = fireql.execute(&verify_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => assert_eq!(rows.len(), 0),
        _ => panic!("expected rows"),
    }

    let verify_b_sql =
        format!("SELECT title FROM collection('{parents_col}/b/posts') WHERE title = '{title_b}'");
    let output = fireql.execute(&verify_b_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => assert_eq!(rows.len(), 1),
        _ => panic!("expected rows"),
    }

    Ok(())
}

/// Covers `executor` JOIN when `join.right_field == "__name__"` and the right-hand
/// `collection(...)` has `parent_path` (subcollection): `doc_path` must include
/// `{documents_path}/{parent_path}/{collection_id}`.
#[tokio::test]
async fn emulator_inner_join_subcollection_right_document_name(
) -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let parents_col = format!("fireql_namejoin_parents_{}", unique_suffix());
    let users_col = format!("fireql_namejoin_users_{}", unique_suffix());
    let post_title = format!("namejoin-{}", unique_suffix());

    create_test_doc(&db, &parents_col, "a", &json!({})).await?;

    let parent_a = format!("{}/{}/{}", db.get_documents_path(), parents_col, "a");
    create_test_doc_at(
        &db,
        &parent_a,
        "posts",
        "doc_for_name",
        &json!({"title": &post_title}),
    )
    .await?;

    create_test_doc(
        &db,
        &users_col,
        "u1",
        &json!({"name": "NameJoinUser", "post_ref": "doc_for_name"}),
    )
    .await?;

    let rel_posts = format!("{parents_col}/a/posts");
    let sql = format!(
        "SELECT p.title, u.name FROM {users_col} u \
         INNER JOIN collection('{rel_posts}') p ON u.post_ref = p.__name__ \
         WHERE u.name = 'NameJoinUser'"
    );
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            match rows[0].data.get("p.title") {
                Some(FireqlValue::String(s)) => assert_eq!(s, &post_title),
                other => panic!("expected p.title, got {other:?}"),
            }
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_inner_join() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let users_col = format!("fireql_join_users_{suffix}");
    let orders_col = format!("fireql_join_orders_{suffix}");

    create_test_doc(&db, &users_col, "u1", &json!({"name": "Alice"})).await?;
    create_test_doc(&db, &users_col, "u2", &json!({"name": "Bob"})).await?;

    create_test_doc(
        &db,
        &orders_col,
        "o1",
        &json!({"user_id": "u1", "amount": 100}),
    )
    .await?;
    create_test_doc(
        &db,
        &orders_col,
        "o2",
        &json!({"user_id": "u1", "amount": 200}),
    )
    .await?;
    create_test_doc(
        &db,
        &orders_col,
        "o3",
        &json!({"user_id": "u2", "amount": 50}),
    )
    .await?;

    let sql =
        format!("SELECT * FROM {users_col} u INNER JOIN {orders_col} o ON u.__name__ = o.user_id");
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 3);
            for row in &rows {
                assert!(row.data.contains_key("u.name"));
                assert!(row.data.contains_key("o.amount"));
            }
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_execute_stream_streams_select_rows() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let users_col = format!("fireql_stream_users_{suffix}");

    create_test_doc(&db, &users_col, "u1", &json!({"name": "Alice", "age": 30})).await?;
    create_test_doc(&db, &users_col, "u2", &json!({"name": "Bob", "age": 40})).await?;

    // A plain SELECT streams rows as documents arrive.
    let select_sql = format!("SELECT * FROM {users_col} WHERE age >= 0 ORDER BY age");
    match fireql.execute_stream(&select_sql).await? {
        FireqlStream::Rows(rows) => {
            let docs: Vec<DocOutput> = rows.try_collect().await?;
            assert_eq!(docs.len(), 2);
            assert_eq!(docs[0].id, "u1");
            assert_eq!(docs[1].id, "u2");
            assert_eq!(
                docs[0].data.get("name"),
                Some(&FireqlValue::String("Alice".to_string()))
            );
        }
        _ => panic!("expected rows stream"),
    }

    // Aggregation results are returned as a single completed output.
    let agg_sql = format!("SELECT COUNT(*) AS total FROM {users_col}");
    match fireql.execute_stream(&agg_sql).await? {
        FireqlStream::Completed(FireqlOutput::Aggregation(map)) => {
            assert_eq!(map.get("total"), Some(&FireqlValue::Integer(2)));
        }
        _ => panic!("expected completed aggregation"),
    }

    // UPDATE statements are returned as a single completed output.
    let update_sql = format!("UPDATE {users_col} SET age = 99 WHERE age >= 0");
    match fireql.execute_stream(&update_sql).await? {
        FireqlStream::Completed(FireqlOutput::Affected { affected }) => {
            assert_eq!(affected, 2);
        }
        _ => panic!("expected completed affected"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_execute_stream_completes_joined_select() -> Result<(), Box<dyn std::error::Error>>
{
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let users_col = format!("fireql_stream_join_users_{suffix}");
    let orders_col = format!("fireql_stream_join_orders_{suffix}");

    create_test_doc(&db, &users_col, "u1", &json!({"name": "Alice"})).await?;
    create_test_doc(
        &db,
        &orders_col,
        "o1",
        &json!({"user_id": "u1", "amount": 100}),
    )
    .await?;

    let sql =
        format!("SELECT * FROM {users_col} u INNER JOIN {orders_col} o ON u.__name__ = o.user_id");
    match fireql.execute_stream(&sql).await? {
        FireqlStream::Completed(FireqlOutput::Rows(rows)) => {
            assert_eq!(rows.len(), 1);
            assert!(rows[0].data.contains_key("u.name"));
            assert!(rows[0].data.contains_key("o.amount"));
        }
        _ => panic!("expected completed rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_left_join() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let users_col = format!("fireql_ljoin_users_{suffix}");
    let orders_col = format!("fireql_ljoin_orders_{suffix}");

    create_test_doc(&db, &users_col, "u1", &json!({"name": "Alice"})).await?;
    create_test_doc(&db, &users_col, "u2", &json!({"name": "Bob"})).await?;
    create_test_doc(&db, &users_col, "u3", &json!({"name": "Charlie"})).await?;

    create_test_doc(
        &db,
        &orders_col,
        "o1",
        &json!({"user_id": "u1", "amount": 100}),
    )
    .await?;

    let sql =
        format!("SELECT * FROM {users_col} u LEFT JOIN {orders_col} o ON u.__name__ = o.user_id");
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 3);
            let matched: Vec<_> = rows
                .iter()
                .filter(|r| r.data.contains_key("o.amount"))
                .collect();
            assert_eq!(matched.len(), 1);
            assert_eq!(matched[0].id, "u1");
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_chained_join_on_prior_right_document_name(
) -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let users_col = format!("fireql_cjoin_users_{suffix}");
    let orders_col = format!("fireql_cjoin_orders_{suffix}");
    let items_col = format!("fireql_cjoin_items_{suffix}");

    create_test_doc(&db, &users_col, "u1", &json!({"name": "Alice"})).await?;
    create_test_doc(&db, &users_col, "u2", &json!({"name": "Bob"})).await?;

    create_test_doc(
        &db,
        &orders_col,
        "o1",
        &json!({"user_id": "u1", "amount": 100}),
    )
    .await?;
    create_test_doc(
        &db,
        &orders_col,
        "o2",
        &json!({"user_id": "u2", "amount": 50}),
    )
    .await?;

    create_test_doc(
        &db,
        &items_col,
        "i1",
        &json!({"order_id": "o1", "item_name": "Keyboard"}),
    )
    .await?;
    create_test_doc(
        &db,
        &items_col,
        "i2",
        &json!({"order_id": "o1", "item_name": "Mouse"}),
    )
    .await?;
    create_test_doc(
        &db,
        &items_col,
        "i3",
        &json!({"order_id": "o2", "item_name": "Monitor"}),
    )
    .await?;

    // 2 つ目の JOIN の ON 句で先行する右側テーブル (o) の `__name__` を左辺に
    // 記述しても、結合キーとして解決できることを検証する。
    let sql = format!(
        "SELECT * FROM {users_col} u \
         INNER JOIN {orders_col} o ON u.__name__ = o.user_id \
         INNER JOIN {items_col} i ON i.order_id = o.__name__"
    );
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 3);
            let mut item_names: Vec<&str> = rows
                .iter()
                .map(|row| match row.data.get("i.item_name") {
                    Some(FireqlValue::String(s)) => s.as_str(),
                    other => panic!("expected i.item_name, got {other:?}"),
                })
                .collect();
            item_names.sort_unstable();
            assert_eq!(item_names, vec!["Keyboard", "Monitor", "Mouse"]);
            // 先行する右側テーブルのドキュメント ID も参照できる。
            for row in &rows {
                match (row.data.get("o.__name__"), row.data.get("i.order_id")) {
                    (Some(FireqlValue::String(name)), Some(FireqlValue::String(order_id))) => {
                        assert_eq!(name, order_id)
                    }
                    other => panic!("expected o.__name__ / i.order_id, got {other:?}"),
                }
            }
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_or_independent_in_filters() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_or_in_{}", unique_suffix());
    for (id, status, role) in [
        ("a1", "a", "z"),
        ("b1", "b", "z"),
        ("x1", "c", "x"),
        ("y1", "c", "y"),
        ("none", "c", "z"),
    ] {
        create_test_doc(
            &db,
            &collection,
            id,
            &json!({ "status": status, "role": role }),
        )
        .await?;
    }

    let sql = format!("SELECT * FROM {collection} WHERE status IN ('a','b') OR role IN ('x','y')");
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(mut rows) => {
            rows.sort_by(|a, b| a.id.cmp(&b.id));
            let ids: Vec<_> = rows.iter().map(|r| r.id.as_str()).collect();
            assert_eq!(ids, vec!["a1", "b1", "x1", "y1"]);
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_or_in_and_array_contains_any() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_or_mixed_{}", unique_suffix());
    for (id, status, tags) in [
        ("by_status", "a", json!(["python"])),
        ("by_tags", "z", json!(["sql", "cli"])),
        ("neither", "z", json!(["python"])),
    ] {
        create_test_doc(
            &db,
            &collection,
            id,
            &json!({ "status": status, "tags": tags }),
        )
        .await?;
    }

    // Independent disjunction filters across OR branches: IN on one side,
    // array-contains-any on the other.
    let sql = format!(
        "SELECT * FROM {collection} WHERE status IN ('a','b') OR array_contains_any(tags, ['sql','cli'])"
    );
    let output = fireql.execute(&sql).await?;
    match output {
        FireqlOutput::Rows(mut rows) => {
            rows.sort_by(|a, b| a.id.cmp(&b.id));
            let ids: Vec<_> = rows.iter().map(|r| r.id.as_str()).collect();
            assert_eq!(ids, vec!["by_status", "by_tags"]);
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_or_with_not_in_is_rejected_by_firestore() -> Result<(), Box<dyn std::error::Error>>
{
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_or_notin_{}", unique_suffix());
    create_test_doc(
        &db,
        &collection,
        "d1",
        &json!({ "status": "a", "role": "x" }),
    )
    .await?;

    // fireql accepts NOT IN in an OR branch (per-branch validation), but
    // Firestore rejects NOT_IN combined with OR. Surface that as a Firestore error.
    let sql =
        format!("SELECT * FROM {collection} WHERE status NOT IN ('gone') OR role IN ('x','y')");
    let err = fireql
        .execute(&sql)
        .await
        .expect_err("NOT IN combined with OR must fail at Firestore");
    assert!(
        matches!(err, FireqlError::Firestore(_)),
        "expected Firestore error, got {err}"
    );
    assert!(
        err.to_string().contains("NOT_IN") || err.to_string().to_ascii_lowercase().contains("or"),
        "unexpected error message: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn emulator_rejects_multiple_in_within_same_branch() -> Result<(), Box<dyn std::error::Error>>
{
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_or_reject_{}", unique_suffix());
    // Two IN filters in one conjunction exceed Firestore's per-branch limit
    // and must fail in fireql validation before the emulator is contacted.
    let sql = format!("SELECT * FROM {collection} WHERE status IN ('a','b') AND role IN ('x','y')");
    let err = fireql
        .execute(&sql)
        .await
        .expect_err("multiple IN in one branch must be rejected");
    assert!(
        matches!(err, FireqlError::InvalidQuery(_)),
        "expected InvalidQuery, got {err}"
    );

    Ok(())
}

#[tokio::test]
async fn emulator_aggregation_count_sum_avg() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let collection = format!("fireql_agg_{}", unique_suffix());
    create_test_doc(
        &db,
        &collection,
        "d1",
        &json!({"score": 10, "active": true}),
    )
    .await?;
    create_test_doc(
        &db,
        &collection,
        "d2",
        &json!({"score": 20, "active": true}),
    )
    .await?;
    create_test_doc(
        &db,
        &collection,
        "d3",
        &json!({"score": 30, "active": false}),
    )
    .await?;

    let count_sql = format!("SELECT COUNT(*) AS total FROM {collection}");
    let output = fireql.execute(&count_sql).await?;
    match output {
        FireqlOutput::Aggregation(data) => {
            assert_eq!(data.get("total"), Some(&FireqlValue::Integer(3)));
        }
        other => panic!("expected aggregation output, got {other:?}"),
    }

    let filter_agg_sql = format!(
        "SELECT COUNT(*) AS cnt, SUM(score) AS total_score, AVG(score) AS avg_score FROM {collection} WHERE active = true"
    );
    let output = fireql.execute(&filter_agg_sql).await?;
    match output {
        FireqlOutput::Aggregation(data) => {
            assert_eq!(data.get("cnt"), Some(&FireqlValue::Integer(2)));
            assert_eq!(data.get("total_score"), Some(&FireqlValue::Integer(30)));
            assert_eq!(data.get("avg_score"), Some(&FireqlValue::Double(15.0)));
        }
        other => panic!("expected aggregation output, got {other:?}"),
    }

    Ok(())
}
