mod support;

use fireql::{FireqlOutput, FireqlValue};
use firestore::FirestoreCreateSupport;
use serde_json::json;
use support::{open_db, open_fireql, project_id, should_skip, unique_suffix};

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
    let _: serde_json::Value = db
        .create_obj(&collection, Some(doc_id), &data, None)
        .await?;

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
    let _: serde_json::Value = db
        .create_obj(
            &parent_collection,
            Some(parent_id),
            &json!({"name": "p"}),
            None,
        )
        .await?;

    let parent_path = format!(
        "{}/{}/{}",
        db.get_documents_path(),
        parent_collection,
        parent_id
    );
    let post_title = format!("hello-{}", unique_suffix());

    let _: serde_json::Value = db
        .create_obj_at(
            &parent_path,
            "posts",
            Some("post1"),
            &json!({"title": &post_title, "likes": 1}),
            None,
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

    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u1"), &json!({"name": "JoinUser"}), None)
        .await?;

    let _: serde_json::Value = db
        .create_obj(
            &parents_col,
            Some("a"),
            &json!({"label": "A"}),
            None,
        )
        .await?;
    let _: serde_json::Value = db
        .create_obj(
            &parents_col,
            Some("b"),
            &json!({"label": "B"}),
            None,
        )
        .await?;

    let parent_a = format!("{}/{}/{}", db.get_documents_path(), parents_col, "a");

    let _: serde_json::Value = db
        .create_obj_at(
            &parent_a,
            "posts",
            Some("p1"),
            &json!({"title": &title_a, "n": 1, "user_id": "u1"}),
            None,
        )
        .await?;
    let parent_b = format!("{}/{}/{}", db.get_documents_path(), parents_col, "b");
    let _: serde_json::Value = db
        .create_obj_at(
            &parent_b,
            "posts",
            Some("p2"),
            &json!({"title": &title_b, "n": 2, "user_id": "u1"}),
            None,
        )
        .await?;

    let rel_a_posts = format!("{parents_col}/a/posts");
    let scoped_sql = format!(
        "SELECT title, n FROM collection('{rel_a_posts}') WHERE title = '{title_a}'"
    );
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

    let update_sql = format!(
        "UPDATE collection('{rel_a_posts}') SET n = 99 WHERE title = '{title_a}'"
    );
    let output = fireql.execute(&update_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        _ => panic!("expected affected"),
    }

    let check_b_sql = format!(
        "SELECT n FROM collection('{parents_col}/b/posts') WHERE title = '{title_b}'"
    );
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

    let delete_sql = format!(
        "DELETE FROM collection('{rel_a_posts}') WHERE title = '{title_a}'"
    );
    let output = fireql.execute(&delete_sql).await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        _ => panic!("expected affected"),
    }

    let verify_sql = format!(
        "SELECT * FROM collection('{rel_a_posts}') WHERE title = '{title_a}'"
    );
    let output = fireql.execute(&verify_sql).await?;
    match output {
        FireqlOutput::Rows(rows) => assert_eq!(rows.len(), 0),
        _ => panic!("expected rows"),
    }

    let verify_b_sql = format!(
        "SELECT title FROM collection('{parents_col}/b/posts') WHERE title = '{title_b}'"
    );
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
async fn emulator_inner_join_subcollection_right_document_name() -> Result<(), Box<dyn std::error::Error>>
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

    let parents_col = format!("fireql_namejoin_parents_{}", unique_suffix());
    let users_col = format!("fireql_namejoin_users_{}", unique_suffix());
    let post_title = format!("namejoin-{}", unique_suffix());

    let _: serde_json::Value = db
        .create_obj(&parents_col, Some("a"), &json!({}), None)
        .await?;

    let parent_a = format!("{}/{}/{}", db.get_documents_path(), parents_col, "a");
    let _: serde_json::Value = db
        .create_obj_at(
            &parent_a,
            "posts",
            Some("doc_for_name"),
            &json!({"title": &post_title}),
            None,
        )
        .await?;

    let _: serde_json::Value = db
        .create_obj(
            &users_col,
            Some("u1"),
            &json!({"name": "NameJoinUser", "post_ref": "doc_for_name"}),
            None,
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

    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u1"), &json!({"name": "Alice"}), None)
        .await?;
    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u2"), &json!({"name": "Bob"}), None)
        .await?;

    let _: serde_json::Value = db
        .create_obj(
            &orders_col,
            Some("o1"),
            &json!({"user_id": "u1", "amount": 100}),
            None,
        )
        .await?;
    let _: serde_json::Value = db
        .create_obj(
            &orders_col,
            Some("o2"),
            &json!({"user_id": "u1", "amount": 200}),
            None,
        )
        .await?;
    let _: serde_json::Value = db
        .create_obj(
            &orders_col,
            Some("o3"),
            &json!({"user_id": "u2", "amount": 50}),
            None,
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

    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u1"), &json!({"name": "Alice"}), None)
        .await?;
    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u2"), &json!({"name": "Bob"}), None)
        .await?;
    let _: serde_json::Value = db
        .create_obj(&users_col, Some("u3"), &json!({"name": "Charlie"}), None)
        .await?;

    let _: serde_json::Value = db
        .create_obj(
            &orders_col,
            Some("o1"),
            &json!({"user_id": "u1", "amount": 100}),
            None,
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
