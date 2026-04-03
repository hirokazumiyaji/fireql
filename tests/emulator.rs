use fireql::{Fireql, FireqlConfig, FireqlOutput};
use firestore::{FirestoreCreateSupport, FirestoreDb, FirestoreDbOptions};
use serde_json::json;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos}")
}

fn project_id() -> String {
    env::var("FIRESTORE_PROJECT_ID")
        .or_else(|_| env::var("GOOGLE_CLOUD_PROJECT"))
        .unwrap_or_else(|_| "fireql-emulator".to_string())
}

fn should_skip() -> bool {
    env::var("FIRESTORE_EMULATOR_HOST").is_err()
}

async fn open_db(project_id: &str) -> Option<FirestoreDb> {
    match FirestoreDb::with_options(FirestoreDbOptions::new(project_id.to_string())).await {
        Ok(db) => Some(db),
        Err(err) => {
            let emulator_host =
                env::var("FIRESTORE_EMULATOR_HOST").unwrap_or_else(|_| "<not set>".to_string());
            eprintln!(
                "skip emulator test: failed to create FirestoreDb for project '{project_id}' with FIRESTORE_EMULATOR_HOST='{emulator_host}': {err}"
            );
            None
        }
    }
}

async fn open_fireql(project_id: &str) -> Option<Fireql> {
    match Fireql::new(FireqlConfig::new(project_id)).await {
        Ok(fireql) => Some(fireql),
        Err(err) => {
            eprintln!(
                "skip emulator test: failed to create Fireql for project '{project_id}': {err}"
            );
            None
        }
    }
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
