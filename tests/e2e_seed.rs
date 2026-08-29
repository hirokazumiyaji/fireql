mod support;

use fireql::FireqlOutput;
use std::process::Command;
use support::{open_fireql, project_id, should_skip};

fn run_seed(project_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_fireql-emulator-seed"))
        .arg("--project-id")
        .arg(project_id)
        .output()?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!(
        "seed command failed with status {}:\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    )
    .into())
}

#[tokio::test]
async fn emulator_seed_provides_reusable_e2e_data() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    // 1st seed: inserts initial documents
    run_seed(&project_id)?;

    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    // Mutate data to verify that the second seed actually updates (upserts) existing documents
    let update_sql = "UPDATE e2e_users SET score = 999, active = false WHERE name = 'Alice'";
    match fireql.execute(update_sql).await? {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    // Verify mutation took effect
    let check_sql = "SELECT score, active FROM e2e_users WHERE name = 'Alice'";
    match fireql.execute(check_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].data.get("score"),
                Some(&fireql::FireqlValue::Integer(999))
            );
            assert_eq!(
                rows[0].data.get("active"),
                Some(&fireql::FireqlValue::Boolean(false))
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Mutate a subcollection document so the second seed must restore nested
    // documents to fixture data as well (#56).
    let mutate_sub_sql = "UPDATE collection('e2e_accounts/acme/posts') \
                          SET likes = 999, title = 'Mutated' WHERE likes = 12";
    match fireql.execute(mutate_sub_sql).await? {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    let check_sub_sql =
        "SELECT title, likes FROM collection('e2e_accounts/acme/posts') WHERE likes = 999";
    match fireql.execute(check_sub_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].data.get("title"),
                Some(&fireql::FireqlValue::String("Mutated".to_string()))
            );
            assert_eq!(
                rows[0].data.get("likes"),
                Some(&fireql::FireqlValue::Integer(999))
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // 2nd seed: should update existing mutated document back to fixture data
    run_seed(&project_id)?;

    let users_sql = "SELECT * FROM e2e_users WHERE active = true ORDER BY score DESC LIMIT 10";
    match fireql.execute(users_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].id, "alice");
            assert_eq!(
                rows[0].data.get("score"),
                Some(&fireql::FireqlValue::Integer(120))
            );
            assert_eq!(
                rows[0].data.get("active"),
                Some(&fireql::FireqlValue::Boolean(true))
            );
            assert_eq!(rows[1].id, "carol");
            assert_eq!(
                rows[1].data.get("score"),
                Some(&fireql::FireqlValue::Integer(90))
            );
        }
        _ => panic!("expected rows"),
    }

    // The second seed must restore subcollection documents directly to their
    // fixture values, not just the top-level collections (#56).
    let restore_sub_sql = "SELECT * FROM collection('e2e_accounts/acme/posts')";
    match fireql.execute(restore_sub_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            let release1 = rows
                .iter()
                .find(|row| row.id == "release-1")
                .expect("release-1");
            assert_eq!(
                release1.data.get("title"),
                Some(&fireql::FireqlValue::String("Launch Notes".to_string()))
            );
            assert_eq!(
                release1.data.get("likes"),
                Some(&fireql::FireqlValue::Integer(12))
            );
            assert_eq!(
                release1.data.get("category"),
                Some(&fireql::FireqlValue::String("release".to_string()))
            );
            assert_eq!(
                release1.data.get("published"),
                Some(&fireql::FireqlValue::Boolean(true))
            );
        }
        _ => panic!("expected rows"),
    }

    let join_sql = "SELECT * FROM e2e_users u LEFT JOIN e2e_orders o ON u.__name__ = o.user_id";
    match fireql.execute(join_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 4);
            let matched: Vec<_> = rows
                .iter()
                .filter(|row| row.data.contains_key("o.amount"))
                .collect();
            assert_eq!(matched.len(), 3);
            assert!(rows
                .iter()
                .any(|row| row.id == "bob" && !row.data.contains_key("o.amount")));
        }
        _ => panic!("expected rows"),
    }

    let collection_group_sql = "SELECT * FROM collection_group('posts') WHERE category = 'release' AND published = true ORDER BY likes DESC LIMIT 10";
    match fireql.execute(collection_group_sql).await? {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].id, "release-1");
            assert_eq!(rows[1].id, "release-2");
        }
        _ => panic!("expected rows"),
    }

    Ok(())
}
