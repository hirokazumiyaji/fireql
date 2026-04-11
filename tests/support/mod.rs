#![allow(dead_code)]

use fireql::{Fireql, FireqlConfig};
use firestore::{FirestoreDb, FirestoreDbOptions};
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos}")
}

pub fn project_id() -> String {
    env::var("FIRESTORE_PROJECT_ID")
        .or_else(|_| env::var("GOOGLE_CLOUD_PROJECT"))
        .unwrap_or_else(|_| "fireql-emulator".to_string())
}

pub fn should_skip() -> bool {
    env::var("FIRESTORE_EMULATOR_HOST").is_err()
}

pub async fn open_db(project_id: &str) -> Option<FirestoreDb> {
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

pub async fn open_fireql(project_id: &str) -> Option<Fireql> {
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
