mod error;
mod executor;
mod output;
mod planner;
mod sql;
mod value;

pub use error::{FireqlError, Result};
pub use output::{DocOutput, FireqlOutput};
pub use value::FireqlValue;

use firestore::{FirestoreDb, FirestoreDbOptions};
use std::path::PathBuf;

pub struct FireqlConfig {
    project_id: String,
    database_id: Option<String>,
    credentials_path: Option<PathBuf>,
    batch_parallelism: usize,
}

impl FireqlConfig {
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            database_id: None,
            credentials_path: None,
            batch_parallelism: 1,
        }
    }

    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = Some(database_id.into());
        self
    }

    pub fn with_credentials_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.credentials_path = Some(path.into());
        self
    }

    pub fn with_batch_parallelism(mut self, parallelism: usize) -> Self {
        self.batch_parallelism = parallelism.max(1);
        self
    }
}

pub struct Fireql {
    db: FirestoreDb,
    batch_parallelism: usize,
}

impl Fireql {
    pub async fn new(config: FireqlConfig) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut options = FirestoreDbOptions::new(config.project_id);
        if let Some(database_id) = config.database_id {
            options = options.with_database_id(database_id);
        }

        let db = if let Some(path) = config.credentials_path {
            FirestoreDb::with_options_service_account_key_file(options, path).await?
        } else {
            FirestoreDb::with_options(options).await?
        };

        Ok(Self {
            db,
            batch_parallelism: config.batch_parallelism,
        })
    }

    pub async fn execute(&self, sql: &str) -> Result<FireqlOutput> {
        let stmt = sql::parse_sql(sql)?;
        executor::execute(&self.db, stmt, self.batch_parallelism).await
    }
}
