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
use gcloud_sdk::TokenSourceType;
use std::path::PathBuf;

pub(crate) enum CredentialSource {
    FilePath(PathBuf),
    Json(String),
}

impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialSource::FilePath(_) => f.write_str("CredentialSource::FilePath(<redacted>)"),
            CredentialSource::Json(_) => f.write_str("CredentialSource::Json(<redacted>)"),
        }
    }
}

pub struct FireqlConfig {
    project_id: String,
    database_id: Option<String>,
    credentials_source: Option<CredentialSource>,
    batch_parallelism: usize,
}

impl FireqlConfig {
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            database_id: None,
            credentials_source: None,
            batch_parallelism: 1,
        }
    }

    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = Some(database_id.into());
        self
    }

    pub fn with_credentials_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.credentials_source = Some(CredentialSource::FilePath(path.into()));
        self
    }

    pub fn with_credentials_json(mut self, json: impl Into<String>) -> Self {
        self.credentials_source = Some(CredentialSource::Json(json.into()));
        self
    }

    pub fn with_authorized_user(
        self,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        refresh_token: impl Into<String>,
    ) -> Self {
        let json = serde_json::json!({
            "type": "authorized_user",
            "client_id": client_id.into(),
            "client_secret": client_secret.into(),
            "refresh_token": refresh_token.into(),
        });
        self.with_credentials_json(json.to_string())
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

        let db = match config.credentials_source {
            Some(CredentialSource::FilePath(path)) => {
                FirestoreDb::with_options_service_account_key_file(options, path).await?
            }
            Some(CredentialSource::Json(json)) => {
                FirestoreDb::with_options_token_source(
                    options,
                    gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
                    TokenSourceType::Json(json),
                )
                .await?
            }
            None => FirestoreDb::with_options(options).await?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_no_credentials() {
        let config = FireqlConfig::new("my-project");
        assert!(config.credentials_source.is_none());
    }

    #[test]
    fn with_credentials_path_sets_file_source() {
        let config = FireqlConfig::new("my-project").with_credentials_path("/tmp/creds.json");
        match config.credentials_source {
            Some(CredentialSource::FilePath(path)) => {
                assert_eq!(path.to_str().unwrap(), "/tmp/creds.json");
            }
            other => panic!("expected FilePath, got {other:?}"),
        }
    }

    #[test]
    fn with_credentials_json_sets_json_source() {
        let json = r#"{"type":"service_account","project_id":"test"}"#;
        let config = FireqlConfig::new("my-project").with_credentials_json(json);
        match config.credentials_source {
            Some(CredentialSource::Json(stored)) => {
                assert_eq!(stored, json);
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn with_authorized_user_builds_valid_json() {
        let config =
            FireqlConfig::new("my-project").with_authorized_user("cid", "csecret", "rtoken");
        match config.credentials_source {
            Some(CredentialSource::Json(json)) => {
                let v: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(v["type"], "authorized_user");
                assert_eq!(v["client_id"], "cid");
                assert_eq!(v["client_secret"], "csecret");
                assert_eq!(v["refresh_token"], "rtoken");
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn with_credentials_json_does_not_validate() {
        let config = FireqlConfig::new("my-project").with_credentials_json("not valid json");
        assert!(matches!(
            config.credentials_source,
            Some(CredentialSource::Json(_))
        ));
    }

    #[test]
    fn last_credential_builder_wins() {
        let config = FireqlConfig::new("my-project")
            .with_credentials_path("/tmp/creds.json")
            .with_credentials_json(r#"{"type":"service_account"}"#);
        assert!(matches!(
            config.credentials_source,
            Some(CredentialSource::Json(_))
        ));

        let config = FireqlConfig::new("my-project")
            .with_credentials_json(r#"{"type":"service_account"}"#)
            .with_credentials_path("/tmp/creds.json");
        assert!(matches!(
            config.credentials_source,
            Some(CredentialSource::FilePath(_))
        ));
    }
}
