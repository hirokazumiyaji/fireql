mod error;
mod executor;
pub mod format;
pub(crate) mod joiner;
mod output;
mod planner;
mod sql;
mod value;

pub use error::{FireqlError, Result};
pub use format::{write_csv_rows, write_csv_rows_stream, write_json_rows_stream, Format};
pub use output::{DocOutput, FireqlOutput, FireqlStream};
pub use sql::parse_collection_relative_path;
pub use value::FireqlValue;

use firestore::{FirestoreDb, FirestoreDbOptions};
use gcloud_sdk::google_cloud_auth::credentials::{
    external_account, impersonated, service_account, user_account, CacheableResource, Credentials,
    CredentialsProvider, EntityTag,
};
use gcloud_sdk::google_cloud_auth::errors::CredentialsError;
use http::header::{HeaderValue, AUTHORIZATION};
use http::{Extensions, HeaderMap};
use std::path::PathBuf;

pub(crate) enum CredentialSource {
    FilePath(PathBuf),
    Json(String),
    AccessToken(String),
}

impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialSource::FilePath(_) => f.write_str("CredentialSource::FilePath(<redacted>)"),
            CredentialSource::Json(_) => f.write_str("CredentialSource::Json(<redacted>)"),
            CredentialSource::AccessToken(_) => {
                f.write_str("CredentialSource::AccessToken(<redacted>)")
            }
        }
    }
}

struct StaticBearerCredentials {
    authorization: HeaderValue,
    entity_tag: EntityTag,
}

impl StaticBearerCredentials {
    fn new(token: &str) -> Result<Self> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|err| FireqlError::InvalidConfig(err.to_string()))?;
        authorization.set_sensitive(true);
        Ok(Self {
            authorization,
            entity_tag: EntityTag::new(),
        })
    }
}

impl std::fmt::Debug for StaticBearerCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StaticBearerCredentials(<redacted>)")
    }
}

impl CredentialsProvider for StaticBearerCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError> {
        if extensions.get::<EntityTag>() == Some(&self.entity_tag) {
            return Ok(CacheableResource::NotModified);
        }
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, self.authorization.clone());
        Ok(CacheableResource::New {
            entity_tag: self.entity_tag.clone(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

fn credentials_from_json(json: &str) -> Result<Credentials> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let cred_type = value.get("type").and_then(|v| v.as_str());
    let credentials = match cred_type {
        Some("authorized_user") => user_account::Builder::new(value).build(),
        Some("impersonated_service_account") => impersonated::Builder::new(value).build(),
        Some("external_account") => external_account::Builder::new(value).build(),
        Some("service_account") | None => service_account::Builder::new(value).build(),
        Some(other) => {
            return Err(FireqlError::InvalidConfig(format!(
                "unsupported credentials type: {other}"
            )));
        }
    };
    credentials.map_err(|err| FireqlError::InvalidConfig(err.to_string()))
}

pub struct FireqlConfig {
    project_id: String,
    database_id: Option<String>,
    credentials_source: Option<CredentialSource>,
    emulator_host: Option<String>,
    batch_parallelism: usize,
}

impl FireqlConfig {
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            database_id: None,
            credentials_source: None,
            emulator_host: None,
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

    /// Uses a pre-issued OAuth access token as the Bearer credential for Firestore requests.
    ///
    /// Token refresh and expiry handling are the caller's responsibility; expired tokens
    /// surface as Firestore authentication errors.
    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.credentials_source = Some(CredentialSource::AccessToken(token.into()));
        self
    }

    /// Targets a Firestore emulator listening on `host` (`host:port`, or a full
    /// URL) instead of the production API.
    ///
    /// Callers do not need to set `FIRESTORE_EMULATOR_HOST`, so the endpoint can
    /// change per connection without mutating the process environment. Unless
    /// credentials are configured explicitly, a stub token is used because the
    /// emulator does not authenticate requests.
    pub fn with_emulator_host(mut self, host: impl Into<String>) -> Self {
        self.emulator_host = Some(host.into());
        self
    }

    pub fn with_batch_parallelism(mut self, parallelism: usize) -> Self {
        self.batch_parallelism = parallelism.max(1);
        self
    }
}

fn emulator_api_url(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
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
        if let Some(host) = config.emulator_host.as_deref() {
            options = options.with_firebase_api_url(emulator_api_url(host));
        }

        let db = match config.credentials_source {
            Some(CredentialSource::FilePath(path)) => {
                FirestoreDb::with_options_service_account_key_file(options, path).await?
            }
            Some(CredentialSource::Json(json)) => {
                FirestoreDb::with_options_auth(options, credentials_from_json(&json)?).await?
            }
            Some(CredentialSource::AccessToken(token)) => {
                FirestoreDb::with_options_auth(
                    options,
                    Credentials::from(StaticBearerCredentials::new(&token)?),
                )
                .await?
            }
            None if config.emulator_host.is_some() => {
                FirestoreDb::with_options_auth(
                    options,
                    Credentials::from(StaticBearerCredentials::new("owner")?),
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

    /// Executes `sql` and streams SELECT rows as documents arrive (#55).
    ///
    /// Plain SELECT statements return [`FireqlStream::Rows`], a stream of
    /// [`DocOutput`]s that avoids buffering the whole result set in memory.
    /// All other statement kinds (and SELECTs with JOIN or aggregation) return
    /// [`FireqlStream::Completed`] with the same output as [`Fireql::execute`].
    pub async fn execute_stream(&self, sql: &str) -> Result<FireqlStream<'_>> {
        let stmt = sql::parse_sql(sql)?;
        executor::execute_stream(&self.db, stmt, self.batch_parallelism).await
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
    fn with_access_token_sets_access_token_source() {
        let config = FireqlConfig::new("my-project").with_access_token("my-access-token");
        match config.credentials_source {
            Some(CredentialSource::AccessToken(token)) => {
                assert_eq!(token, "my-access-token");
            }
            other => panic!("expected AccessToken, got {other:?}"),
        }
    }

    #[test]
    fn access_token_source_debug_is_redacted() {
        let source = CredentialSource::AccessToken("secret-token".to_string());
        assert_eq!(
            format!("{source:?}"),
            "CredentialSource::AccessToken(<redacted>)"
        );
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
    fn default_config_has_no_emulator_host() {
        let config = FireqlConfig::new("my-project");
        assert!(config.emulator_host.is_none());
    }

    #[test]
    fn with_emulator_host_sets_host() {
        let config = FireqlConfig::new("my-project").with_emulator_host("127.0.0.1:8080");
        assert_eq!(config.emulator_host.as_deref(), Some("127.0.0.1:8080"));
    }

    #[test]
    fn emulator_api_url_defaults_to_plain_http() {
        assert_eq!(emulator_api_url("127.0.0.1:8080"), "http://127.0.0.1:8080");
    }

    #[test]
    fn emulator_api_url_keeps_an_explicit_scheme() {
        assert_eq!(
            emulator_api_url("https://firestore.example:443"),
            "https://firestore.example:443"
        );
        assert_eq!(
            emulator_api_url("http://localhost:8080"),
            "http://localhost:8080"
        );
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

    #[test]
    fn credentials_from_json_rejects_unknown_type() {
        let err = credentials_from_json(r#"{"type":"not-a-credential"}"#).unwrap_err();
        assert!(
            matches!(err, FireqlError::InvalidConfig(message) if message.contains("not-a-credential"))
        );
    }

    #[test]
    fn static_bearer_credentials_reject_invalid_tokens() {
        let err = StaticBearerCredentials::new("token\nwith\nnewlines").unwrap_err();
        assert!(matches!(err, FireqlError::InvalidConfig(_)));
    }
}
