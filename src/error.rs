use thiserror::Error;

#[derive(Debug, Error)]
pub enum FireqlError {
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("Unsupported SQL: {0}")]
    Unsupported(String),
    #[error("UPDATE/DELETE requires a WHERE clause")]
    MissingWhere,
    #[error("Invalid document name: {0}")]
    InvalidDocName(String),
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("Partial failure after {affected} writes: {error}")]
    PartialFailure { affected: u64, error: String },
    #[error("Firestore error: {0}")]
    Firestore(#[from] firestore::errors::FirestoreError),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Format error: {0}")]
    Format(String),
}

pub type Result<T> = std::result::Result<T, FireqlError>;
