use clap::Parser;
use fireql::{write_csv_rows, Fireql, FireqlConfig, FireqlError, FireqlOutput, Format};
use std::env;
use std::io::{self, Read, Write};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "fireql", version, about = "SQL interface for Firestore")]
struct Cli {
    #[arg(long)]
    project_id: Option<String>,

    #[arg(long)]
    database_id: Option<String>,

    #[arg(long)]
    credentials: Option<PathBuf>,

    #[arg(long)]
    sql: Option<String>,

    #[arg(long)]
    pretty: bool,

    #[arg(long, value_enum, default_value_t = fireql::Format::Json)]
    format: fireql::Format,

    #[arg(long, default_value_t = 1)]
    batch_parallelism: usize,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("fireql error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), FireqlError> {
    let cli = Cli::parse();

    let project_id = resolve_project_id(cli.project_id, |key| env::var(key).ok())?;

    let mut config = FireqlConfig::new(project_id);
    if let Some(database_id) = cli.database_id {
        config = config.with_database_id(database_id);
    }
    if let Some(credentials) = cli.credentials {
        config = config.with_credentials_path(credentials);
    }
    config = config.with_batch_parallelism(cli.batch_parallelism);

    let sql = match cli.sql {
        Some(sql) => sql,
        None => {
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer)?;
            buffer
        }
    };

    let fireql = Fireql::new(config).await?;
    let output = fireql.execute(&sql).await?;

    // CSV row output streams through the writer using the first row's fields
    // as the header, avoiding a second full-field-union pass (#28).
    match (cli.format, output) {
        (Format::Csv, FireqlOutput::Rows(rows)) => {
            let mut stdout = io::stdout().lock();
            write_csv_rows(rows, &mut stdout)?;
            stdout.flush()?;
        }
        (format, output) => {
            let formatted = format.format(&output, cli.pretty)?;
            println!("{formatted}");
        }
    }
    Ok(())
}

/// Resolves the project id from the CLI option and the environment variable
/// fallback chain (`FIRESTORE_PROJECT_ID` / `GOOGLE_CLOUD_PROJECT` /
/// `GCLOUD_PROJECT`).
fn resolve_project_id(
    cli_project_id: Option<String>,
    mut lookup_env: impl FnMut(&str) -> Option<String>,
) -> Result<String, FireqlError> {
    cli_project_id
        .or_else(|| lookup_env("FIRESTORE_PROJECT_ID"))
        .or_else(|| lookup_env("GOOGLE_CLOUD_PROJECT"))
        .or_else(|| lookup_env("GCLOUD_PROJECT"))
        .ok_or_else(|| {
            FireqlError::InvalidConfig(
                "project_id is required (use --project-id or set GOOGLE_CLOUD_PROJECT / FIRESTORE_PROJECT_ID)"
                    .to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use fireql::Format;

    #[test]
    fn cli_default_format_is_json() {
        let cli = Cli::try_parse_from(["fireql", "--project-id", "p", "--sql", "SELECT * FROM c"])
            .unwrap();
        assert_eq!(cli.format, Format::Json);
    }

    #[test]
    fn cli_format_csv() {
        let cli = Cli::try_parse_from([
            "fireql",
            "--project-id",
            "p",
            "--format",
            "csv",
            "--sql",
            "SELECT * FROM c",
        ])
        .unwrap();
        assert_eq!(cli.format, Format::Csv);
    }

    #[test]
    fn cli_format_table() {
        let cli = Cli::try_parse_from([
            "fireql",
            "--project-id",
            "p",
            "--format",
            "table",
            "--sql",
            "SELECT * FROM c",
        ])
        .unwrap();
        assert_eq!(cli.format, Format::Table);
    }

    #[test]
    fn cli_invalid_format_rejected() {
        let result = Cli::try_parse_from([
            "fireql",
            "--project-id",
            "p",
            "--format",
            "xml",
            "--sql",
            "q",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_project_id_prefers_cli_option() {
        let resolved = resolve_project_id(Some("cli-project".to_string()), |_| {
            Some("env-project".to_string())
        })
        .unwrap();
        assert_eq!(resolved, "cli-project");
    }

    #[test]
    fn resolve_project_id_falls_back_to_firestore_project_id_first() {
        let mut keys = Vec::new();
        let resolved = resolve_project_id(None, |key| {
            keys.push(key.to_string());
            if key == "FIRESTORE_PROJECT_ID" {
                Some("emulator-project".to_string())
            } else {
                None
            }
        })
        .unwrap();
        assert_eq!(resolved, "emulator-project");
        assert_eq!(
            keys.first().map(String::as_str),
            Some("FIRESTORE_PROJECT_ID")
        );
    }

    #[test]
    fn resolve_project_id_falls_back_to_google_cloud_project() {
        let resolved = resolve_project_id(None, |key| {
            if key == "GOOGLE_CLOUD_PROJECT" {
                Some("gcp-project".to_string())
            } else {
                None
            }
        })
        .unwrap();
        assert_eq!(resolved, "gcp-project");
    }

    #[test]
    fn resolve_project_id_falls_back_to_gcloud_project() {
        let resolved = resolve_project_id(None, |key| {
            if key == "GCLOUD_PROJECT" {
                Some("gcloud-project".to_string())
            } else {
                None
            }
        })
        .unwrap();
        assert_eq!(resolved, "gcloud-project");
    }

    #[test]
    fn resolve_project_id_reports_error_when_unset() {
        let err = resolve_project_id(None, |_| None).unwrap_err();
        assert!(matches!(err, FireqlError::InvalidConfig(_)), "got: {err:?}");
        assert!(err.to_string().contains("FIRESTORE_PROJECT_ID"));
    }
}
