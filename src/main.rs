use clap::Parser;
use fireql::{Fireql, FireqlConfig, FireqlError};
use std::env;
use std::io::{self, Read};
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

    let project_id = cli
        .project_id
        .or_else(|| env::var("GOOGLE_CLOUD_PROJECT").ok())
        .or_else(|| env::var("GCLOUD_PROJECT").ok())
        .ok_or_else(|| {
            FireqlError::InvalidConfig(
                "project_id is required (use --project-id or set GOOGLE_CLOUD_PROJECT)".to_string(),
            )
        })?;

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

    let formatted = cli.format.format(&output, cli.pretty)?;
    println!("{formatted}");
    Ok(())
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
}
