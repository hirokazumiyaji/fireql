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

    let json = if cli.pretty {
        serde_json::to_string_pretty(&output)?
    } else {
        serde_json::to_string(&output)?
    };

    println!("{json}");
    Ok(())
}
