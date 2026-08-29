use clap::Parser;
use fireql::{
    write_csv_rows, write_csv_rows_stream, write_json_rows_stream, DocOutput, Fireql, FireqlConfig,
    FireqlError, FireqlOutput, FireqlStream, Format,
};
use futures::TryStreamExt;
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

    // SELECT rows stream to stdout as documents arrive (#55); CSV and JSON
    // flush row-by-row so the memory footprint stays constant. Table output
    // needs the full field union, so it buffers once before rendering.
    let mut stdout = io::stdout().lock();
    match fireql.execute_stream(&sql).await? {
        FireqlStream::Rows(rows) => match cli.format {
            Format::Csv => write_csv_rows_stream(rows, &mut stdout).await?,
            Format::Json => {
                write_json_rows_stream(rows, &mut stdout, cli.pretty).await?;
                stdout.write_all(b"\n")?;
            }
            Format::Table => {
                let rows: Vec<DocOutput> = rows.try_collect().await?;
                write_completed_output(
                    Format::Table,
                    FireqlOutput::Rows(rows),
                    cli.pretty,
                    &mut stdout,
                )?;
            }
        },
        FireqlStream::Completed(output) => {
            write_completed_output(cli.format, output, cli.pretty, &mut stdout)?;
        }
    }
    stdout.flush()?;
    Ok(())
}

/// Writes an already materialized output to `out`.
///
/// Completed `Rows` results (e.g. joined SELECTs) keep the CSV row-writer path
/// (#28): `format_csv` already terminates the last record with a newline, so
/// `writeln!` would append a spurious blank line, and this route avoids
/// materializing a second full CSV string. Every other output (and CSV's
/// non-row outputs) is rendered once and terminated with a single newline.
fn write_completed_output<W: std::io::Write>(
    format: Format,
    output: FireqlOutput,
    pretty: bool,
    out: &mut W,
) -> Result<(), FireqlError> {
    match output {
        FireqlOutput::Rows(rows) if format == Format::Csv => write_csv_rows(rows, out),
        output => {
            let formatted = format.format(&output, pretty)?;
            writeln!(out, "{formatted}")?;
            Ok(())
        }
    }
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
                "project_id is required (use --project-id or set FIRESTORE_PROJECT_ID / GOOGLE_CLOUD_PROJECT)"
                    .to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use fireql::{FireqlValue, Format};
    use std::collections::HashMap;

    fn doc(id: &str, name: &str) -> DocOutput {
        DocOutput {
            id: id.to_string(),
            path: format!("c/{id}"),
            data: HashMap::from([("name".to_string(), FireqlValue::String(name.to_string()))]),
        }
    }

    #[test]
    fn completed_csv_rows_do_not_emit_trailing_blank_line() {
        let mut buf: Vec<u8> = Vec::new();
        write_completed_output(
            Format::Csv,
            FireqlOutput::Rows(vec![doc("u1", "Alice"), doc("u2", "Bob")]),
            false,
            &mut buf,
        )
        .unwrap();

        let out = String::from_utf8(buf).unwrap();
        assert!(out.ends_with('\n'), "got: {out:?}");
        assert!(!out.ends_with("\n\n"), "spurious blank line: {out:?}");
        assert_eq!(out.lines().count(), 3, "header + 2 rows: {out:?}");
    }

    #[test]
    fn completed_json_rows_end_with_single_newline() {
        let mut buf: Vec<u8> = Vec::new();
        write_completed_output(
            Format::Json,
            FireqlOutput::Rows(vec![doc("u1", "Alice")]),
            false,
            &mut buf,
        )
        .unwrap();

        let out = String::from_utf8(buf).unwrap();
        assert!(out.ends_with("]\n"), "got: {out:?}");
        assert!(!out.ends_with("\n\n"), "got: {out:?}");
    }

    #[test]
    fn completed_aggregation_ends_with_single_newline() {
        let mut buf: Vec<u8> = Vec::new();
        write_completed_output(
            Format::Json,
            FireqlOutput::Aggregation(HashMap::from([(
                "total".to_string(),
                FireqlValue::Integer(2),
            )])),
            false,
            &mut buf,
        )
        .unwrap();

        let out = String::from_utf8(buf).unwrap();
        assert!(out.ends_with("}\n"), "got: {out:?}");
    }

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
