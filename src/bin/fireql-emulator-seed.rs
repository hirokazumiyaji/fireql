use clap::Parser;
use firestore::{
    FirestoreCreateSupport, FirestoreDb, FirestoreDbOptions, FirestoreGetByIdSupport,
    FirestoreUpdateSupport,
};
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(
    name = "fireql-emulator-seed",
    about = "Seed reusable e2e data into the Firestore emulator"
)]
struct Cli {
    #[arg(long)]
    project_id: Option<String>,

    #[arg(long)]
    database_id: Option<String>,

    #[arg(long)]
    fixture: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    collections: Vec<CollectionSeed>,
}

#[derive(Debug, Deserialize)]
struct CollectionSeed {
    path: String,
    documents: Vec<DocumentSeed>,
}

#[derive(Debug, Deserialize)]
struct DocumentSeed {
    id: String,
    data: Value,
}

struct CollectionTarget {
    parent: Option<String>,
    collection_id: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("fireql-emulator-seed error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let emulator_host = env::var("FIRESTORE_EMULATOR_HOST").map_err(|_| {
        "FIRESTORE_EMULATOR_HOST is required; this command only seeds the emulator".to_string()
    })?;

    let project_id = cli
        .project_id
        .or_else(|| env::var("FIRESTORE_PROJECT_ID").ok())
        .or_else(|| env::var("GOOGLE_CLOUD_PROJECT").ok())
        .unwrap_or_else(|| "fireql-emulator".to_string());
    let fixture_path = cli.fixture.unwrap_or_else(default_fixture_path);
    let fixture = read_fixture(&fixture_path)?;

    let mut options = FirestoreDbOptions::new(project_id.clone());
    if let Some(database_id) = cli.database_id {
        options = options.with_database_id(database_id);
    }
    let db = FirestoreDb::with_options(options).await?;

    let mut total_documents = 0;
    for collection in &fixture.collections {
        let target = parse_collection_target(&db, &collection.path)?;
        for document in &collection.documents {
            upsert_document(&db, &target, document).await?;
        }
        total_documents += collection.documents.len();
        println!(
            "seeded {} documents into {}",
            collection.documents.len(),
            collection.path
        );
    }

    println!(
        "seed complete: project_id={project_id} emulator_host={emulator_host} collections={} documents={} fixture={}",
        fixture.collections.len(),
        total_documents,
        fixture_path.display()
    );

    Ok(())
}

fn default_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/emulator-e2e.json")
}

fn read_fixture(path: &Path) -> Result<Fixture, Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&contents)?)
}

fn parse_collection_target(
    db: &FirestoreDb,
    path: &str,
) -> Result<CollectionTarget, Box<dyn Error>> {
    if path.is_empty() || path.starts_with('/') || path.ends_with('/') || path.contains("//") {
        return Err(format!("invalid collection path: {path}").into());
    }

    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() % 2 == 0 {
        return Err(format!("collection path must end with a collection id: {path}").into());
    }

    let collection_id = segments.last().unwrap().to_string();
    let parent = if segments.len() == 1 {
        None
    } else {
        Some(format!(
            "{}/{}",
            db.get_documents_path(),
            segments[..segments.len() - 1].join("/")
        ))
    };

    Ok(CollectionTarget {
        parent,
        collection_id,
    })
}

async fn upsert_document(
    db: &FirestoreDb,
    target: &CollectionTarget,
    document: &DocumentSeed,
) -> Result<(), Box<dyn Error>> {
    match &target.parent {
        Some(parent) => {
            if db
                .get_obj_at_if_exists::<Value, _>(parent, &target.collection_id, &document.id, None)
                .await?
                .is_some()
            {
                let _: Value = db
                    .update_obj_at(
                        parent,
                        &target.collection_id,
                        &document.id,
                        &document.data,
                        None,
                        None,
                        None,
                    )
                    .await?;
            } else {
                let _: Value = db
                    .create_obj_at(
                        parent,
                        &target.collection_id,
                        Some(&document.id),
                        &document.data,
                        None,
                    )
                    .await?;
            }
        }
        None => {
            if db
                .get_obj_if_exists::<Value, _>(&target.collection_id, &document.id, None)
                .await?
                .is_some()
            {
                let _: Value = db
                    .update_obj(
                        &target.collection_id,
                        &document.id,
                        &document.data,
                        None,
                        None,
                        None,
                    )
                    .await?;
            } else {
                let _: Value = db
                    .create_obj(
                        &target.collection_id,
                        Some(&document.id),
                        &document.data,
                        None,
                    )
                    .await?;
            }
        }
    }

    Ok(())
}
