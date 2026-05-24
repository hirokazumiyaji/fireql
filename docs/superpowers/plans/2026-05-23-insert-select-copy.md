# INSERT SELECT Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `INSERT INTO ... SELECT ...` document copy support for auto-ID copies and optional `__name__` ID preservation.

**Architecture:** Extend the SQL AST with an insert-select statement, reuse the existing SELECT parser for the source query, and execute copies through the existing Firestore batch writer. Keep unsupported forms explicit so `VALUES`, aggregation copy, joins, and collection group copy do not create accidental behavior.

**Tech Stack:** Rust 2021, `sqlparser` 0.61, `firestore` 0.48, `tokio`, existing emulator integration tests.

---

### Task 1: Parser Support

**Files:**
- Modify: `src/sql.rs`

- [ ] **Step 1: Write failing parser tests**

Add tests under `mod tests` in `src/sql.rs`:

```rust
#[test]
fn parse_insert_select_auto_id_copy() {
    let stmt =
        parse_sql("INSERT INTO archived_users SELECT * FROM users WHERE disabled = true").unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "archived_users");
            assert!(insert.columns.is_none());
            assert_eq!(insert.source.collection.collection_id, "users");
            assert!(matches!(insert.source.projection, SelectProjection::Fields(Projection::All)));
            assert!(insert.source.filter.is_some());
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_subcollections() {
    let stmt = parse_sql(
        "INSERT INTO collection('users/u1/archive') \
         SELECT * FROM collection('users/u1/posts') WHERE published = false",
    )
    .unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(insert.collection.collection_id, "archive");
            assert_eq!(insert.collection.parent_path.as_deref(), Some("users/u1"));
            assert_eq!(insert.source.collection.collection_id, "posts");
            assert_eq!(insert.source.collection.parent_path.as_deref(), Some("users/u1"));
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn parse_insert_select_with_id_preservation_columns() {
    let stmt = parse_sql(
        "INSERT INTO archived_users (__name__, name, age) \
         SELECT __name__, name, age FROM users WHERE disabled = true",
    )
    .unwrap();

    match stmt {
        StatementAst::InsertSelect(insert) => {
            assert_eq!(
                insert.columns.as_deref(),
                Some(["__name__", "name", "age"].as_slice())
            );
            assert!(matches!(
                insert.source.projection,
                SelectProjection::Fields(Projection::Fields(_))
            ));
        }
        other => panic!("expected insert select, got {other:?}"),
    }
}

#[test]
fn insert_select_rejects_aggregation() {
    let err = parse_sql("INSERT INTO archived_users SELECT COUNT(*) FROM users").unwrap_err();
    assert!(err.to_string().contains("Aggregation is not supported"));
}

#[test]
fn insert_select_rejects_collection_group_source() {
    let err =
        parse_sql("INSERT INTO archived_users SELECT * FROM collection_group('users')").unwrap_err();
    assert!(err.to_string().contains("collection_group"));
}
```

- [ ] **Step 2: Run parser tests and verify RED**

Run: `cargo test sql::tests::parse_insert_select_auto_id_copy sql::tests::parse_insert_select_subcollections sql::tests::parse_insert_select_with_id_preservation_columns sql::tests::insert_select_rejects_aggregation sql::tests::insert_select_rejects_collection_group_source`

Expected: FAIL because `StatementAst::InsertSelect` and `InsertSelectStatement` do not exist yet.

- [ ] **Step 3: Implement parser support**

Add `InsertSelect(InsertSelectStatement)` to `StatementAst`, add:

```rust
#[derive(Debug, Clone)]
pub struct InsertSelectStatement {
    pub collection: CollectionSpec,
    pub columns: Option<Vec<String>>,
    pub source: SelectStatement,
}
```

Parse `Statement::Insert(insert)` by rejecting unsupported insert options, parsing the target as a collection or `collection('path')`, parsing `insert.source` through `parse_query`, and validating:

- no aggregation projection
- no join
- source is not `collection_group()`
- destination is not `collection_group()`
- source projection is `*` when no destination columns are supplied
- explicit destination columns match explicit source field projection count

- [ ] **Step 4: Run parser tests and verify GREEN**

Run the same `cargo test sql::tests::...` command.

Expected: PASS.

### Task 2: Executor Copy Support

**Files:**
- Modify: `src/executor.rs`
- Modify: `tests/emulator.rs`

- [ ] **Step 1: Write failing emulator tests**

Add emulator tests for:

```rust
#[tokio::test]
async fn emulator_insert_select_auto_id_copy() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_source_{suffix}");
    let dest = format!("fireql_insert_dest_{suffix}");

    let _: serde_json::Value = db
        .create_obj(
            &source,
            Some("u1"),
            &json!({"name": "Alice", "disabled": true, "score": 10}),
            None,
        )
        .await?;
    let _: serde_json::Value = db
        .create_obj(
            &source,
            Some("u2"),
            &json!({"name": "Bob", "disabled": false, "score": 20}),
            None,
        )
        .await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} SELECT * FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    let output = fireql
        .execute(&format!("SELECT * FROM {dest} WHERE disabled = true"))
        .await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_ne!(rows[0].id, "u1");
            match rows[0].data.get("name") {
                Some(FireqlValue::String(name)) => assert_eq!(name, "Alice"),
                other => panic!("expected copied name, got {other:?}"),
            }
            match rows[0].data.get("score") {
                Some(FireqlValue::Integer(score)) => assert_eq!(*score, 10),
                other => panic!("expected copied score, got {other:?}"),
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_insert_select_preserves_id_when_name_column_is_used() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_named_source_{suffix}");
    let dest = format!("fireql_insert_named_dest_{suffix}");

    let _: serde_json::Value = db
        .create_obj(
            &source,
            Some("preserved_id"),
            &json!({"name": "Alice", "disabled": true}),
            None,
        )
        .await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} (__name__, name) \
             SELECT __name__, name FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 1),
        other => panic!("expected affected, got {other:?}"),
    }

    let output = fireql
        .execute(&format!("SELECT * FROM {dest} WHERE name = 'Alice'"))
        .await?;
    match output {
        FireqlOutput::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, "preserved_id");
            assert_eq!(rows[0].path, format!("{dest}/preserved_id"));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn emulator_insert_select_empty_source_reports_zero() -> Result<(), Box<dyn std::error::Error>> {
    if should_skip() {
        eprintln!("skip emulator test: FIRESTORE_EMULATOR_HOST is not set");
        return Ok(());
    }

    let project_id = project_id();
    let db = match open_db(&project_id).await {
        Some(db) => db,
        None => return Ok(()),
    };
    let fireql = match open_fireql(&project_id).await {
        Some(fireql) => fireql,
        None => return Ok(()),
    };

    let suffix = unique_suffix();
    let source = format!("fireql_insert_empty_source_{suffix}");
    let dest = format!("fireql_insert_empty_dest_{suffix}");

    let _: serde_json::Value = db
        .create_obj(&source, Some("u1"), &json!({"disabled": false}), None)
        .await?;

    let output = fireql
        .execute(&format!(
            "INSERT INTO {dest} SELECT * FROM {source} WHERE disabled = true"
        ))
        .await?;
    match output {
        FireqlOutput::Affected { affected } => assert_eq!(affected, 0),
        other => panic!("expected affected, got {other:?}"),
    }

    Ok(())
}
```

- [ ] **Step 2: Run emulator tests and verify RED**

Run: `cargo test emulator_insert_select`

Expected: FAIL because executor does not handle `InsertSelect`.

- [ ] **Step 3: Implement executor support**

Add an `InsertSelect` match arm in `execute`. Implement a helper that:

1. Builds source query params with `build_query_params`.
2. Reads source docs.
3. Converts source docs into write batches.
4. Uses auto-generated IDs when no `__name__` destination column is present.
5. Uses source document IDs when `__name__` is mapped to the destination.
6. Returns `FireqlOutput::Affected { affected }`.

Use raw Firestore `Write` values with `write::Operation::Update` so auto IDs can be generated locally for batch writes.

- [ ] **Step 4: Run emulator tests and verify GREEN**

Run: `cargo test emulator_insert_select`

Expected: PASS.

### Task 3: Documentation and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `README_en.md`
- Modify: `docs/USAGE.md`
- Modify: `docs/USAGE_en.md`

- [ ] **Step 1: Update docs**

Document:

```sql
INSERT INTO archived_users SELECT * FROM users WHERE disabled = true;
INSERT INTO archived_users (__name__, name, age)
SELECT __name__, name, age FROM users WHERE disabled = true;
```

Mention unsupported `VALUES`, `UPSERT`, aggregation, join, and `collection_group()` source copies.

- [ ] **Step 2: Run formatting**

Run: `cargo fmt`

Expected: no output or formatted files only.

- [ ] **Step 3: Run full tests**

Run: `cargo test`

Expected: all tests pass.
