# fireql

A Rust CLI / library for querying Firestore with SQL.

[日本語](README.md) | [Detailed Documentation](docs/USAGE_en.md)

## Supported SQL

- `SELECT ... FROM <collection>` / `FROM collection_group('name')`
- `WHERE` (AND / OR / comparison / IN / IS NULL / array_contains / array_contains_any / ref / timestamp / CURRENT_TIMESTAMP)
- Aggregation: `COUNT`, `SUM`, `AVG`
- `ORDER BY` / `LIMIT`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

## Examples

```sql
SELECT * FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10;
SELECT name, profile.age FROM collection_group('profiles') WHERE active = true;
SELECT * FROM users WHERE array_contains(tags, 'rust');
SELECT * FROM users WHERE array_contains_any(tags, ['rust','sql']);
SELECT * FROM users WHERE owner = ref('users/user1');
SELECT * FROM users WHERE created_at >= timestamp('2024-01-01T00:00:00Z');
SELECT * FROM users WHERE created_at >= CURRENT_TIMESTAMP;
SELECT COUNT(*) FROM users WHERE active = true;
SELECT SUM(score) AS total FROM users WHERE active = true;
UPDATE users SET status = 'active', updated_at = CURRENT_TIMESTAMP WHERE last_login < '2024-01-01';
DELETE FROM users WHERE disabled = true;
DELETE FROM collection_group('logs') WHERE created_at < '2023-01-01';
```

## CLI

```bash
fireql --project-id my-project --sql "SELECT * FROM users LIMIT 5" --pretty
```

```bash
cat query.sql | fireql --project-id my-project
```

Use `--batch-parallelism` to parallelize batch writes for UPDATE/DELETE.

```bash
fireql --project-id my-project --sql "DELETE FROM users WHERE disabled = true" --batch-parallelism 4
```

### Authentication

- ADC (`gcloud auth application-default login`, etc.)
- Service account JSON (`--credentials /path/to/key.json`)

## Library

When used as a library, results are returned as typed `FireqlValue` values. Firestore-specific type information (Timestamp, Reference, etc.) is preserved.

```rust
use fireql::{Fireql, FireqlConfig, FireqlOutput, FireqlValue};

let fireql = Fireql::new(
    FireqlConfig::new("my-project")
        .with_credentials_path("/path/to/key.json"),
)
.await?;

let output = fireql.execute("SELECT * FROM users LIMIT 5").await?;

// Access as typed data
if let FireqlOutput::Rows(docs) = &output {
    for doc in docs {
        match doc.data.get("created_at") {
            Some(FireqlValue::Timestamp(dt)) => println!("created: {dt}"),
            Some(FireqlValue::Reference(path)) => println!("ref: {path}"),
            _ => {}
        }
    }
}

// Output as JSON (with Firestore type info)
println!("{}", serde_json::to_string_pretty(&output)?);
```

## Output Format

Firestore-specific types (Timestamp, Reference, GeoPoint, Bytes) are serialized with a `_firestore_type` tag. Primitive types (null, bool, int, double, string) are output as-is.

```json
[
  {
    "id": "user1",
    "path": "users/user1",
    "data": {
      "name": { "_firestore_type": "string", "value": "Alice" },
      "age": { "_firestore_type": "integer", "value": 30 },
      "created_at": {
        "_firestore_type": "timestamp",
        "value": "2024-01-01T00:00:00Z"
      },
      "profile_ref": {
        "_firestore_type": "reference",
        "value": "projects/my-project/databases/(default)/documents/profiles/user1"
      }
    }
  }
]
```

Aggregation queries:

```json
{ "count": { "_firestore_type": "integer", "value": 123 } }
```

Aggregation keys are the function name (`count`/`sum`/`avg`) or the `AS` alias.

## Emulator Tests

Integration tests run only when `FIRESTORE_EMULATOR_HOST` is set.

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
export FIRESTORE_PROJECT_ID=demo-fireql
cargo test
```
