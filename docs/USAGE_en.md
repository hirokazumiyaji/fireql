# fireql Usage

A Rust CLI / library for querying Firestore with SQL.

[日本語](USAGE.md)

## 1. CLI

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/fireql --project-id my-project --sql "SELECT * FROM users LIMIT 5" --pretty
```

File input:

```bash
cat query.sql | ./target/release/fireql --project-id my-project
```

### CLI Options

| Option | Description |
|---|---|
| `--project-id` | GCP project ID (required; can also use env vars `GOOGLE_CLOUD_PROJECT` / `GCLOUD_PROJECT`) |
| `--database-id` | Firestore database ID (defaults to `(default)`) |
| `--credentials` | Path to a service account JSON key file |
| `--sql` | SQL query string (reads from stdin if omitted) |
| `--pretty` | Pretty-print JSON output |
| `--batch-parallelism` | Parallelism for UPDATE/DELETE batch writes (default 1) |

### Authentication

- ADC (`gcloud auth application-default login`, etc.)
- Service account JSON (`--credentials /path/to/key.json`)

## 2. Library

### Basic Usage

```rust
use fireql::{Fireql, FireqlConfig, FireqlOutput};

let fireql = Fireql::new(
    FireqlConfig::new("my-project")
        .with_database_id("my-database")
        .with_credentials_path("/path/to/key.json")
        .with_batch_parallelism(4),
)
.await?;

let output = fireql.execute("SELECT * FROM users LIMIT 5").await?;

// JSON output
println!("{}", serde_json::to_string_pretty(&output)?);
```

### Typed Access with FireqlValue

When used as a library, results are returned as `FireqlValue` types, preserving Firestore-specific type information.

```rust
use fireql::{FireqlOutput, FireqlValue};

match output {
    FireqlOutput::Rows(docs) => {
        for doc in &docs {
            println!("id: {}, path: {}", doc.id, doc.path);
            for (field, value) in &doc.data {
                match value {
                    FireqlValue::Null => println!("  {field}: null"),
                    FireqlValue::Boolean(b) => println!("  {field}: {b}"),
                    FireqlValue::Integer(i) => println!("  {field}: {i}"),
                    FireqlValue::Double(d) => println!("  {field}: {d}"),
                    FireqlValue::String(s) => println!("  {field}: {s}"),
                    FireqlValue::Timestamp(dt) => println!("  {field}: timestamp({dt})"),
                    FireqlValue::Reference(path) => println!("  {field}: ref({path})"),
                    FireqlValue::GeoPoint { latitude, longitude } => {
                        println!("  {field}: geopoint({latitude}, {longitude})")
                    }
                    FireqlValue::Bytes(b) => println!("  {field}: bytes(len={})", b.len()),
                    FireqlValue::Array(arr) => println!("  {field}: array(len={})", arr.len()),
                    FireqlValue::Map(map) => println!("  {field}: map(len={})", map.len()),
                }
            }
        }
    }
    FireqlOutput::Affected { affected } => {
        println!("{affected} documents affected");
    }
    FireqlOutput::Aggregation(data) => {
        for (key, value) in &data {
            println!("{key}: {value:?}");
        }
    }
}
```

### FireqlValue Types

| FireqlValue | Firestore Type | JSON Output |
|---|---|---|
| `Null` | Null | `{"_firestore_type": "null"}` |
| `Boolean(bool)` | Boolean | `{"_firestore_type": "boolean", "value": true}` |
| `Integer(i64)` | Integer | `{"_firestore_type": "integer", "value": 123}` |
| `Double(f64)` | Double | `{"_firestore_type": "double", "value": 1.23}` |
| `String(String)` | String | `{"_firestore_type": "string", "value": "hello"}` |
| `Timestamp(DateTime<Utc>)` | Timestamp | `{"_firestore_type": "timestamp", "value": "..."}` |
| `Reference(String)` | Reference | `{"_firestore_type": "reference", "value": "..."}` |
| `GeoPoint { latitude, longitude }` | GeoPoint | `{"_firestore_type": "geopoint", "latitude": ..., "longitude": ...}` |
| `Bytes(Vec<u8>)` | Bytes | `{"_firestore_type": "bytes", "value": [...]}` |
| `Array(Vec<FireqlValue>)` | Array | `{"_firestore_type": "array", "value": [...]}` |
| `Map(HashMap<String, FireqlValue>)` | Map | `{"_firestore_type": "map", "value": {...}}` |

## 3. SQL Support

### SELECT

- `SELECT * FROM <collection>`
- `SELECT field1, field2 FROM <collection>`
- `FROM collection_group('name')` supported
- `WHERE` / `ORDER BY` / `LIMIT`

### UPDATE / DELETE

- `UPDATE <collection> SET ... WHERE ...`
- `DELETE FROM <collection> WHERE ...`
- `DELETE FROM collection_group('name') WHERE ...`

`WHERE` is required.

## 4. WHERE Operators / Value Functions

- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`
- `IN`, `NOT IN`
- `IS NULL`, `IS NOT NULL`
- `AND`, `OR`
- `array_contains(field, value)`
- `array_contains_any(field, [v1, v2, ...])`
- `ref('collection/doc')` or `ref('projects/.../databases/(default)/documents/...')`
- `timestamp('RFC3339')` e.g. `timestamp('2024-01-01T00:00:00Z')`, `timestamp('2025-01-01T00:00:00+09:00')`
- `CURRENT_TIMESTAMP` (or `current_timestamp()`)

> `ref('collection/doc')` is expanded at runtime using the project/database `documents` path.
> These value functions can be used in both `WHERE` conditions and `UPDATE SET` values.
> `CURRENT_TIMESTAMP` is converted to serverTimestamp in `UPDATE SET`, and treated as the current client-side timestamp elsewhere.

## 5. Aggregation

Supported: `COUNT`, `SUM`, `AVG`

```sql
SELECT COUNT(*) FROM users WHERE active = true;
SELECT COUNT(age) FROM users WHERE active = true; -- treated same as COUNT(*)
SELECT SUM(score) AS total FROM users WHERE active = true;
SELECT AVG(score) FROM users WHERE active = true;
```

> Aggregation keys are the function name (`count`/`sum`/`avg`) or the `AS` alias.
> `ORDER BY` / `LIMIT` cannot be used with aggregation queries.

## 6. Output Format

### Regular Queries (SELECT)

```json
[
  {
    "id": "user1",
    "path": "users/user1",
    "data": {
      "name": { "_firestore_type": "string", "value": "Alice" },
      "age": { "_firestore_type": "integer", "value": 30 },
      "active": { "_firestore_type": "boolean", "value": true },
      "created_at": {
        "_firestore_type": "timestamp",
        "value": "2024-01-01T00:00:00Z"
      },
      "profile_ref": {
        "_firestore_type": "reference",
        "value": "projects/my-project/databases/(default)/documents/profiles/user1"
      },
      "location": {
        "_firestore_type": "geopoint",
        "latitude": 35.6762,
        "longitude": 139.6503
      }
    }
  }
]
```

### Aggregation Queries

```json
{ "count": { "_firestore_type": "integer", "value": 123 } }
```

```json
{ "total": { "_firestore_type": "double", "value": 456.78 } }
```

### UPDATE / DELETE

```json
{ "affected": 5 }
```

## 7. Firestore Constraints (Validated at Query Time)

- `UPDATE` / `DELETE` require a `WHERE` clause
- When using inequality operators (`<`, `<=`, `>`, `>=`, `!=`, `NOT IN`), the first `ORDER BY` field must match the inequality field
- `IN` / `NOT IN` support up to 10 values
- `NOT IN` cannot be combined with `IN` or `!=`
- Only one `array_contains` / `array_contains_any` filter at a time
- `array_contains_any` cannot be combined with `IN` / `NOT IN`
- `array_contains_any` supports up to 10 values
- Aggregations cannot be mixed with regular fields (`SELECT name, COUNT(*)` is not allowed)

## 8. Emulator Tests

Integration tests run only when `FIRESTORE_EMULATOR_HOST` is set.

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
export FIRESTORE_PROJECT_ID=demo-fireql
cargo test
```
