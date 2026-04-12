# fireql 使い方

Firestore を SQL で操作する Rust 製 CLI / ライブラリです。

[English](USAGE_en.md)

## 1. CLI

### ビルド

```bash
cargo build --release
```

### 実行

```bash
./target/release/fireql --project-id my-project --sql "SELECT * FROM users LIMIT 5" --pretty
```

ファイル入力:

```bash
cat query.sql | ./target/release/fireql --project-id my-project
```

### CLI オプション

| オプション | 説明 |
|---|---|
| `--project-id` | GCP プロジェクト ID（必須。環境変数 `GOOGLE_CLOUD_PROJECT` / `GCLOUD_PROJECT` でも可） |
| `--database-id` | Firestore database ID（省略時は `(default)`） |
| `--credentials` | サービスアカウント JSON のパス |
| `--sql` | SQL を直接渡す（省略時は stdin から読む） |
| `--pretty` | JSON を整形出力 |
| `--batch-parallelism` | UPDATE/DELETE のバッチ並列度（既定 1） |

### 認証

- ADC（`gcloud auth application-default login` など）
- サービスアカウント JSON（`--credentials /path/to/key.json`）

## 2. ライブラリ

### 基本的な使い方

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

// JSON 出力
println!("{}", serde_json::to_string_pretty(&output)?);
```

### FireqlValue による型付きアクセス

ライブラリ利用時は結果が `FireqlValue` 型で返され、Firestore 固有の型情報がそのまま保持されます。

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

### FireqlValue 一覧

| FireqlValue | Firestore 型 | JSON 出力 |
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

## 3. SQL 対応範囲

### SELECT

- `SELECT * FROM <collection>`
- `SELECT field1, field2 FROM <collection>`
- `FROM collection_group('name')` に対応
- `WHERE` / `ORDER BY` / `LIMIT`

### UPDATE / DELETE

- `UPDATE <collection> SET ... WHERE ...`
- `DELETE FROM <collection> WHERE ...`
- `DELETE FROM collection_group('name') WHERE ...`

`WHERE` は必須です。

## 4. WHERE で使える演算子 / 値関数

- 比較: `=`, `!=`, `<`, `<=`, `>`, `>=`
- `IN`, `NOT IN`
- `IS NULL`, `IS NOT NULL`
- `AND`, `OR`
- `array_contains(field, value)`
- `array_contains_any(field, [v1, v2, ...])`
- `ref('collection/doc')` または `ref('projects/.../databases/(default)/documents/...')`
- `timestamp('RFC3339')` 例: `timestamp('2024-01-01T00:00:00Z')`, `timestamp('2025-01-01T00:00:00+09:00')`
- `CURRENT_TIMESTAMP`（または `current_timestamp()`）

> `ref('collection/doc')` は実行時のプロジェクト/DB の `documents` パスに展開されます。
> これらの値関数は `WHERE` だけでなく `UPDATE SET` の値にも使えます。
> `CURRENT_TIMESTAMP` は `UPDATE SET` では serverTimestamp に変換され、それ以外では実行時の現在時刻として扱われます。

## 5. 集約（Aggregation）

対応: `COUNT`, `SUM`, `AVG`

```sql
SELECT COUNT(*) FROM users WHERE active = true;
SELECT COUNT(age) FROM users WHERE active = true; -- COUNT(*) と同じ扱い
SELECT SUM(score) AS total FROM users WHERE active = true;
SELECT AVG(score) FROM users WHERE active = true;
```

> 集約のキー名は関数名（`count`/`sum`/`avg`）または `AS` の別名です。
> 集約クエリでは `ORDER BY` / `LIMIT` は使えません。

## 6. 出力形式

### 通常クエリ（SELECT）

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

### 集約クエリ

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

## 7. Firestore 制約（実装で検証）

- `UPDATE` / `DELETE` は `WHERE` 必須
- 不等号（`<`, `<=`, `>`, `>=`, `!=`, `NOT IN`）がある場合、最初の `ORDER BY` が同じフィールドである必要がある
- `IN` / `NOT IN` は最大 10 件まで
- `NOT IN` は `IN` / `!=` と併用不可
- `array_contains` / `array_contains_any` は同時に1つまで
- `array_contains_any` は `IN` / `NOT IN` と併用不可
- `array_contains_any` の要素数は最大 10 件
- 集約は通常フィールドと混在不可（`SELECT name, COUNT(*)` は不可）

## 8. Emulator テスト

`FIRESTORE_EMULATOR_HOST` を設定した場合のみ統合テストが動きます。

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
export FIRESTORE_PROJECT_ID=demo-fireql
cargo test
```

固定の e2e データを投入する場合は `fixtures/emulator-e2e.json` を `fireql-emulator-seed` で流し込みます。

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
export FIRESTORE_PROJECT_ID=demo-fireql
cargo run --bin fireql-emulator-seed
```

投入後にそのまま使えるクエリ:

```sql
SELECT * FROM e2e_users WHERE active = true ORDER BY score DESC LIMIT 10;
SELECT * FROM e2e_users u LEFT JOIN e2e_orders o ON u.__name__ = o.user_id;
SELECT * FROM collection_group('posts') WHERE category = 'release' AND published = true;
```
