# fireql

Firestore を SQL で操作する Rust 製 CLI / ライブラリです。

[English](README_en.md) | [詳細ドキュメント](docs/USAGE.md)

## 対応 SQL

- `SELECT ... FROM <collection>` / `FROM collection_group('name')`
- `WHERE`（AND / OR / 比較 / IN / IS NULL / array_contains / array_contains_any / ref / timestamp / CURRENT_TIMESTAMP）
- 集約: `COUNT`, `SUM`, `AVG`
- `ORDER BY` / `LIMIT`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

## 例

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

`--batch-parallelism` で UPDATE/DELETE のバッチ実行を並列化できます。

```bash
fireql --project-id my-project --sql "DELETE FROM users WHERE disabled = true" --batch-parallelism 4
```

### 認証

- ADC（`gcloud auth application-default login` など）
- サービスアカウント JSON（`--credentials /path/to/key.json`）

## ライブラリ

ライブラリとして使う場合、結果は `FireqlValue` 型で返されます。Firestore 固有の型情報（Timestamp, Reference など）がそのまま保持されます。

```rust
use fireql::{Fireql, FireqlConfig, FireqlOutput, FireqlValue};

let fireql = Fireql::new(
    FireqlConfig::new("my-project")
        .with_credentials_path("/path/to/key.json"),
)
.await?;

let output = fireql.execute("SELECT * FROM users LIMIT 5").await?;

// 型付きデータとしてアクセス
if let FireqlOutput::Rows(docs) = &output {
    for doc in docs {
        match doc.data.get("created_at") {
            Some(FireqlValue::Timestamp(dt)) => println!("created: {dt}"),
            Some(FireqlValue::Reference(path)) => println!("ref: {path}"),
            _ => {}
        }
    }
}

// JSON として出力（Firestore 型情報付き）
println!("{}", serde_json::to_string_pretty(&output)?);
```

## 出力形式

すべての値は `_firestore_type` タグ付きで JSON 出力されます。プリミティブ型（null, bool, int, double, string）も同様です。

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
        "value": "profiles/user1"
      }
    }
  }
]
```

集約クエリ:

```json
{ "count": { "_firestore_type": "integer", "value": 123 } }
```

集約のキー名は関数名（`count`/`sum`/`avg`）または `AS` の別名です。

## Emulator テスト

`FIRESTORE_EMULATOR_HOST` を設定したときのみ統合テストが実行されます。

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
export FIRESTORE_PROJECT_ID=demo-fireql
cargo test
```
