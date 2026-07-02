# 技術的負債解消リファクタリング実装プラン

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** fireql の技術的負債を、パフォーマンス(不要コピー・過剰なネットワーク往復・無制限メモリ)、セキュリティ(未使用依存・CSV インジェクション)、シンプルさ(重複コード・肥大化ファイル)の3観点で解消する。

**Architecture:** 既存の `sql (parse) → planner (Firestore クエリ構築) → executor (実行) → format (出力)` というパイプライン構造・公開 API は一切変えない。各タスクは独立した振る舞い保存リファクタリング(Task 7 のみ CSV 出力の意図的な仕様追加)で、1タスク = 1コミット。

**Tech Stack:** Rust 2021 / `sqlparser` 0.62 / `firestore` 0.49 / `tokio` / `futures`。テストは `cargo test`(ユニット)+ Firestore エミュレーター e2e(あれば)。

## Global Constraints

- 作業ブランチ: `main` から `refactor/tech-debt-cleanup` を作成して作業する。**push はしない**(ローカルコミットのみ。push とマージはリポジトリオーナーが行う)。
- コミットメッセージに AI 由来のトレーラー(`Claude-Session:`、`Co-Authored-By: Claude`、`🤖 Generated with ...` 等)を**絶対に付けない**。subject + 必要なら body のみ。
- 新しい依存クレートを追加しない(Task 1 は削除のみ)。
- `src/lib.rs` の公開 API(`pub use` している型・関数のシグネチャ)を変更しない。
- 各タスクの完了条件(全タスク共通。以下「標準検証」と呼ぶ):
  1. `cargo fmt --all -- --check` → 出力なしで終了コード 0
  2. `cargo clippy --all-targets --all-features -- -D warnings` → `Finished` で終わり警告なし
  3. `cargo test` → すべて `test result: ok.`(エミュレーター未起動時、`tests/emulator.rs` / `tests/e2e_seed.rs` は内部でスキップされる。これは正常)
- タスクは番号順に実行する(Task 3〜5・10 は同じ `src/executor.rs`、Task 7〜8 は同じ `src/format.rs` を触るため、順序が入れ替わるとプラン中のコードと一致しなくなる)。
- プラン中のコードブロックは「変更後の完全な姿」を示す。既存コードを置き換える際は、対象の関数・ブロック全体をコードブロックの内容で置き換えること。

### 採用しなかった変更(実装しないこと)

セキュリティ監査・パフォーマンス監査で挙がったが、複雑性・依存追加・API 変更のコストに見合わないと判断したもの。将来のエージェントが「見落とし」と誤解して再提案しないよう根拠を残す。

- JOIN の右側チャンク取得の並列化 → 1対多 JOIN の出力順が非決定になるため見送り(Task 2 の 10→30 で往復数は 1/3 になる)。
- INSERT SELECT ソースクエリのストリーミング化 → フィールド全体が必要でチャンク処理が複雑化する。LIMIT で抑制可能なため YAGNI。
- `WHERE current_timestamp` の複数出現時刻の統一 → 差はマイクロ秒オーダーで実害なし。UPDATE 系はすでにサーバー側 `RequestTime` transform を使っており問題なし。
- 資格情報の `zeroize` 化 → 新規依存が必要で、トークンは `gcloud-sdk` 内部にもコピーされるため単独では効果が薄い。`Debug` 実装での redact は実装済み。
- エラーメッセージの redact(Firestore SDK エラーの隠蔽) → CLI の主要ユースケースでデバッグ性を大きく損なう。エラーに秘密情報を埋め込むのは SDK 側の責務。
- `ref(path)` / JOIN パスの `..`・`/` バリデーション → Firestore サーバーがリソース名を検証して拒否する。クライアント側の二重検証は複雑性だけ増える。
- `rand` の除去・`getrandom` 直接利用 → `rand::rng()` は CSPRNG(ChaCha + OsRng reseed)であり十分。依存の置き換えは新規依存の追加になる。
- UPDATE/DELETE の行数上限・LIMIT 強制 → `MissingWhere` ガードが既にあり、メモリ問題は Task 5 のストリーミング化で解消。追加フラグは UX 複雑化。
- stdin の SQL サイズ上限 → 自分自身への DoS でしかなく YAGNI。
- `install_default()` の結果の警告ログ → Err は「ホストアプリが既にプロバイダーを設定済み」という正常系。ロギング基盤の導入に見合わない。
- 書き換えプリパーサーへの `;` 拒否追加 → `statements.len() == 1` チェックが既に多文注入を防いでいる。`;` を含む正当なコレクション ID を壊すリスクの方が大きい。
- `to_plain_string` の `expect` 除去 → 到達不能な不変条件であり、握りつぶすとバグを隠す(No Defensive Checks 原則)。
- `FireqlValue` / `JoinKey` の `Arc<str>` 化による clone 削減 → データモデル全体に波及する侵襲的変更。出力行はそれぞれデータを所有する必要があり、効果は限定的。
- UPDATE バッチの `UpdateParts` clone 削減(Arc 化・Write テンプレート) → 各 `Write` proto はワイヤー送信用に自前のコピーを持つ必要があり、clone は本質的に不可避。
- JOIN 右側クエリへの projection pushdown / 左側への LIMIT pushdown → LIMIT+JOIN は現在パーサーで明示的に拒否しており、解除は機能追加(セマンティクス設計が必要)。リファクタリングの範囲外。
- LEFT JOIN の空キー短絡追加 → 不要。キーが空なら `chunk_keys` が空を返し、右側クエリは1本も発行されない(既存テスト `chunk_keys_empty` が保証)。
- `collect_filter_stats` / `build_row_data` / `strip_alias_from_filter` の `&str`・`Cow` 化 → 1文につき1回だけ実行される検証・整形パスであり、測定可能な効果がない。

---

### Task 1: 未使用の直接依存 `jsonwebtoken` を削除(セキュリティ)

`Cargo.toml` の `jsonwebtoken` はソースコードのどこからも直接使われていない(`rg jsonwebtoken src/ tests/` が 0 件)。`gcloud-sdk` が推移的に必要とするだけであり、直接依存に残すとバージョン・フィーチャー管理の負債になる。`jsonwebtoken` を外しても `cargo check` が通ることは検証済み。

**Files:**
- Modify: `Cargo.toml`(23行目 `jsonwebtoken = { version = "10", features = ["rust_crypto"] }` を削除)
- Modify: `Cargo.lock`(cargo が自動更新)

**Interfaces:**
- Consumes: なし
- Produces: なし(ビルドグラフのみの変更)

- [ ] **Step 1: 依存が未使用であることを確認**

Run: `rg -n 'jsonwebtoken' src/ tests/`
Expected: 出力なし(終了コード 1)

- [ ] **Step 2: Cargo.toml から削除**

`Cargo.toml` の `[dependencies]` から次の1行を削除する:

```toml
jsonwebtoken = { version = "10", features = ["rust_crypto"] }
```

- [ ] **Step 3: ビルドと推移的依存の確認**

Run: `cargo check`
Expected: `Finished` で終了コード 0

Run: `cargo tree -i jsonwebtoken | head -8`
Expected: `jsonwebtoken vX.Y.Z` の親が `gcloud-sdk` (経由で `fireql`) のみになっている。`├── fireql`(直接依存)の行が消えている。

- [ ] **Step 4: 標準検証**

Global Constraints の標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 5: コミット**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: remove unused direct dependency jsonwebtoken"
```

---

### Task 2: JOIN の IN チャンクサイズを Firestore 上限の 30 に(パフォーマンス)

`src/executor.rs:28` の `FIRESTORE_IN_LIMIT = 10` は古い制限値。Firestore の `in` フィルターは現在 30 値まで許容し、`src/planner.rs:21` の `MAX_IN_VALUES = 30` もそれを前提に検証している。10→30 にすると JOIN の右側取得クエリ数が 1/3 になる。

**Files:**
- Modify: `src/executor.rs:28`

**Interfaces:**
- Consumes: `crate::joiner::chunk_keys(keys, chunk_size)`(シグネチャ不変)
- Produces: なし(定数値のみの変更)

- [ ] **Step 1: 定数を変更**

`src/executor.rs` の

```rust
const FIRESTORE_IN_LIMIT: usize = 10;
```

を次に置き換える:

```rust
// Firestore allows up to 30 disjunctions in an `in` filter; keep in sync
// with MAX_IN_VALUES in planner.rs.
const FIRESTORE_IN_LIMIT: usize = 30;
```

- [ ] **Step 2: 標準検証**

標準検証 3 コマンドを実行。`src/joiner.rs` のテストはチャンクサイズを引数で渡しているため変更不要。

Expected: すべて成功。

- [ ] **Step 3: (任意)エミュレーター e2e**

エミュレーターが使える環境であれば:

```bash
FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator cargo test --test emulator
```

Expected: `test result: ok.`

- [ ] **Step 4: コミット**

```bash
git add src/executor.rs
git commit -m "perf: raise join IN chunk size to Firestore limit of 30"
```

---

### Task 3: Firestore ドキュメント変換のディープコピー除去(パフォーマンス)

`FireqlValue::from_proto(&Value)` は文字列・バイト列・配列・マップをすべて `clone()` しており、SELECT 結果の全ドキュメントを丸ごとディープコピーしている。呼び出し元(`executor.rs`)はドキュメントを所有しているので、参照ではなく値で受け取ればコピーは丸ごと消せる。振る舞いは変わらない。

**Files:**
- Modify: `src/value.rs:23-59`(`from_proto` / `from_document_fields`)
- Modify: `src/executor.rs`(`doc_to_output` / `docs_to_output` と呼び出し元4箇所)

**Interfaces:**
- Consumes: `firestore::FirestoreQuerySupport::query_doc(...) -> Vec<Document>`(所有権あり)
- Produces:
  - `FireqlValue::from_proto(value: Value) -> Self`(値渡しに変更、`pub(crate)`)
  - `FireqlValue::from_document_fields(fields: HashMap<String, Value>) -> HashMap<String, Self>`(値渡しに変更、`pub(crate)`)
  - `executor::docs_to_output(docs: Vec<Document>) -> Result<Vec<DocOutput>>`(モジュール内 private)

- [ ] **Step 1: `src/value.rs` の変換関数を値渡しに変更**

`from_proto` と `from_document_fields` の2関数を丸ごと次に置き換える:

```rust
    pub(crate) fn from_proto(value: Value) -> Self {
        match value.value_type {
            None | Some(ValueType::NullValue(_)) => Self::Null,
            Some(ValueType::BooleanValue(b)) => Self::Boolean(b),
            Some(ValueType::IntegerValue(i)) => Self::Integer(i),
            Some(ValueType::DoubleValue(d)) => Self::Double(d),
            Some(ValueType::TimestampValue(ts)) => {
                let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos.max(0) as u32)
                    .unwrap_or_default();
                Self::Timestamp(dt)
            }
            Some(ValueType::StringValue(s)) => Self::String(s),
            Some(ValueType::BytesValue(b)) => Self::Bytes(b),
            Some(ValueType::ReferenceValue(r)) => Self::Reference(r),
            Some(ValueType::GeoPointValue(g)) => Self::GeoPoint {
                latitude: g.latitude,
                longitude: g.longitude,
            },
            Some(ValueType::ArrayValue(arr)) => {
                Self::Array(arr.values.into_iter().map(Self::from_proto).collect())
            }
            Some(ValueType::MapValue(map)) => Self::Map(
                map.fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::from_proto(v)))
                    .collect(),
            ),
            _ => Self::Null,
        }
    }

    pub(crate) fn from_document_fields(fields: HashMap<String, Value>) -> HashMap<String, Self> {
        fields
            .into_iter()
            .map(|(k, v)| (k, Self::from_proto(v)))
            .collect()
    }
```

- [ ] **Step 2: `src/executor.rs` の変換関数を所有権受け取りに変更**

`docs_to_output` / `doc_to_output`(executor.rs 末尾付近、`fn docs_to_output` から `fn doc_to_output` の終わりまで)を次に置き換える:

```rust
fn docs_to_output(docs: Vec<gcloud_sdk::google::firestore::v1::Document>) -> Result<Vec<DocOutput>> {
    docs.into_iter().map(doc_to_output).collect()
}

fn doc_to_output(doc: gcloud_sdk::google::firestore::v1::Document) -> Result<DocOutput> {
    let parts = parse_doc_name(&doc.name)?;
    let data = FireqlValue::from_document_fields(doc.fields);

    Ok(DocOutput {
        id: parts.id,
        path: parts.path,
        data,
    })
}
```

- [ ] **Step 3: 呼び出し元4箇所の `&` を外す**

`src/executor.rs` 内で以下のとおり変更する(いずれも参照 `&docs` → 所有 `docs`):

1. `execute_select` の Fields 分岐(executor.rs:100-101 付近):

```rust
            let docs = db.query_doc(params).await?;
            Ok(FireqlOutput::Rows(docs_to_output(docs)?))
```

2. `execute_select` の Aggregations 分岐(executor.rs:113-117 付近)。`&doc.fields` → `doc.fields`:

```rust
            let data = docs
                .into_iter()
                .next()
                .map(|doc| FireqlValue::from_document_fields(doc.fields))
                .unwrap_or_default();
```

3. `execute_join_select` の左側取得(executor.rs:220-221 付近):

```rust
    let left_docs_raw = db.query_doc(left_params).await?;
    let left_docs = docs_to_output(left_docs_raw)?;
```

4. `execute_join_select` のチャンク取得(executor.rs:282-283 付近):

```rust
            let chunk_docs = db.query_doc(right_params).await?;
            right_docs.extend(docs_to_output(chunk_docs)?);
```

- [ ] **Step 4: 標準検証**

標準検証 3 コマンドを実行。既存のシリアライズ・フォーマット系テストがすべて通ること(振る舞い保存の確認)。

Expected: すべて成功。コンパイルエラーが出る場合は `from_proto` / `from_document_fields` の呼び出し漏れ(参照渡しのまま)なので、エラー箇所の `&` を外す。

- [ ] **Step 5: コミット**

```bash
git add src/value.rs src/executor.rs
git commit -m "perf: convert Firestore documents into FireqlValue without deep copies"
```

---

### Task 4: バッチ書き込み結果集約の共通化(シンプルさ)

`execute_insert_select`(executor.rs:409-429)と `execute_batch_write`(executor.rs:611-631)は「`buffer_unordered` ストリームを排出し、成功数を合算、最初のエラーを記録し、`PartialFailure` か `Affected` を返す」というロジックが完全に重複している。共通ヘルパー `drain_batch_results` に抽出する。

**Files:**
- Modify: `src/executor.rs`(ヘルパー追加 + 2箇所の置き換え + ユニットテスト追加)

**Interfaces:**
- Consumes: `FireqlError::PartialFailure { affected, error }`、`FireqlOutput::Affected`
- Produces: `async fn drain_batch_results(stream: impl futures::Stream<Item = std::result::Result<(usize, Option<String>), FireqlError>> + Unpin) -> Result<FireqlOutput>`(モジュール内 private。`(usize, Option<String>)` は `count_batch_outcome` の返り値と同じ「成功数と最初の書き込みエラー」)

- [ ] **Step 1: 失敗するテストを書く**

`src/executor.rs` の `#[cfg(test)] mod tests` 内(`batch_outcome_empty_statuses_assumes_success` テストの後)に追加する:

```rust
    #[tokio::test]
    async fn drain_batch_results_sums_affected_on_success() {
        let stream = stream::iter(vec![
            Ok::<_, FireqlError>((2, None)),
            Ok((3, None)),
        ]);
        let output = drain_batch_results(stream).await.unwrap();
        match output {
            FireqlOutput::Affected { affected } => assert_eq!(affected, 5),
            other => panic!("expected affected output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drain_batch_results_reports_first_error_as_partial_failure() {
        let stream = stream::iter(vec![
            Ok::<_, FireqlError>((2, None)),
            Ok((1, Some("boom".to_string()))),
            Err(FireqlError::Format("io".to_string())),
        ]);
        let err = drain_batch_results(stream).await.unwrap_err();
        match err {
            FireqlError::PartialFailure { affected, error } => {
                assert_eq!(affected, 3);
                assert_eq!(error, "boom");
            }
            other => panic!("expected partial failure, got {other}"),
        }
    }
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test drain_batch_results`
Expected: コンパイルエラー `cannot find function drain_batch_results`

- [ ] **Step 3: ヘルパーを実装**

`src/executor.rs` の `count_batch_outcome` 関数の直後に追加する:

```rust
/// Drains a `buffer_unordered` stream of per-chunk write results, summing the
/// succeeded count and keeping only the first error. Any error downgrades the
/// whole statement to `PartialFailure` so callers never see a partial success
/// reported as a full success.
async fn drain_batch_results(
    mut stream: impl futures::Stream<Item = std::result::Result<(usize, Option<String>), FireqlError>>
        + Unpin,
) -> Result<FireqlOutput> {
    let mut affected = 0u64;
    let mut first_error: Option<String> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok((count, write_error)) => {
                affected += count as u64;
                if first_error.is_none() {
                    first_error = write_error;
                }
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err.to_string());
                }
            }
        }
    }

    if let Some(error) = first_error {
        return Err(FireqlError::PartialFailure { affected, error });
    }

    Ok(FireqlOutput::Affected { affected })
}
```

- [ ] **Step 4: `execute_insert_select` を差し替え**

`execute_insert_select` 内の

```rust
    let mut affected = 0u64;
    let mut first_error: Option<String> = None;

    let mut stream = stream::iter(chunks.into_iter().map(|chunk| {
```

から関数末尾の

```rust
    if let Some(error) = first_error {
        return Err(FireqlError::PartialFailure { affected, error });
    }

    Ok(FireqlOutput::Affected { affected })
}
```

までを、次のコードに置き換える(`async move` ブロック本体は既存と同一):

```rust
    let stream = stream::iter(chunks.into_iter().map(|chunk| {
        let db = db.clone();
        let collection = stmt.collection.clone();
        let columns = stmt.columns.clone();
        let projection = projection.clone();
        async move {
            let parent = insert_parent_path(&db, &collection);
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();

            for doc in &chunk {
                let parts = build_insert_select_parts(doc, columns.as_deref(), &projection)?;
                let id = parts.id.unwrap_or_else(generate_document_id);
                let doc_path = format!("{parent}/{}/{}", collection.collection_id, id);
                let insert_doc = firestore_document_from_map(&doc_path, parts.fields)?;
                batch.add(FireqlWrite(Write {
                    update_mask: None,
                    update_transforms: vec![],
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(false)),
                    }),
                    operation: Some(write::Operation::Update(insert_doc)),
                }))?;
            }

            let response = batch.write().await?;
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk.len(),
            ))
        }
    }))
    .buffer_unordered(batch_parallelism);

    drain_batch_results(stream).await
}
```

- [ ] **Step 5: `execute_batch_write` を同様に差し替え**

`execute_batch_write` 内の `let mut affected = 0u64;` から関数末尾までを次のコードに置き換える(`async move` ブロック本体は既存と同一):

```rust
    let stream = stream::iter(chunks.into_iter().map(|chunk| {
        let db = db.clone();
        let op = op.clone();
        async move {
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();
            for name in &chunk {
                let parts = parse_doc_name(name)?;
                let parent = parts.parent_path(db.get_documents_path().as_str());
                match &op {
                    BatchOp::Update(update_parts) => {
                        let doc_path = format!("{parent}/{}/{}", parts.collection, parts.id);
                        let update_doc =
                            firestore_document_from_map(&doc_path, update_parts.fields.clone())?;
                        batch.add(FireqlWrite(Write {
                            update_mask: Some(DocumentMask {
                                field_paths: update_parts.update_mask_fields.clone(),
                            }),
                            update_transforms: update_parts.transforms.clone(),
                            current_document: None,
                            operation: Some(write::Operation::Update(update_doc)),
                        }))?;
                    }
                    BatchOp::Delete => {
                        batch.delete_by_id_at(parent, &parts.collection, &parts.id, None)?;
                    }
                }
            }
            let response = batch.write().await?;
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk.len(),
            ))
        }
    }))
    .buffer_unordered(batch_parallelism);

    drain_batch_results(stream).await
}
```

- [ ] **Step 6: テストが通ることを確認 + 標準検証**

Run: `cargo test drain_batch_results`
Expected: `test result: ok. 2 passed`

続けて標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 7: コミット**

```bash
git add src/executor.rs
git commit -m "refactor: extract batch write result accumulation into drain_batch_results"
```

---

### Task 5: UPDATE/DELETE の対象取得をストリーミング化(パフォーマンス/メモリ)

`execute_batch_write` は対象ドキュメントを **フィールドごと全件** `Vec<Document>` に載せてから名前だけ取り出している(executor.rs:561-564 の NOTE コメント参照)。書き込みに必要なのはドキュメント名だけなので、ストリーミング API でフィールドをメモリに保持せず名前だけを集める。大規模 UPDATE/DELETE のメモリ使用量が「全ドキュメント本体」から「名前の一覧」に減る。

**Files:**
- Modify: `src/executor.rs`(import 1行 + `execute_batch_write` 冒頭)

**Interfaces:**
- Consumes: `firestore::FirestoreQuerySupport::stream_query_doc_with_errors(params) -> FirestoreResult<BoxStream<'_, FirestoreResult<Document>>>`(firestore 0.49 で存在確認済み)、`futures::stream::TryStreamExt::{map_ok, try_collect}`
- Produces: なし(関数内部のみの変更)

- [ ] **Step 1: import を更新**

`src/executor.rs` の

```rust
use futures::stream::{self, StreamExt};
```

を次に置き換える:

```rust
use futures::stream::{self, StreamExt, TryStreamExt};
```

- [ ] **Step 2: クエリをストリーミングに変更**

`execute_batch_write` 内の

```rust
    // NOTE: All matching documents are loaded into memory before batching.
    // For large result sets, callers should use LIMIT to bound memory usage.
    let docs = db.query_doc(params).await?;
    let doc_names: Vec<String> = docs.into_iter().map(|doc| doc.name).collect();
```

を次に置き換える:

```rust
    // Stream the query so only document names are kept in memory; the full
    // document bodies are dropped as each result arrives.
    let doc_names: Vec<String> = db
        .stream_query_doc_with_errors(params)
        .await?
        .map_ok(|doc| doc.name)
        .try_collect()
        .await?;
```

- [ ] **Step 3: 標準検証**

標準検証 3 コマンドを実行。

Expected: すべて成功。エミュレーターが使える場合は Task 2 Step 3 と同じ e2e も実行し、UPDATE/DELETE 系のテストが通ること。

- [ ] **Step 4: コミット**

```bash
git add src/executor.rs
git commit -m "perf: stream document names for batch update/delete instead of buffering full documents"
```

---

### Task 6: `FireqlValue` の Serialize 実装をヘルパーで簡素化(シンプルさ)

`src/value.rs:95-172` の `Serialize` 実装は「`_firestore_type` + `value` の2エントリーマップ」という同じ形を10回書いている。ヘルパー1つに畳む。JSON 出力はバイト単位で不変(既存テストが保証)。

**Files:**
- Modify: `src/value.rs:95-172`(`impl Serialize for FireqlValue` 全体を置き換え + ヘルパー追加)

**Interfaces:**
- Consumes: 既存の `to_relative_path`、`TypedArray`
- Produces: `fn typed_entry<S: Serializer, T: Serialize + ?Sized>(serializer: S, type_name: &str, value: &T) -> Result<S::Ok, S::Error>`(モジュール内 private)

- [ ] **Step 1: `impl Serialize for FireqlValue` を丸ごと置き換え**

既存の `impl Serialize for FireqlValue { ... }`(直前のコメント2行は残す)を次に置き換える:

```rust
// All types serialize with `_firestore_type`. Most include a `value` key,
// but `Null` omits it and `GeoPoint` uses `latitude`/`longitude` instead.
impl Serialize for FireqlValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("_firestore_type", "null")?;
                map.end()
            }
            Self::Boolean(b) => typed_entry(serializer, "boolean", b),
            Self::Integer(i) => typed_entry(serializer, "integer", i),
            Self::Double(d) => typed_entry(serializer, "double", d),
            Self::String(s) => typed_entry(serializer, "string", s),
            Self::Timestamp(dt) => typed_entry(
                serializer,
                "timestamp",
                &dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            ),
            Self::Bytes(b) => typed_entry(serializer, "bytes", b),
            Self::Reference(r) => typed_entry(serializer, "reference", to_relative_path(r)),
            Self::GeoPoint {
                latitude,
                longitude,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("_firestore_type", "geopoint")?;
                map.serialize_entry("latitude", latitude)?;
                map.serialize_entry("longitude", longitude)?;
                map.end()
            }
            Self::Array(arr) => typed_entry(serializer, "array", &TypedArray(arr)),
            Self::Map(inner) => typed_entry(serializer, "map", inner),
        }
    }
}

fn typed_entry<S: Serializer, T: Serialize + ?Sized>(
    serializer: S,
    type_name: &str,
    value: &T,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(2))?;
    map.serialize_entry("_firestore_type", type_name)?;
    map.serialize_entry("value", value)?;
    map.end()
}
```

- [ ] **Step 2: 標準検証**

標準検証 3 コマンドを実行。`src/value.rs` と `src/format.rs` のシリアライズ系テスト(`test_serialize_reference_normal`、`json_rows`、`json_aggregation` 等)がすべて通ること = JSON 出力が不変であることの証明。

Expected: すべて成功。

- [ ] **Step 3: コミット**

```bash
git add src/value.rs
git commit -m "refactor: collapse FireqlValue Serialize arms into typed_entry helper"
```

---

### Task 7: CSV 出力のフォーミュラインジェクション対策(セキュリティ)

Firestore に保存された文字列は外部入力(攻撃者が書ける)である可能性がある。`--format csv` の出力セルが `=`、`+`、`-`、`@`、タブ、CR で始まると、Excel / Google Sheets で開いたときに数式として実行される(CSV injection)。**文字列型のセルのみ**先頭にシングルクォートを付けて無害化する。数値型(`Integer(-5)` → `"-5"`)や Bytes の base64(`+` 始まりがあり得る)は型で判別して触らないので、機械可読性は壊れない。table / JSON 出力は変更しない。

**Files:**
- Modify: `src/format.rs`(`build_row_data` のシグネチャ変更 + ヘルパー追加 + 呼び出し元2箇所 + テスト追加)

**Interfaces:**
- Consumes: `FireqlValue::to_plain_string()`、`DocOutput`
- Produces:
  - `fn build_row_data(rows: &[DocOutput], escape_formulas: bool) -> (Vec<String>, Vec<Vec<String>>)`(既存関数に bool 引数追加。CSV は `true`、table は `false` を渡す)
  - `fn escape_formula_cell(text: String) -> String`(モジュール内 private)

- [ ] **Step 1: 失敗するテストを書く**

`src/format.rs` の `mod tests` 末尾に追加する:

```rust
    #[test]
    fn csv_string_formula_cell_is_escaped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("=HYPERLINK(\"http://evil\")".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let mut rdr = csv::Reader::from_reader(result.as_bytes());
        let record = rdr.records().next().unwrap().unwrap();
        assert_eq!(record.get(2).unwrap(), "'=HYPERLINK(\"http://evil\")");
    }

    #[test]
    fn csv_negative_integer_is_not_escaped() {
        let mut data = HashMap::new();
        data.insert("delta".to_string(), FireqlValue::Integer(-5));
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines[1], "d1,c/d1,-5");
    }

    #[test]
    fn table_formula_cell_is_not_escaped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("=SUM(A1)".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("=SUM(A1)"));
        assert!(!result.contains("'=SUM(A1)"));
    }
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test format::tests::csv_string_formula_cell_is_escaped`
Expected: FAIL(`'=HYPERLINK` ではなく `=HYPERLINK` が出力されるため assertion 失敗)

- [ ] **Step 3: 実装**

`src/format.rs` の `build_row_data` を丸ごと次に置き換える:

```rust
/// Spreadsheet apps execute cells starting with '=', '+', '-', '@', TAB or CR
/// as formulas, so exported CSV can trigger code execution when opened
/// (CSV injection). Only string-typed cells are escaped: numeric, bytes, and
/// JSON-encoded cells must stay machine-readable and their leading characters
/// are produced by fireql itself, not by document authors.
fn escape_formula_cell(text: String) -> String {
    match text.as_bytes().first() {
        Some(b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r') => format!("'{text}"),
        _ => text,
    }
}

fn build_row_data(rows: &[DocOutput], escape_formulas: bool) -> (Vec<String>, Vec<Vec<String>>) {
    let escape = |text: String| {
        if escape_formulas {
            escape_formula_cell(text)
        } else {
            text
        }
    };

    let field_names = collect_field_names(rows);
    let mut header = vec![escape("id".to_string()), escape("path".to_string())];
    header.extend(field_names.iter().map(|f| escape(format!("data.{f}"))));

    let data_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut record = vec![escape(row.id.clone()), escape(row.path.clone())];
            for field in &field_names {
                let cell = match row.data.get(field) {
                    Some(v @ (FireqlValue::String(_) | FireqlValue::Reference(_))) => {
                        escape(v.to_plain_string())
                    }
                    Some(v) => v.to_plain_string(),
                    None => String::new(),
                };
                record.push(cell);
            }
            record
        })
        .collect();

    (header, data_rows)
}
```

`src/format.rs` の先頭 import に `FireqlValue` を追加する:

```rust
use crate::error::Result;
use crate::output::{DocOutput, FireqlOutput};
use crate::value::FireqlValue;
```

呼び出し元を2箇所変更する:

- `format_csv` 内: `let (header, data_rows) = build_row_data(rows);` → `let (header, data_rows) = build_row_data(rows, true);`
- `format_table` 内: `let (header, data_rows) = build_row_data(rows);` → `let (header, data_rows) = build_row_data(rows, false);`

- [ ] **Step 4: テストが通ることを確認 + 標準検証**

Run: `cargo test --lib format`
Expected: 既存 CSV/table テスト + 新規3テストがすべて PASS(既存テストのデータは危険文字で始まらないため出力不変)。

続けて標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 5: README に仕様を明記**

`README.md` の CSV 出力(`--format csv`)について説明している箇所の直後に次の1文を追加する(該当箇所は `rg -n 'csv' README.md` で探す):

```markdown
> CSV 出力では、`=` `+` `-` `@` などで始まる文字列セルに `'` を前置してスプレッドシートでの数式実行(CSV injection)を防ぎます。
```

`README_en.md` の対応箇所には次を追加する:

```markdown
> In CSV output, string cells starting with `=` `+` `-` `@` are prefixed with `'` to prevent formula execution (CSV injection) in spreadsheet apps.
```

- [ ] **Step 6: コミット**

```bash
git add src/format.rs README.md README_en.md
git commit -m "fix: escape formula-like string cells in CSV output to prevent CSV injection"
```

---

### Task 8: table 出力の制御文字サニタイズ(セキュリティ)

Firestore に保存された文字列に ANSI/OSC エスケープシーケンス(例: `\x1b]8;;http://attacker/\x07`)が含まれていると、`--format table` で端末に表示した際に端末の書き換え・ハイパーリンク偽装ができてしまう。table は人間が端末で読む表示専用フォーマットなので、制御文字(改行・タブ以外)を落としてから描画する。JSON は serde が `\u001b` のようにエスケープするため安全、CSV は機械可読データなので変更しない。

**注意:** このタスクは Task 7 完了後に実行すること(同じ `src/format.rs` を変更し、Task 7 適用後のコードを前提とする)。

**Files:**
- Modify: `src/format.rs`(ヘルパー追加 + `format_table` の Rows / Aggregation 分岐 + テスト追加)

**Interfaces:**
- Consumes: Task 7 適用後の `build_row_data(rows, false)`
- Produces: `fn strip_control_chars(text: &str) -> String`(モジュール内 private)

- [ ] **Step 1: 失敗するテストを書く**

`src/format.rs` の `mod tests` 末尾に追加する:

```rust
    #[test]
    fn table_control_chars_are_stripped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("\u{1b}]8;;http://evil\u{7}click me".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Table.format(&output, false).unwrap();
        assert!(!result.contains('\u{1b}'));
        assert!(!result.contains('\u{7}'));
        assert!(result.contains("click me"));
    }
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test format::tests::table_control_chars_are_stripped`
Expected: FAIL(`\u{1b}` が出力に残っているため assertion 失敗)

- [ ] **Step 3: 実装**

`src/format.rs` の `format_table` の直前にヘルパーを追加する:

```rust
/// Firestore strings may embed ANSI/OSC escape sequences that rewrite the
/// operator's terminal when rendered. Table output is display-only, so drop
/// control characters (keeping newline and tab, which comfy-table renders
/// safely) before drawing. JSON already escapes them and CSV must stay
/// byte-faithful for machine consumers.
fn strip_control_chars(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
        .collect()
}
```

`format_table` の `FireqlOutput::Rows` 分岐で、ヘッダーと行の追加を次に変更する:

```rust
            table.set_header(header.iter().map(|h| strip_control_chars(h)));
            for cells in data_rows {
                table.add_row(cells.iter().map(|c| strip_control_chars(c)));
            }
```

`FireqlOutput::Aggregation` 分岐で、ヘッダーと値の行を次に変更する:

```rust
            table.set_header(keys.iter().map(|k| strip_control_chars(k)));
            let values: Vec<String> = keys
                .iter()
                .map(|k| strip_control_chars(&map[*k].to_plain_string()))
                .collect();
            table.add_row(values);
```

`Affected` 分岐は数値のみなので変更しない。

- [ ] **Step 4: テストが通ることを確認 + 標準検証**

Run: `cargo test --lib format`
Expected: 既存 table テスト + 新規テストがすべて PASS。

続けて標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 5: コミット**

```bash
git add src/format.rs
git commit -m "fix: strip terminal control characters from table output"
```

---

### Task 9: `sql.rs` のモジュール分割(シンプルさ)

`src/sql.rs` は 2,234 行(実装 ~1,480 行 + テスト ~750 行)あり、AST 型定義・sqlparser ベースのパーサー・文字列書き換えプリパーサー(`INSERT INTO collection(...)` / `DELETE FROM collection(...)` 対応)という3つの責務が同居している。**コードは一切変更せず**、機械的に4ファイルへ移動する。テストは公開シンボル(`parse_sql` と AST 型)しか参照していないことを確認済みなので、そのまま移動できる。

**Files:**
- Delete: `src/sql.rs`
- Create: `src/sql/mod.rs`(AST 型 + 定数 + `parse_collection_relative_path`)
- Create: `src/sql/parser.rs`(sqlparser AST → fireql AST 変換のすべて)
- Create: `src/sql/rewrite.rs`(文字列書き換えプリパーサー)
- Create: `src/sql/tests.rs`(既存 `mod tests` の中身)

**Interfaces:**
- Consumes: 既存の全シンボル(移動のみ)
- Produces: 外部から見えるパスは完全に不変 — `crate::sql::parse_sql`、`crate::sql::parse_collection_relative_path`、`crate::sql::{StatementAst, SelectStatement, ...}`、`crate::sql::{FIREQL_REF_KEY, FIREQL_TS_KEY, FIREQL_CURRENT_TS_KEY}`。`src/lib.rs` は変更不要(`mod sql;` はディレクトリモジュールでもそのまま動く)。

- [ ] **Step 1: 分割前の状態を記録**

Run: `cargo test --lib sql 2>&1 | tail -1 && wc -l src/sql.rs`
Expected: `test result: ok.` と行数(後で移動漏れがないか突き合わせる)。テスト数を控えておく。

- [ ] **Step 2: `src/sql/` を作成し、`mod.rs` を書く**

`mkdir src/sql` のうえで `src/sql/mod.rs` を作成する。内容は次のフレームに、現在の `src/sql.rs` から **型定義・定数をそのまま** 移す:

```rust
mod parser;
mod rewrite;
#[cfg(test)]
mod tests;

pub use parser::parse_sql;

use crate::error::{FireqlError, Result};
use serde_json::Value as JsonValue;

// ここに src/sql.rs から次の項目を「一字一句そのまま」移動する:
// - StatementAst, CollectionSpec, Projection (sql.rs:11-30)
// - FIREQL_REF_KEY, FIREQL_TS_KEY, FIREQL_CURRENT_TS_KEY (sql.rs:32-34)
// - SelectProjection, AggregationExpr, AggregationFunc (sql.rs:53-71)
// - SelectStatement, UpdateStatement, DeleteStatement, InsertSelectStatement (sql.rs:73-106)
// - OrderBy, OrderDirection, JoinType, JoinSpec (sql.rs:108-134)
// - FilterExpr, CompareOp, UnaryOp (sql.rs:136-178)
// - COLLECTION_PATH_ERR と pub fn parse_collection_relative_path (sql.rs:902-917)
```

- [ ] **Step 3: `src/sql/rewrite.rs` を書く**

次の関数を `src/sql.rs` から**本体は一字一句そのまま**移動する。ただし先頭に挙げた3関数は `parser.rs` の `parse_sql` から呼ばれるため、`fn` を `pub(super) fn` に変更する:

- `try_parse_insert_collection_function`(sql.rs:248-292)→ `pub(super) fn`
- `try_parse_delete_table_function`(sql.rs:508-564)→ `pub(super) fn`
- `strip_keyword`(sql.rs:294-305)→ private のまま
- `find_matching_paren`(sql.rs:307-342)→ private のまま
- `parse_collection_target_expr`(sql.rs:344-360)→ private のまま

ファイル先頭の import:

```rust
use super::parser::{parse_insert_select, parse_query};
use super::{CollectionSpec, DeleteStatement, StatementAst};
use crate::error::{FireqlError, Result};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
```

- [ ] **Step 4: `src/sql/parser.rs` を書く**

`src/sql.rs` に残っている**すべての関数**(Step 2・3 で移動したもの以外全部)を移動する。対象:
`sentinel_object`, `reject_function_modifiers`, `parse_sql`, `parse_insert_select`, `parse_insert_target`, `validate_insert_select_projection`, `parse_query`, `parse_select`, `validate_join_filter_aliases`, `parse_table_with_joins`, `parse_table_with_joins_for_select`, `parse_table_factor_with_alias`, `parse_join_on_expr`, `parse_compound_ident_expr`, `parse_table_factor`, `collection_function_arg`, `parse_collection_group_args`, `parse_collection_args`, `parse_object_name`, `parse_projection`, `parse_aggregate_expr`, `expr_to_string_literal`, `extract_function_arg_list`, `parse_count_arg`, `parse_single_field_arg`, `validate_unique_aggregate_aliases`, `parse_order_and_limit_from_query_parts`, `parse_order_by_expr`, `parse_limit_expr`, `parse_assignments`, `parse_filter_expr`, `parse_filter_function`, `parse_function_args`, `parse_value_list_expr`, `merge_filters`, `parse_field_expr`, `parse_value_expr`, `parse_value_function`, `parse_value`, `parse_numeric`, `object_name_to_string`

可視性の変更は次の3つだけ。それ以外は元のまま:

- `pub fn parse_sql` → そのまま `pub`(mod.rs が re-export)
- `fn parse_query` → `pub(super) fn parse_query`(rewrite.rs から呼ばれる)
- `fn parse_insert_select` → `pub(super) fn parse_insert_select`(rewrite.rs から呼ばれる)

`parse_sql` 内の `try_parse_delete_table_function(input)?` / `try_parse_insert_collection_function(input)?` 呼び出しを `super::rewrite::try_parse_delete_table_function(input)?` / `super::rewrite::try_parse_insert_collection_function(input)?` に変更する。

`parse_collection_args` 内の `parse_collection_relative_path(&raw)?` は `super::parse_collection_relative_path(&raw)?` に変更する(または `use super::parse_collection_relative_path;` を足す)。

ファイル先頭の import(現在の sql.rs:1-9 とほぼ同じ + super の型):

```rust
use super::{
    AggregationExpr, AggregationFunc, CollectionSpec, CompareOp, DeleteStatement, FilterExpr,
    InsertSelectStatement, JoinSpec, JoinType, OrderBy, OrderDirection, Projection,
    SelectProjection, SelectStatement, StatementAst, UnaryOp, UpdateStatement,
    FIREQL_CURRENT_TS_KEY, FIREQL_REF_KEY, FIREQL_TS_KEY,
};
use crate::error::{FireqlError, Result};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderByExpr, OrderByKind, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
```

- [ ] **Step 5: `src/sql/tests.rs` を書く**

現在の `src/sql.rs` の `#[cfg(test)] mod tests { ... }`(sql.rs:1486 以降)の**中身**(`mod tests {` と最後の `}` を除いた内部)を丸ごと `src/sql/tests.rs` に移動する。先頭の `use super::*;` はそのままでよい(親が `sql` モジュールになり、`parse_sql`・AST 型・定数はすべて解決する)。

- [ ] **Step 6: 旧ファイルを削除して検証**

```bash
rm src/sql.rs
cargo test --lib sql 2>&1 | tail -1
```

Expected: Step 1 と同じテスト数で `test result: ok.`。

コンパイルエラーが出た場合の対処:
- `cannot find function/type X` → その関数・型の移動漏れ。エラーメッセージの指す名前を該当ファイルに移す。
- `function X is private` → 上記の可視性リスト(`pub(super)`)の適用漏れ。
- unused import 警告 → そのファイルで使っていない `use` 行を削除する。

行数の突き合わせ: `wc -l src/sql/*.rs` の合計が分割前の `src/sql.rs`(+モジュール宣言ぶん数行)とおおむね一致すること。大きく減っていたら移動漏れ。

- [ ] **Step 7: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 8: コミット**

```bash
git add src/sql.rs src/sql/
git commit -m "refactor: split sql.rs into ast (mod), parser, rewrite, and tests modules"
```

---

### Task 10: INSERT SELECT のドキュメント二重コピー除去(パフォーマンス)

`execute_insert_select` はコピー対象ドキュメントを (1) `chunks(...).map(|c| c.to_vec())` で全ドキュメント丸ごと複製し、(2) `build_insert_select_parts` 内の `value.clone()` でフィールドを再度複製している。大量コピー時にデータサイズの2倍の無駄なコピーが発生する。チャンクを所有権移動で構築し、`SELECT *` 経路ではフィールドをムーブする。

**注意:** このタスクは Task 4 適用後の `execute_insert_select` を前提とする(Task 9 とは独立なので、Task 4 完了後であれば順序は問わない)。

**列指定経路(`Projection::Fields`)は `doc.fields.get(source)` + clone のまま残すこと。** `remove` に変えると `SELECT x, x` のように同一ソース列を2回参照するクエリで2回目が欠落し、振る舞いが変わるため。

**Files:**
- Modify: `src/executor.rs`(`execute_insert_select` のチャンク構築 + async ブロック内ループ + `build_insert_select_parts` + 既存テスト3件の呼び出し)

**Interfaces:**
- Consumes: Task 4 適用後の `drain_batch_results`
- Produces: `fn build_insert_select_parts(doc: Document, columns: Option<&[String]>, projection: &Projection) -> Result<InsertSelectParts>`(`&Document` から値渡しに変更)

- [ ] **Step 1: チャンク構築を所有権移動に変更**

`execute_insert_select` 内の

```rust
    let chunks = docs
        .chunks(BATCH_LIMIT)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();
```

を次に置き換える:

```rust
    let mut chunks: Vec<Vec<Document>> = Vec::new();
    let mut doc_iter = docs.into_iter().peekable();
    while doc_iter.peek().is_some() {
        chunks.push(doc_iter.by_ref().take(BATCH_LIMIT).collect());
    }
```

- [ ] **Step 2: async ブロック内でドキュメントを値で消費**

`execute_insert_select` の async ブロック内の

```rust
            let parent = insert_parent_path(&db, &collection);
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();

            for doc in &chunk {
                let parts = build_insert_select_parts(doc, columns.as_deref(), &projection)?;
```

を次に置き換える(`chunk.len()` は消費前に控える):

```rust
            let parent = insert_parent_path(&db, &collection);
            let writer = db.create_simple_batch_writer().await?;
            let mut batch = writer.new_batch();
            let chunk_len = chunk.len();

            for doc in chunk {
                let parts = build_insert_select_parts(doc, columns.as_deref(), &projection)?;
```

同じ async ブロック末尾の

```rust
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk.len(),
            ))
```

を次に置き換える:

```rust
            Ok::<(usize, Option<String>), FireqlError>(count_batch_outcome(
                &response.statuses,
                chunk_len,
            ))
```

- [ ] **Step 3: `build_insert_select_parts` を値渡しに変更**

関数全体を次に置き換える(`SELECT *` 経路のみムーブ化。列指定経路は同一ソース列の重複参照を壊さないよう clone のまま):

```rust
fn build_insert_select_parts(
    doc: Document,
    columns: Option<&[String]>,
    projection: &Projection,
) -> Result<InsertSelectParts> {
    match (columns, projection) {
        (None, Projection::All) => Ok(InsertSelectParts {
            id: None,
            fields: doc
                .fields
                .into_iter()
                .map(|(field, value)| (field, firestore::FirestoreValue::from(value)))
                .collect(),
        }),
        (Some(columns), Projection::Fields(source_fields)) => {
            let doc_id = parse_doc_name(&doc.name)?.id;
            let mut id = None;
            let mut fields = Vec::new();

            for (target, source) in columns.iter().zip(source_fields) {
                if target == "__name__" {
                    if source != "__name__" {
                        return Err(FireqlError::InvalidQuery(
                            "__name__ destination column requires __name__ source field"
                                .to_string(),
                        ));
                    }
                    id = Some(doc_id.clone());
                    continue;
                }

                if source == "__name__" {
                    let value: firestore::FirestoreValue = JsonValue::String(doc_id.clone()).into();
                    fields.push((target.clone(), value));
                } else if let Some(value) = doc.fields.get(source) {
                    fields.push((
                        target.clone(),
                        firestore::FirestoreValue::from(value.clone()),
                    ));
                }
            }

            Ok(InsertSelectParts { id, fields })
        }
        _ => Err(FireqlError::InvalidQuery(
            "Invalid INSERT SELECT projection".to_string(),
        )),
    }
}
```

- [ ] **Step 4: 既存テストの呼び出しを値渡しに更新**

`src/executor.rs` の `mod tests` 内で `build_insert_select_parts(&doc, ...)` と呼んでいる3箇所(`insert_select_parts_copy_all_fields_with_auto_id`、`insert_select_parts_preserve_document_id_from_name_column`、`insert_select_parts_can_rename_explicit_fields`)の `&doc` を `doc` に変更する。

- [ ] **Step 5: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。エミュレーターが使える場合は INSERT SELECT 系の e2e(`cargo test --test emulator insert`)も通すこと。

- [ ] **Step 6: コミット**

```bash
git add src/executor.rs
git commit -m "perf: move documents into INSERT SELECT batches instead of double-cloning"
```

---

## 完了後の最終確認

- [ ] 全タスク完了後、ブランチ全体で標準検証 3 コマンドを再実行してすべて成功すること。
- [ ] `git log --oneline main..HEAD` がタスク数ぶん(10コミット)並んでいること。
- [ ] エミュレーターが使える環境なら `FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator cargo test` で e2e を通すこと(CI と同条件)。
- [ ] push はしない。完了報告のみ行い、push/マージはオーナーに委ねる。
