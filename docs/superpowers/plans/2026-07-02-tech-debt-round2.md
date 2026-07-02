# 技術的負債解消リファクタリング実装プラン(第2ラウンド)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 第1ラウンド(PR #18)後に残った fireql の技術的負債を解消する。中心は「sqlparser が受理するのに fireql が黙って無視する SQL 句」の排除(破壊的操作の意味が変わる正しさ/安全性の問題)、冗長になった DELETE プリパーサーの削除、センチネル JSON オブジェクトの型付き enum 化。

**Architecture:** 既存の `sql (parse) → planner (Firestore クエリ構築) → executor (実行) → format (出力)` というパイプライン構造は一切変えない。Task 1 のみ意図的な振る舞い修正(UPDATE の ORDER BY/LIMIT が黙殺→機能する)、Task 2 は「黙殺→明示エラー」、Task 6 はエラー分類の是正、Task 3〜5・7 は振る舞い保存リファクタリング。1タスク = 1コミット。

**Tech Stack:** Rust 2021 / `sqlparser` 0.62 / `firestore` 0.49 / `chrono` / `tokio`。テストは `cargo test`(ユニット 169 件+)と Firestore エミュレーター e2e(あれば)。

## Global Constraints

- 作業ブランチ: `main` から `refactor/tech-debt-round2` を作成して作業する。**push はしない**(ローカルコミットのみ。push とマージはリポジトリオーナーが行う)。
- コミットメッセージに AI 由来のトレーラー(`Claude-Session:`、`Co-Authored-By: Claude`、`🤖 Generated with ...` 等)を**絶対に付けない**。subject + 必要なら body のみ。
- 新しい依存クレートを追加しない(`chrono` は既存依存)。
- `src/lib.rs` の公開 API(`pub use` している型・関数のシグネチャ)を変更しない。
- 各タスクの完了条件(全タスク共通。以下「標準検証」と呼ぶ):
  1. `cargo fmt --all -- --check` → 出力なしで終了コード 0
  2. `cargo clippy --all-targets --all-features -- -D warnings` → `Finished` で終わり警告なし
  3. `cargo test` → すべて `test result: ok.`(エミュレーター未起動時、`tests/emulator.rs` / `tests/e2e_seed.rs` は内部でスキップされ passed 表示になる。これは正常)
- タスクは番号順に実行する。Task 1〜3 と Task 5 は同じ `src/sql/parser.rs` を、Task 5〜7 は同じ `src/executor.rs` を触るため、順序が入れ替わるとプラン中のコードと一致しなくなる(特に Task 7 の分割は最後)。
- プラン中のコードブロックは「変更後の完全な姿」を示す。既存コードを置き換える際は、対象の関数・ブロック全体をコードブロックの内容で置き換えること。
- 行番号は Task 開始時点(直前のタスク適用後)の概算。関数名・コード内容を優先して位置を特定すること。

## 監査サマリー(このプランの根拠)

パフォーマンス・セキュリティ・シンプルさの3観点で src/ 全体(+tests/、CI 設定)を監査した。第1ラウンドで主要なホットパス最適化(deep copy 除去・ストリーミング化・IN チャンク 30)とセキュリティ対策(CSV インジェクション・制御文字サニタイズ・資格情報 Debug redact)は完了済みであり、今回新たに確認された負債は以下:

1. **[正しさ/安全性・高]** `UPDATE ... ORDER BY x LIMIT n` が GenericDialect で正常にパースされるのに、`src/sql/parser.rs` が `order_by: vec![], limit: None` をハードコードしているため**黙って全件更新になる**(sqlparser の `Update` 構造体は `order_by`/`limit` を持ち、`executor::execute_batch_write` は既に両方を処理できる。パーサーだけが未配線)。→ Task 1
2. **[正しさ/安全性・高]** `DELETE ... USING` / `DELETE ... RETURNING` / `UPDATE ... FROM` / `UPDATE OR IGNORE` / `WITH (CTE)` / `FETCH FIRST n ROWS` / `FOR UPDATE` はすべて sqlparser が受理し、fireql が**黙って無視**する(sqlparser 0.62 でパースされ該当フィールドに格納されることを実機検証済み)。`parse_select` には「黙殺禁止」の拒否テーブルが既にあるのに、`parse_query`/UPDATE/DELETE 分岐には無い。→ Task 2
3. **[シンプルさ・高]** `src/sql/rewrite.rs` の `try_parse_delete_table_function`(57行)は冗長。sqlparser 0.62 は `DELETE FROM collection('...')` / `collection_group('...')`(ORDER BY/LIMIT 付き含む)を `TableFactor::Table { args: Some(..) }` としてネイティブにパースし、既存の `parse_table_factor` がそのまま処理する。**削除して全 169 ユニットテスト+clippy がグリーンになることを一時 worktree で検証済み。**(INSERT 側のリライトは `INSERT INTO collection('path') (cols...) SELECT` がネイティブではパースエラーになるため今も必要。削除しないこと。)→ Task 3
4. **[シンプルさ・低]** `src/format.rs` の CSV / table 出力で Aggregation のキーのソート+値の文字列化ロジックが重複。→ Task 4
5. **[シンプルさ/型安全・中]** `ref()`/`timestamp()`/`CURRENT_TIMESTAMP` を `JsonValue::Object` +マジックキー(`__fireql_ref` 等)のセンチネルで表現しており、パーサー・プランナー・エグゼキューターの3箇所がキー文字列の暗黙の合意で結合している。型付き enum `SqlValue` にすると不正状態が表現不能になり、定数3つ・センチネル生成/判定関数が消える。→ Task 5
6. **[エラー品質・中]** `joiner.rs` が `Result<_, String>` を返し、executor が `FireqlError::Unsupported` に包み直すため、JOIN キーに Double 等を使ったときの**データ起因のエラーが「Unsupported SQL: ...」と誤分類**される(ユーザーは SQL を直せと誘導されるが、問題はドキュメントのデータ型)。→ Task 6
7. **[シンプルさ・中]** `src/executor.rs`(1,010行)が SELECT/JOIN 実行・INSERT SELECT・バッチ書き込み・ドキュメント名パースの無関係な責務を1ファイルに抱えるグラブバッグになっている(第1ラウンドで `sql.rs` を同じ理由で分割済み)。→ Task 7

パフォーマンス観点は、第1ラウンドで対処済みの項目以外に複雑度クラスの誤りや行単位の無駄な往復は発見されなかった(JOIN はハッシュ結合、バッチ書き込みはストリーミング+並列、集約はサーバーサイド)。セキュリティ観点は独立監査でも「WHERE 必須ガード・部分失敗報告・CSV/端末エスケープ対策・資格情報 redact・`ref()` 展開・JOIN の `__name__` インジェクション・再帰深度・整数キャスト・依存クレート」のすべてが健全と確認された。

### 採用しなかった変更(実装しないこと)

第1ラウンドのプランの同名セクション(PR #18 のプラン文書参照)は全項目引き続き有効。今回の監査で新たに検討して見送ったもの:

- `FirestoreDb` のトレイト抽象化(モック注入用) → エミュレーターが「本物に近い fake」としてテスト境界を担っており、CI でも常時起動している。トレイト化は firestore クレートの多数の support トレイトを包む大きな抽象を要求し、YAGNI。
- `CollectionSpec` へのパス構築ヘルパー追加(`{documents_path}/{parent_path}` 形式の組み立てが planner・executor 2箇所+insert_parent_path にある) → 3箇所は各々微妙に形が違い(親のみ/親+コレクション ID/Option 検証付き)、共通化すると引数フラグが増えて逆に複雑になる。
- エミュレーターテストのスキップガード(`if should_skip() { ... return; }` の9回繰り返し)のマクロ化 → 定型だが自明で、マクロは可読性を下げる。CI では常にエミュレーターが起動しておりスキップされない。
- `chunk_keys` の削除(`keys.chunks(n).collect()` の薄いラッパー) → テスト済みの명確な名前付き関数。削除の利益がない。
- `parse_select` の拒否テーブルの「構造体全フィールド分解」化(sqlparser 更新時のフィールド追加をコンパイルエラーで検出する技法) → sqlparser のフィールドは非常に多く、ノイズが利益を上回る。Task 2 で拒否リストを埋めれば実用上十分。
- UPDATE の `order_by`/`limit` フィールドを「拒否」にする案 → sqlparser の `Update` はフィールドを持ち、`UpdateStatement`(fireql 側)にもフィールドが存在し、`execute_batch_write` は DELETE 用に既に両方を処理している。配線が最小差分(Task 1)。
- SELECT / INSERT SELECT の結果セットのストリーミング化・行数上限(セキュリティ監査指摘: LIMIT なしの `SELECT *` が全ドキュメントをメモリに載せる) → CSV/table 出力は全行のフィールド和集合からヘッダーを作るため全件材料化が本質的に必要。抑制手段は LIMIT で、被害は自プロセスの OOM のみ(CLI の主要ユースケース)。INSERT SELECT のストリーミング化は第1ラウンドで YAGNI として見送り済み。ライブラリとして多テナント API の背後に置く場合はホスト側でレート制御・LIMIT 強制を行うこと。
- `collection()` パスの `.` / `..` セグメント拒否(セキュリティ監査指摘: defense-in-depth) → Firestore サーバーがリソース名セグメントとして `.` / `..` を拒否するため現状悪用不能。第1ラウンドの「ref(path)/JOIN パスのクライアント側二重検証はしない」方針と同一クラス。サーバー側の正規化仕様が変わった場合のみ再検討。
- JOIN プローブクエリ構築の planner API 化(`build_join_probe_params`)と `strip_alias_from_filter` の sql/ への移動(アーキテクチャ監査指摘) → プローブ構築は10行程度で、専用 API は間接層を増やすだけ。JOIN 関連コードは Task 7 の分割で `executor/select.rs` に集約され、見通しの問題は解消する。
- `DocOutput.data` を `own_fields` + `joined` の2層構造に分ける案(アーキテクチャ監査指摘: エイリアス接頭辞と素のフィールド名が同一名前空間) → `DocOutput` は公開 API かつ JSON 出力形式そのもの。Firestore はフィールド名に `.` を許さないため衝突は実際には起こらず、形式変更のコストが利益を大きく上回る。
- `validate_join_filter_aliases` の planner への移動 → エイリアス名が自然にスコープにあるのはパーサーであり、この検証は fireql の SQL 方言仕様(WHERE は FROM 側にのみ適用)の一部。移動はプラミングを増やすだけ。
- `FireqlError::SqlParse` の `#[from] sqlparser::ParserError` 化・`PartialFailure` の構造化(status code 保持) → 現行メッセージに行・列位置と status code が既に文字列として含まれており、CLI 用途では十分。公開エラー型に sqlparser の型を露出させるとコンシューマーが sqlparser のバージョンに結合される。
- `Format` から `clap::ValueEnum` derive を外す(ライブラリの clap 結合解消) → 同一クレート内に CLI バイナリがある限り clap はどのみちコンパイルされる。実利のある分離には clap の optional 化+ `required-features` が必要で、条件付きコンパイルの複雑さが現時点の利益(外部ライブラリ利用者の想定なし)に見合わない。YAGNI。

---

### Task 1: UPDATE の ORDER BY / LIMIT をパーサーで配線する(正しさ/安全性)

`UPDATE users SET a = 1 WHERE b = 2 ORDER BY c LIMIT 5` は sqlparser の GenericDialect で正常にパースされ、`update.order_by` / `update.limit` に格納される(検証済み)。しかし `src/sql/parser.rs` の `Statement::Update` 分岐は `order_by: vec![], limit: None` をハードコードしているため、**LIMIT 5 のつもりの UPDATE が条件一致の全ドキュメントを書き換える**。実行側(`execute_batch_write` → `build_query_params`)は DELETE 用に ORDER BY/LIMIT を既に処理できるため、パーサーの2行が唯一の欠落。

**Files:**
- Modify: `src/sql/parser.rs`(`parse_sql` 内 `Statement::Update` 分岐、54〜69行付近)
- Test: `src/sql/tests.rs`

**Interfaces:**
- Consumes: `parse_order_and_limit_from_query_parts(order_by_exprs: Option<Vec<OrderByExpr>>, limit_expr: Option<Expr>) -> Result<(Vec<OrderBy>, Option<u32>)>`(parser.rs 内の既存関数。DELETE 分岐が同じ形で使用中)
- Produces: `UpdateStatement.order_by: Vec<OrderBy>` / `UpdateStatement.limit: Option<u32>` が実際の SQL を反映するようになる(構造体定義は不変)

- [ ] **Step 1: 失敗するテストを書く**

`src/sql/tests.rs` の `update_requires_where` テストの直後に追加:

```rust
#[test]
fn parse_update_with_order_by_and_limit() {
    let stmt =
        parse_sql("UPDATE users SET status = 'active' WHERE age >= 18 ORDER BY age LIMIT 5")
            .unwrap();
    match stmt {
        StatementAst::Update(update) => {
            assert_eq!(update.order_by.len(), 1);
            assert_eq!(update.order_by[0].field, "age");
            assert!(matches!(update.order_by[0].direction, OrderDirection::Asc));
            assert_eq!(update.limit, Some(5));
        }
        _ => panic!("expected update"),
    }
}
```

`OrderDirection` が tests.rs 冒頭の `use super::*;` で見えることを確認(sql/mod.rs で定義済みなので見える)。

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test parse_update_with_order_by_and_limit`
Expected: FAIL(`assertion ... failed` — order_by が空)

- [ ] **Step 3: パーサーを配線する**

`src/sql/parser.rs` の `parse_sql` 内 `Statement::Update(update)` 分岐全体を次に置き換える:

```rust
        Statement::Update(update) => {
            let collection = parse_table_with_joins(&update.table)?;
            let filter = update
                .selection
                .map(|expr| parse_filter_expr(&expr))
                .transpose()?
                .ok_or(FireqlError::MissingWhere)?;
            let assignments = parse_assignments(update.assignments)?;
            let (order_by, limit) =
                parse_order_and_limit_from_query_parts(Some(update.order_by), update.limit)?;
            Ok(StatementAst::Update(UpdateStatement {
                collection,
                assignments,
                filter,
                order_by,
                limit,
            }))
        }
```

- [ ] **Step 4: テストが通ることを確認**

Run: `cargo test parse_update_with_order_by_and_limit`
Expected: PASS

- [ ] **Step 5: 標準検証**

Global Constraints の標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 6: コミット**

```bash
git add src/sql/parser.rs src/sql/tests.rs
git commit -m "fix: honor ORDER BY and LIMIT in UPDATE statements"
```

コミット body に1行: `Previously both clauses parsed successfully but were silently dropped, so UPDATE ... LIMIT n updated every matching document.`

---

### Task 2: sqlparser が受理し fireql が黙殺する句を明示的に拒否する(正しさ/安全性)

`parse_select` には「sqlparser が受理するが fireql は翻訳しない句を黙って落とさない」ための拒否テーブルが既にある(parser.rs の `let unsupported: &[(bool, &str)]`)。同じ原則を `parse_query`(クエリ外殻)・`Statement::Delete`・`Statement::Update`・`parse_insert_select` に適用する。現状の黙殺の実害(すべて sqlparser 0.62 GenericDialect でのパース結果を実機検証済み):

- `DELETE FROM users USING orders WHERE flag = true` → USING が無視され、**users の flag=true 全件が削除される**
- `UPDATE users SET a = 1 FROM orders WHERE ...` → FROM が無視され意図と違う範囲を更新
- `UPDATE OR IGNORE users SET ...` → 競合時の挙動指定が無視される
- `... RETURNING id` → 結果行を期待するクライアントに affected 数だけ返る
- `WITH x AS (SELECT * FROM users) SELECT * FROM x` → CTE 定義が無視され「x コレクション」を素で読む
- `SELECT * FROM users FETCH FIRST 5 ROWS ONLY` → 全件返る
- `SELECT * FROM users FOR UPDATE` → ロック指定が無視される

**Files:**
- Modify: `src/sql/parser.rs`(`parse_query` / `parse_sql` の Update・Delete 分岐 / `parse_insert_select`)
- Test: `src/sql/tests.rs`

**Interfaces:**
- Consumes: `FireqlError::Unsupported(String)`(既存エラーバリアント)
- Produces: なし(拒否のみの変更。他タスクへの影響なし)

- [ ] **Step 1: 失敗するテストを書く**

`src/sql/tests.rs` の末尾に追加:

```rust
#[test]
fn delete_using_is_rejected() {
    let err = parse_sql("DELETE FROM users USING orders WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn delete_returning_is_rejected() {
    let err = parse_sql("DELETE FROM users WHERE flag = true RETURNING id").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_from_is_rejected() {
    let err = parse_sql("UPDATE users SET a = 1 FROM orders WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_returning_is_rejected() {
    let err = parse_sql("UPDATE users SET a = 1 WHERE flag = true RETURNING id").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn update_or_conflict_clause_is_rejected() {
    let err = parse_sql("UPDATE OR IGNORE users SET a = 1 WHERE flag = true").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_with_cte_is_rejected() {
    let err = parse_sql("WITH x AS (SELECT * FROM users) SELECT * FROM x").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_fetch_is_rejected() {
    let err = parse_sql("SELECT * FROM users FETCH FIRST 5 ROWS ONLY").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}

#[test]
fn select_for_update_is_rejected() {
    let err = parse_sql("SELECT * FROM users FOR UPDATE").unwrap_err();
    assert!(matches!(err, FireqlError::Unsupported(_)));
}
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test -- delete_using_is_rejected delete_returning_is_rejected update_from_is_rejected update_returning_is_rejected update_or_conflict_clause_is_rejected select_with_cte_is_rejected select_fetch_is_rejected select_for_update_is_rejected`
Expected: 8 件すべて FAIL(`unwrap_err` がパニック — 現状は正常パースされてしまうため)

- [ ] **Step 3: parse_query にクエリ外殻の拒否テーブルを追加**

`src/sql/parser.rs` の `parse_query` の先頭(`let order_by_exprs = ...` の前)に追加:

```rust
    // Reject query-shell clauses that sqlparser accepts but fireql does not
    // translate, so they can never be silently dropped (same principle as the
    // clause table in parse_select).
    let unsupported: &[(bool, &str)] = &[
        (query.with.is_some(), "WITH (CTE)"),
        (query.fetch.is_some(), "FETCH"),
        (!query.locks.is_empty(), "FOR UPDATE/FOR SHARE"),
        (query.for_clause.is_some(), "FOR XML/JSON/BROWSE"),
        (query.settings.is_some(), "SETTINGS"),
        (query.format_clause.is_some(), "FORMAT"),
        (!query.pipe_operators.is_empty(), "Pipe operators"),
    ];
    if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
        return Err(FireqlError::Unsupported(format!(
            "{clause} is not supported"
        )));
    }
```

- [ ] **Step 4: Update / Delete 分岐に拒否テーブルを追加**

`parse_sql` 内 `Statement::Update(update)` 分岐の先頭(`let collection = ...` の前)に追加:

```rust
            let unsupported: &[(bool, &str)] = &[
                (!update.optimizer_hints.is_empty(), "optimizer hints"),
                (update.from.is_some(), "UPDATE ... FROM"),
                (update.returning.is_some(), "RETURNING"),
                (update.output.is_some(), "OUTPUT"),
                (update.or.is_some(), "UPDATE OR ..."),
            ];
            if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
                return Err(FireqlError::Unsupported(format!(
                    "{clause} is not supported"
                )));
            }
```

`Statement::Delete(delete)` 分岐の先頭(`let from = ...` の前)に追加:

```rust
            let unsupported: &[(bool, &str)] = &[
                (!delete.optimizer_hints.is_empty(), "optimizer hints"),
                (!delete.tables.is_empty(), "Multi-table DELETE"),
                (delete.using.is_some(), "USING"),
                (delete.returning.is_some(), "RETURNING"),
                (delete.output.is_some(), "OUTPUT"),
            ];
            if let Some((_, clause)) = unsupported.iter().find(|(present, _)| *present) {
                return Err(FireqlError::Unsupported(format!(
                    "{clause} is not supported"
                )));
            }
```

- [ ] **Step 5: parse_insert_select の拒否条件に output を追加**

`parse_insert_select` 冒頭の大きな `if !insert.into || ...` 条件に1行追加する。`|| insert.settings.is_some()` の行の直前に:

```rust
        || insert.output.is_some()
```

(条件全体は既存の並びのまま。`insert.output` は MSSQL の OUTPUT 句で、現状は黙殺されている。)

- [ ] **Step 6: テストが通ることを確認**

Run: `cargo test`
Expected: 全テスト PASS(Step 1 の 8 件を含む)

- [ ] **Step 7: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 8: コミット**

```bash
git add src/sql/parser.rs src/sql/tests.rs
git commit -m "fix: reject SQL clauses that were parsed but silently ignored"
```

コミット body に1行: `DELETE USING, UPDATE FROM, RETURNING, CTEs, FETCH and row locks now fail with Unsupported instead of silently changing statement semantics.`

---

### Task 3: 冗長な DELETE プリパーサーを削除する(シンプルさ)

`src/sql/rewrite.rs` の `try_parse_delete_table_function` は、古い sqlparser が `DELETE FROM collection('...')` をパースできなかった時代の回避策。sqlparser 0.62 は `DELETE FROM collection('users/u1/posts') WHERE ... ORDER BY ... LIMIT n` と `DELETE FROM collection_group('logs') WHERE ...` を `TableFactor::Table { args: Some(TableFunctionArgs { .. }) }` としてネイティブにパースし、`parse_sql` の通常の `Statement::Delete` 分岐 → `parse_table_with_joins` → `parse_table_factor` が既に両関数を処理する。**この関数と呼び出しを削除して `cargo test`(169件)・`cargo clippy -D warnings` がグリーンになることを一時 worktree で検証済み。**

注意: **INSERT 側の `try_parse_insert_collection_function` は削除しないこと。** `INSERT INTO collection('path') (cols...) SELECT ...` はネイティブではパースエラー(検証済み)、`INSERT INTO collection('path') SELECT *` は path が「カラムリスト」として誤パースされるため、文字列リライトが今も唯一の手段。`strip_keyword` / `find_matching_paren` / `parse_collection_target_expr` は INSERT 側が使うので残す。

**Files:**
- Modify: `src/sql/parser.rs`(`parse_sql` 冒頭の呼び出し 3 行を削除)
- Modify: `src/sql/rewrite.rs`(`try_parse_delete_table_function` 関数全体と未使用 import を削除)

**Interfaces:**
- Consumes: なし
- Produces: なし(パース結果は完全に同一。既存テスト `parse_delete_collection_group_with_where` / `parse_update_delete_collection_subcollection` が同一性を保証)

- [ ] **Step 1: parse_sql から呼び出しを削除**

`src/sql/parser.rs` の `parse_sql` 冒頭から次の 3 行を削除する:

```rust
    if let Some(stmt) = super::rewrite::try_parse_delete_table_function(input)? {
        return Ok(stmt);
    }
```

(直後の `try_parse_insert_collection_function` の呼び出しは残す。)

- [ ] **Step 2: rewrite.rs から関数と未使用 import を削除**

`src/sql/rewrite.rs` から `pub(super) fn try_parse_delete_table_function` 関数全体(ファイル末尾の 122〜178 行、`pub(super) fn try_parse_delete_table_function(input: &str) -> Result<Option<StatementAst>> {` から閉じ括弧まで)を削除する。

同ファイル冒頭の import を次のように変更する(`DeleteStatement` が未使用になるため):

```rust
use super::{CollectionSpec, StatementAst};
```

- [ ] **Step 3: DELETE 経路が同一に動くことを確認**

Run: `cargo test -- parse_delete_collection_group_with_where parse_update_delete_collection_subcollection delete_collection_group_requires_where`
Expected: 3 件 PASS

- [ ] **Step 4: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること(未使用関数・import が残っていれば clippy `-D warnings` が落ちるので、落ちたら Step 2 の削除漏れを確認)。

- [ ] **Step 5: (任意)エミュレーター e2e**

エミュレーターが使える環境であれば:

```bash
FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator cargo test --test emulator
```

Expected: `test result: ok.`

- [ ] **Step 6: コミット**

```bash
git add src/sql/parser.rs src/sql/rewrite.rs
git commit -m "refactor: drop DELETE pre-parser now that sqlparser handles table functions"
```

---

### Task 4: format.rs の Aggregation 出力の重複を解消する(シンプルさ)

`format_csv` と `format_table` の `FireqlOutput::Aggregation` 分岐が「キーを集めてソート → 値を `to_plain_string()` で文字列化」という同一ロジックを重複して持っている。ヘルパーに抽出して drift を防ぐ。

**Files:**
- Modify: `src/format.rs`

**Interfaces:**
- Consumes: `FireqlValue::to_plain_string(&self) -> String`(既存)
- Produces: `fn aggregation_row(map: &HashMap<String, FireqlValue>) -> (Vec<&String>, Vec<String>)`(format.rs 内 private。ソート済みキーと対応する値文字列)

- [ ] **Step 1: ヘルパーを追加**

`src/format.rs` の `collect_field_names` 関数の直後に追加する。ファイル冒頭に `use std::collections::HashMap;` が無ければ追加(現状は `std::collections::BTreeSet` をフルパスで使っているため、`use` は関数内でなくファイル冒頭に置く):

```rust
fn aggregation_row(map: &HashMap<String, FireqlValue>) -> (Vec<&String>, Vec<String>) {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();
    let values = keys.iter().map(|k| map[*k].to_plain_string()).collect();
    (keys, values)
}
```

`use std::collections::HashMap;` を `use crate::value::FireqlValue;` の下に追加する。

- [ ] **Step 2: format_csv の Aggregation 分岐を置き換える**

`format_csv` の `FireqlOutput::Aggregation(map)` 分岐全体を次に置き換える:

```rust
        FireqlOutput::Aggregation(map) => {
            if map.is_empty() {
                return Ok(String::new());
            }
            let (keys, values) = aggregation_row(map);
            wtr.write_record(keys.iter().map(|k| k.as_str()))
                .map_err(csv_error)?;
            wtr.write_record(&values).map_err(csv_error)?;
        }
```

- [ ] **Step 3: format_table の Aggregation 分岐を置き換える**

`format_table` の `FireqlOutput::Aggregation(map)` 分岐全体を次に置き換える:

```rust
        FireqlOutput::Aggregation(map) => {
            if map.is_empty() {
                return Ok(String::new());
            }
            let (keys, values) = aggregation_row(map);
            let mut table = Table::new();
            table.load_preset(ASCII_FULL);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(keys.iter().map(|k| strip_control_chars(k)));
            table.add_row(values.iter().map(|v| strip_control_chars(v)));
            Ok(table.to_string())
        }
```

- [ ] **Step 4: 既存テストで確認**

Run: `cargo test --lib format`
Expected: `csv_aggregation` / `table_aggregation` / `csv_empty_aggregation` / `table_empty_aggregation` を含む全 format テスト PASS

- [ ] **Step 5: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 6: コミット**

```bash
git add src/format.rs
git commit -m "refactor: extract shared aggregation row formatting"
```

---

### Task 5: センチネル JSON オブジェクトを型付き `SqlValue` enum に置き換える(シンプルさ/型安全)

`ref('path')` / `timestamp('...')` / `CURRENT_TIMESTAMP` は現在、`JsonValue::Object` にマジックキー(`__fireql_ref` / `__fireql_ts` / `__fireql_current_ts`)を入れた「センチネルオブジェクト」で表現され、parser(生成)・planner(キー探索で判別)・executor(キー存在チェック)の3モジュールがキー文字列の暗黙の合意で結合している。これを enum `SqlValue` に置き換えると:

- 定数3つ(`FIREQL_REF_KEY` 等)、`sentinel_object()`、`is_current_timestamp_value()`、planner のキー探索が消える
- 「参照でもタイムスタンプでもある」ような不正状態が型で表現不能になる
- `timestamp('...')` の RFC3339 検証がパース時に前倒しされる(ユーザー体験は同一: どちらも実行前にエラー)

**挙動は完全に保存される**(生成される Firestore クエリ/Write のワイヤー表現は同一)。

**Files:**
- Modify: `src/sql/mod.rs`(`SqlValue` 追加、`FilterExpr`/`UpdateStatement` の型変更、定数削除)
- Modify: `src/sql/parser.rs`(値パース系関数の戻り型変更、`sentinel_object` 削除)
- Modify: `src/planner.rs`(変換関数の引数型変更・改名、キー探索削除)
- Modify: `src/executor.rs`(`is_current_timestamp_value` 削除、`SqlValue` でのマッチ)
- Test: `src/sql/tests.rs` / `src/planner.rs` 内テスト / `src/executor.rs` 内テスト

**Interfaces:**
- Produces(このタスク内で全箇所を同時に更新する。外部 API への影響なし):
  - `pub enum SqlValue { Literal(JsonValue), Reference(String), Timestamp(DateTime<Utc>), CurrentTimestamp }`(`src/sql/mod.rs`、`Debug + Clone + PartialEq` derive)
  - `FilterExpr::{Compare, ArrayContains}.value: SqlValue`、`FilterExpr::{ArrayContainsAny, InList}.values: Vec<SqlValue>`
  - `UpdateStatement.assignments: Vec<(String, SqlValue)>`
  - `planner::sql_value_to_firestore(value: &SqlValue, documents_path: Option<&str>) -> Result<FirestoreValue>`(旧 `json_to_firestore_value_with_context`)
  - `planner::sql_values_to_firestore_array(values: &[SqlValue], documents_path: Option<&str>) -> Result<FirestoreValue>`(旧 `json_array_to_firestore_value_with_context`)

- [ ] **Step 1: sql/mod.rs に SqlValue を追加し型を差し替える**

`src/sql/mod.rs` に対して:

1. `use serde_json::Value as JsonValue;` の下に `use chrono::{DateTime, Utc};` を追加。
2. 定数3行(`FIREQL_REF_KEY` / `FIREQL_TS_KEY` / `FIREQL_CURRENT_TS_KEY`)を削除。
3. `Projection` enum の直後に追加:

```rust
/// SQL 値式の解析結果。Firestore 固有のリテラル(参照・タイムスタンプ)を
/// JSON センチネルではなく型で表現し、不正状態を表現不能にする。
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Literal(JsonValue),
    Reference(String),
    Timestamp(DateTime<Utc>),
    CurrentTimestamp,
}
```

4. `UpdateStatement` の `assignments` を変更:

```rust
    pub assignments: Vec<(String, SqlValue)>,
```

5. `FilterExpr` を次に置き換える:

```rust
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Compare {
        field: String,
        op: CompareOp,
        value: SqlValue,
    },
    ArrayContains {
        field: String,
        value: SqlValue,
    },
    ArrayContainsAny {
        field: String,
        values: Vec<SqlValue>,
    },
    InList {
        field: String,
        values: Vec<SqlValue>,
        negated: bool,
    },
    Unary {
        field: String,
        op: UnaryOp,
    },
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}
```

- [ ] **Step 2: parser.rs の値パースを SqlValue 化する**

`src/sql/parser.rs` に対して:

1. ファイル冒頭の `use super::{...}` から `FIREQL_CURRENT_TS_KEY, FIREQL_REF_KEY, FIREQL_TS_KEY` を外し、`SqlValue` を追加。
2. `use chrono::{DateTime, Utc};` を追加。
3. `sentinel_object` 関数(冒頭 17〜19 行)を削除。
4. `parse_value_expr` を次に置き換える:

```rust
fn parse_value_expr(expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Value(vws) => Ok(SqlValue::Literal(parse_value(&vws.value)?)),
        Expr::Function(function) => parse_value_function(function),
        Expr::Identifier(ident) => {
            if ident.value.eq_ignore_ascii_case("current_timestamp") {
                Ok(SqlValue::CurrentTimestamp)
            } else {
                Err(FireqlError::Unsupported(format!(
                    "Unsupported identifier in value expression: {ident}"
                )))
            }
        }
        Expr::UnaryOp { op, expr } => match op {
            sqlparser::ast::UnaryOperator::Minus => match &**expr {
                Expr::Value(vws) => match &vws.value {
                    Value::Number(num, _) => {
                        let with_sign = format!("-{num}");
                        Ok(SqlValue::Literal(parse_numeric(&with_sign)?))
                    }
                    _ => Err(FireqlError::Unsupported(
                        "Unary minus only supported for numeric literals".to_string(),
                    )),
                },
                _ => Err(FireqlError::Unsupported(
                    "Unary minus only supported for numeric literals".to_string(),
                )),
            },
            _ => Err(FireqlError::Unsupported(
                "Only unary minus is supported for values".to_string(),
            )),
        },
        other => Err(FireqlError::Unsupported(format!(
            "Unsupported value expression: {other}"
        ))),
    }
}
```

(`parse_value` / `parse_numeric` は `JsonValue` を返したまま変えない。)

5. `parse_value_function` を次に置き換える:

```rust
fn parse_value_function(function: &sqlparser::ast::Function) -> Result<SqlValue> {
    let name = object_name_to_string(&function.name);
    let name_lower = name.to_ascii_lowercase();
    let args = parse_function_args(&function.args)?;

    match name_lower.as_str() {
        "ref" | "reference" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "ref(path) expects exactly one argument".to_string(),
                ));
            }
            let path = expr_to_string_literal(&args[0], "ref(path)")?;
            Ok(SqlValue::Reference(path))
        }
        "timestamp" => {
            if args.len() != 1 {
                return Err(FireqlError::Unsupported(
                    "timestamp(value) expects exactly one argument".to_string(),
                ));
            }
            let value = expr_to_string_literal(&args[0], "timestamp(value)")?;
            let parsed = DateTime::parse_from_rfc3339(&value)
                .map_err(|e| FireqlError::InvalidQuery(format!("Invalid timestamp: {e}")))?;
            Ok(SqlValue::Timestamp(parsed.with_timezone(&Utc)))
        }
        "current_timestamp" => {
            if !args.is_empty() {
                return Err(FireqlError::Unsupported(
                    "CURRENT_TIMESTAMP expects no arguments".to_string(),
                ));
            }
            Ok(SqlValue::CurrentTimestamp)
        }
        _ => Err(FireqlError::Unsupported(format!(
            "Unsupported function in value expression: {name}"
        ))),
    }
}
```

6. `parse_value_list_expr` の戻り型を `Result<Vec<SqlValue>>` に変更(本体は同一、`parse_value_expr` の戻り型変更に追随するだけ)。
7. `parse_assignments` の戻り型を `Result<Vec<(String, SqlValue)>>` に変更(本体は同一)。
8. `use serde_json::Value as JsonValue;` は `parse_value`/`parse_numeric` が使うので残す。

- [ ] **Step 3: planner.rs の変換を SqlValue 化する**

`src/planner.rs` に対して:

1. `use crate::sql::{...}` から `FIREQL_CURRENT_TS_KEY, FIREQL_REF_KEY, FIREQL_TS_KEY` を外し、`SqlValue` を追加。
2. `json_to_firestore_value_with_context` を次に置き換える(改名に注意):

```rust
pub(crate) fn sql_value_to_firestore(
    value: &SqlValue,
    documents_path: Option<&str>,
) -> Result<FirestoreValue> {
    match value {
        SqlValue::Literal(json) => Ok(json.clone().into()),
        SqlValue::Reference(path) => {
            let full = expand_reference_path(path, documents_path)?;
            Ok(FirestoreReference(full).into())
        }
        SqlValue::Timestamp(ts) => Ok(FirestoreTimestamp(*ts).into()),
        SqlValue::CurrentTimestamp => Ok(FirestoreTimestamp(Utc::now()).into()),
    }
}
```

3. `json_array_to_firestore_value_with_context` を次に置き換える(改名に注意):

```rust
pub(crate) fn sql_values_to_firestore_array(
    values: &[SqlValue],
    documents_path: Option<&str>,
) -> Result<FirestoreValue> {
    let mut array_values = Vec::with_capacity(values.len());
    for value in values {
        let fv = sql_value_to_firestore(value, documents_path)?;
        array_values.push(fv.value);
    }
    Ok(FirestoreValue::from(
        gcloud_sdk::google::firestore::v1::Value {
            value_type: Some(
                gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(
                    gcloud_sdk::google::firestore::v1::ArrayValue {
                        values: array_values,
                    },
                ),
            ),
        },
    ))
}
```

4. `build_filter` / `compare_op_to_firestore` 内の旧関数名の呼び出しを新名に置き換え、`compare_op_to_firestore` の引数を `value: &SqlValue` に変更(本体のマッチは不変)。
5. `use chrono::{DateTime, Utc};` の `DateTime` が未使用になったら `use chrono::Utc;` に変更(clippy が教えてくれる)。`use serde_json::Value as JsonValue;` はテストでのみ必要になるため、未使用警告が出たら `#[cfg(test)]` 側の `use serde_json::json;` などテスト内 import に寄せる。

- [ ] **Step 4: executor.rs を SqlValue 化する**

`src/executor.rs` に対して:

1. `use crate::planner::{...}` を `build_aggregated_query_params, build_query_params, sql_value_to_firestore` に変更。
2. `use crate::sql::{...}` から `FIREQL_CURRENT_TS_KEY` を外し、`SqlValue` を追加。
3. `is_current_timestamp_value` 関数を削除。
4. `build_update_parts` のループ本体を次に置き換える(シグネチャは `assignments: &[(String, SqlValue)]` に変更):

```rust
fn build_update_parts(
    assignments: &[(String, SqlValue)],
    base_doc_path: Option<&str>,
) -> Result<UpdateParts> {
    let mut update_mask_fields = Vec::with_capacity(assignments.len());
    let mut fields = Vec::with_capacity(assignments.len());
    let mut transforms = Vec::new();

    for (field, value) in assignments {
        if matches!(value, SqlValue::CurrentTimestamp) {
            transforms.push(document_transform::FieldTransform {
                field_path: field.clone(),
                transform_type: Some(
                    document_transform::field_transform::TransformType::SetToServerValue(
                        document_transform::field_transform::ServerValue::RequestTime as i32,
                    ),
                ),
            });
            continue;
        }

        let fv = sql_value_to_firestore(value, base_doc_path)?;
        fields.push((field.clone(), fv));
        update_mask_fields.push(field.clone());
    }

    Ok(UpdateParts {
        update_mask_fields,
        fields,
        transforms,
    })
}
```

5. `execute_join_select` 内の `in_values` 構築を次に置き換える(型が `Vec<SqlValue>` になる):

```rust
            let in_values: Vec<SqlValue> = if join.right_field == "__name__" {
                chunk
                    .iter()
                    .map(|k| match k {
                        crate::joiner::JoinKey::String(s) => SqlValue::Literal(
                            serde_json::Value::String(format!("{doc_path}/{s}")),
                        ),
                        _ => SqlValue::Literal(k.to_json_value()),
                    })
                    .collect()
            } else {
                chunk
                    .iter()
                    .map(|k| SqlValue::Literal(k.to_json_value()))
                    .collect()
            };
```

6. `strip_alias_from_filter` は変更不要(`value.clone()` が `SqlValue: Clone` でそのまま動く)。
7. `use serde_json::Value as JsonValue;` が未使用になったら削除(テスト側で使うなら `#[cfg(test)]` 内 import へ)。

- [ ] **Step 5: コンパイルを通す**

Run: `cargo build 2>&1 | head -50`

残りの型エラーはすべて「`JsonValue` を渡している箇所を `SqlValue::Literal(...)` で包む」か「センチネル構築を `SqlValue::CurrentTimestamp` 等の直接構築に変える」かのどちらかで機械的に解消できる。エラーが出なくなるまで繰り返す。プロダクションコードで上記以外の種類の変更が必要になった場合は手を止めてプランの想定漏れとして報告すること。

- [ ] **Step 6: テストを更新する**

コンパイルエラー・アサーション失敗が出るテストを以下の規則で書き換える:

1. `src/sql/tests.rs` — センチネルのキー探索を enum の等値比較に変える。4 テストの match 腕の中身を置き換え:

```rust
// parse_ref_value:
FilterExpr::Compare { value, .. } => {
    assert_eq!(value, SqlValue::Reference("users/user1".to_string()));
}
// parse_timestamp_value:
FilterExpr::Compare { value, .. } => {
    let expected = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert_eq!(value, SqlValue::Timestamp(expected));
}
// parse_current_timestamp_value:
FilterExpr::Compare { value, .. } => {
    assert_eq!(value, SqlValue::CurrentTimestamp);
}
// parse_update_with_current_timestamp_assignment:
let (field, value) = &update.assignments[0];
assert_eq!(field, "updated_at");
assert_eq!(value, &SqlValue::CurrentTimestamp);
```

2. `src/sql/tests.rs` のその他のテストで `FilterExpr` の値を検査している箇所は `JsonValue` → `SqlValue::Literal(JsonValue)` に包む。
3. `src/planner.rs` のテスト — `FilterExpr` 構築の `value: JsonValue::from(10)` を `value: SqlValue::Literal(JsonValue::from(10))` に、`values: (0..31).map(JsonValue::from).collect()` を `values: (0..31).map(|v| SqlValue::Literal(JsonValue::from(v))).collect()` に、という規則で全テストを機械的に更新。センチネル構築テスト 5 件(`reference_value_expands_relative_path` 等)は `SqlValue::Reference("users/u1".to_string())` / `SqlValue::Timestamp(...)` / `SqlValue::CurrentTimestamp` の直接構築に変え、呼び出しを `sql_value_to_firestore` に改名。
4. `src/executor.rs` のテスト — `update_parts_turn_current_timestamp_into_server_timestamp_transform` / `update_parts_mix_normal_fields_and_server_timestamp_transform` の assignments 構築を:

```rust
let assignments = vec![("updated_at".to_string(), SqlValue::CurrentTimestamp)];
// mix の方:
let assignments = vec![
    (
        "status".to_string(),
        SqlValue::Literal(JsonValue::String("active".to_string())),
    ),
    ("updated_at".to_string(), SqlValue::CurrentTimestamp),
];
```

- [ ] **Step 7: 全テスト実行**

Run: `cargo test`
Expected: 全 PASS。ユニットテスト数が 169 件から減っていないこと(テストの削除は禁止。書き換えのみ)。

- [ ] **Step 8: (推奨)エミュレーター e2e**

ワイヤー表現の同一性を確認するため、エミュレーターが使える環境であれば必ず:

```bash
FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator cargo test --test emulator
```

Expected: `test result: ok. 9 passed`

- [ ] **Step 9: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 10: コミット**

```bash
git add src/sql/mod.rs src/sql/parser.rs src/sql/tests.rs src/planner.rs src/executor.rs
git commit -m "refactor: replace sentinel JSON objects with typed SqlValue enum"
```

コミット body に2行:
```
ref()/timestamp()/CURRENT_TIMESTAMP were passed through FilterExpr and
assignments as JSON objects keyed by magic __fireql_* strings. A typed
enum makes the invalid states unrepresentable and moves RFC3339
validation to parse time.
```

---

### Task 6: JOIN キー型エラーの誤分類を直す(エラー品質)

`joiner.rs` の `extract_join_keys` / `hash_join` は `Result<_, String>` を返し、executor が一律 `FireqlError::Unsupported` に包む。その結果、JOIN キーに Double や Array を持つ**ドキュメントデータ起因**の実行時エラーが `Unsupported SQL: unsupported Firestore value type used as join key: Double(1.5)` と表示され、ユーザーは SQL の修正に誘導される。専用エラーバリアントを設け、joiner が `FireqlError` を直接返すようにする。

**Files:**
- Modify: `src/error.rs`(バリアント追加)
- Modify: `src/joiner.rs`(`doc_key` / `extract_join_keys` / `hash_join` の戻り型)
- Modify: `src/executor.rs`(`.map_err(FireqlError::Unsupported)` 2箇所を削除)

**Interfaces:**
- Produces: `FireqlError::UnsupportedJoinKey { field: String, reason: String }`
- Produces: `joiner::extract_join_keys(docs: &[DocOutput], field: &str) -> crate::error::Result<Vec<JoinKey>>`、`joiner::hash_join(left, right, params) -> crate::error::Result<Vec<DocOutput>>`(`String` エラーから変更。`JoinKey::from_fireql_value` は `Result<Self, String>` のまま)

- [ ] **Step 1: 失敗するテストを書く**

`src/joiner.rs` のテストモジュール末尾に追加:

```rust
    #[test]
    fn hash_join_unsupported_key_type_reports_join_key_error() {
        let left = vec![make_doc("u1", vec![("score", FireqlValue::Double(1.5))])];
        let right = vec![make_doc("d1", vec![])];
        let err = hash_join(
            &left,
            &right,
            &jp("score", "__name__", JoinType::Inner, "l", "r", true),
        )
        .unwrap_err();
        match err {
            crate::error::FireqlError::UnsupportedJoinKey { field, .. } => {
                assert_eq!(field, "score");
            }
            other => panic!("expected UnsupportedJoinKey, got {other:?}"),
        }
    }
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `cargo test hash_join_unsupported_key_type_reports_join_key_error`
Expected: コンパイルエラー(`UnsupportedJoinKey` バリアントが存在しない / エラー型が `String`)。これが RED に相当する。

- [ ] **Step 3: エラーバリアントを追加**

`src/error.rs` の `InvalidQuery` バリアントの直後に追加:

```rust
    #[error("Cannot join on field `{field}`: {reason}")]
    UnsupportedJoinKey { field: String, reason: String },
```

- [ ] **Step 4: joiner.rs の戻り型を変更**

`src/joiner.rs` の冒頭に import を追加:

```rust
use crate::error::{FireqlError, Result};
```

`doc_key` を次に置き換える:

```rust
fn doc_key(doc: &DocOutput, field: &str) -> Result<JoinKey> {
    if field == "__name__" {
        Ok(JoinKey::String(doc.id.clone()))
    } else {
        match doc.data.get(field) {
            Some(v) => {
                JoinKey::from_fireql_value(v).map_err(|reason| FireqlError::UnsupportedJoinKey {
                    field: field.to_string(),
                    reason,
                })
            }
            None => Ok(JoinKey::Null),
        }
    }
}
```

`extract_join_keys` のシグネチャを `pub fn extract_join_keys(docs: &[DocOutput], field: &str) -> Result<Vec<JoinKey>>` に、`hash_join` のシグネチャを `pub fn hash_join(left_docs: &[DocOutput], right_docs: &[DocOutput], params: &JoinParams<'_>) -> Result<Vec<DocOutput>>` に変更する(本体は不変。`std::result::Result<_, String>` からの変更)。

`JoinKey::from_fireql_value` と `to_json_value` は変更しない(`from_fireql_value` の `Result<Self, String>` は「型名の説明文」を返す内部ヘルパーとして残す)。

- [ ] **Step 5: executor.rs の包み直しを削除**

`src/executor.rs` の `execute_join_select` 内、次の2箇所から `.map_err(FireqlError::Unsupported)` を削除する:

```rust
        let keys = extract_join_keys(&current_result, &effective_left_field)?;
```

```rust
        current_result = hash_join(
            &current_result,
            &right_docs,
            &JoinParams {
                left_field: &effective_left_field,
                right_field: &join.right_field,
                join_type: join.join_type,
                left_prefix: left_alias,
                right_prefix,
                prefix_left: !is_joined,
            },
        )?;
```

- [ ] **Step 6: テストが通ることを確認**

Run: `cargo test`
Expected: 全 PASS(既存の `join_key_from_unsupported_type_returns_error` は `from_fireql_value` を直接呼ぶため変更不要で、そのまま通る)

- [ ] **Step 7: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 8: コミット**

```bash
git add src/error.rs src/joiner.rs src/executor.rs
git commit -m "fix: report join key type errors as data errors, not unsupported SQL"
```

---

### Task 7: executor.rs をサブモジュールに分割する(シンプルさ)

`src/executor.rs`(1,010行)は SELECT/JOIN 実行・INSERT SELECT・バッチ書き込み・Firestore ドキュメント名パースという独立した責務を1ファイルに抱えている。第1ラウンドで `sql.rs` を同じ理由で分割した前例に従い、**コードを一切変更せず移動だけで**サブモジュールに分割する。

**移動ルール(このタスクの全体規則):**
- 関数・型・定数・テストの**本体は1文字も変更しない**。変更してよいのは `use` 文・可視性(`fn` → `pub(super) fn` 等)・`mod` 宣言のみ。
- 兄弟モジュールから参照される項目は `pub(super)` にする。`crate::` 側(lib.rs)から見える必要があるのは `execute` のみ(現状どおり `pub`)。
- テストは対象関数と同じファイルの `#[cfg(test)] mod tests` に移す。テストヘルパー(`document`/`string_value`/`integer_value`/`join_spec`/`status`)は使うテストと同じファイルへ。
- import の解決は `cargo clippy --all-targets --all-features -- -D warnings` が通るまで機械的に行う(未使用 import は削除)。

**Files:**
- Delete: `src/executor.rs`
- Create: `src/executor/mod.rs` / `src/executor/select.rs` / `src/executor/insert_select.rs` / `src/executor/batch.rs` / `src/executor/doc_name.rs`

**Interfaces:**
- Consumes: 既存の全項目(シグネチャ不変)
- Produces: `executor::execute` のパスは不変(`lib.rs` の `mod executor;` はディレクトリモジュールをそのまま解決する)

**移動マッピング(完全な対応表):**

| 現 executor.rs の項目 | 移動先 |
|---|---|
| `execute`(dispatch 関数) | `mod.rs` |
| `BATCH_LIMIT` 定数, `into_batches`, `FireqlWrite`, `BatchOp`, `UpdateParts`, `build_update_parts`, `count_batch_outcome`, `drain_batch_results`, `execute_batch_write` | `batch.rs` |
| `FIRESTORE_IN_LIMIT` 定数, `execute_select`, `strip_alias_from_filter`, `effective_left_join_field`, `execute_join_select` | `select.rs` |
| `execute_insert_select`, `insert_select_query_projection`, `insert_parent_path`, `InsertSelectParts`, `build_insert_select_parts`, `generate_document_id` | `insert_select.rs` |
| `DocNameParts`, `parse_doc_name`, `docs_to_output`, `doc_to_output` | `doc_name.rs` |
| テスト: `update_parts_*`, `batch_outcome_*`, `drain_batch_results_*`, ヘルパー `status` | `batch.rs` |
| テスト: `insert_select_parts_*`, ヘルパー `document`/`string_value`/`integer_value` | `insert_select.rs` |
| テスト: `effective_left_field_*`, ヘルパー `join_spec` | `select.rs` |

- [ ] **Step 1: ディレクトリとスケルトンを作る**

`src/executor.rs` を `git mv src/executor.rs src/executor/mod.rs` で移動し、`src/executor/{select,insert_select,batch,doc_name}.rs` を空で作成。`mod.rs` の冒頭(既存 import の上)に追加:

```rust
mod batch;
mod doc_name;
mod insert_select;
mod select;
```

- [ ] **Step 2: マッピング表に従って項目を移動**

各項目を対応表どおりのファイルへ**verbatim で**移動する。兄弟モジュールから使われる項目に `pub(super)` を付ける(対象: `into_batches` / `FireqlWrite` / `BatchOp` / `build_update_parts` / `count_batch_outcome` / `drain_batch_results` / `execute_batch_write` / `execute_select` / `execute_insert_select` / `docs_to_output` / `parse_doc_name` / `DocNameParts` とそのフィールド・メソッド)。各ファイルの `use` 文は元の executor.rs のものから必要な分をコピーし、モジュール間参照は `use super::batch::into_batches;` の形式にする。

- [ ] **Step 3: コンパイルと import 整理**

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: 未使用 import・可視性エラーを潰しきって警告ゼロ。**関数本体のコード変更が必要になった場合は手を止めてプランの想定漏れとして報告すること。**

- [ ] **Step 4: テスト実行**

Run: `cargo test`
Expected: 全 PASS。テスト数が移動前と同数であること(`cargo test 2>&1 | rg "test result"` の合計が一致)。

- [ ] **Step 5: 標準検証**

標準検証 3 コマンドを実行し、すべて成功すること。

- [ ] **Step 6: コミット**

```bash
git add -A src/executor/ && git status --short
git commit -m "refactor: split executor into select/insert_select/batch/doc_name modules"
```

(`git mv` 済みのため `src/executor.rs` の削除はステージ済み。`git status --short` で `R` または `D`+`A` の組み合わせだけが表示されることを確認してからコミットする。)

---

## 完了後の最終確認

- [ ] `git log --oneline main..HEAD` → 7 コミット(Task 1〜7 が各1つ)
- [ ] `cargo fmt --all -- --check` / `cargo clippy --all-targets --all-features -- -D warnings` / `cargo test` すべて成功
- [ ] `rg -n "FIREQL_REF_KEY|FIREQL_TS_KEY|FIREQL_CURRENT_TS_KEY|sentinel_object|is_current_timestamp_value|try_parse_delete_table_function" src/` → 出力なし(終了コード 1)
- [ ] エミュレーターが使えるなら `FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator cargo test --test emulator --test e2e_seed` → すべて `ok`
- [ ] **push はしない。** ブランチをローカルに残した状態で作業終了を報告する。
