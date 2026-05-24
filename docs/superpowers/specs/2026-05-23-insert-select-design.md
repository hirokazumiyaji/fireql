# INSERT SELECT Copy Design

## Goal

Add a small, Firestore-friendly copy operation that lets users copy documents selected by a query into another collection.

The feature starts with safe auto-ID copies, then extends to document ID preservation through `__name__`.

## Non-Goals

- `UPSERT`, merge, or overwrite behavior.
- `INSERT ... VALUES`.
- Copying aggregation results.
- Client-side transformation expressions.
- Cross-project or cross-database copies.

## Phase 1: Auto-ID Copy

Supported form:

```sql
INSERT INTO archived_users
SELECT * FROM users WHERE disabled = true;
```

Also supported:

```sql
INSERT INTO collection('users/u1/archive')
SELECT * FROM collection('users/u1/posts') WHERE published = false;
```

Rules:

- The destination must be a normal collection target: `<collection>` or `collection('path')`.
- The source must be a normal collection target: `<collection>` or `collection('path')`.
- `SELECT *` is required.
- `WHERE`, `ORDER BY`, and `LIMIT` follow existing `SELECT` query behavior.
- Destination document IDs are generated automatically by Firestore.
- `collection_group()` sources are unsupported in Phase 1.
- `JOIN` and aggregation are unsupported.
- Output is the existing affected-write shape: `{ "affected": n }`.

Data flow:

1. Parse `INSERT INTO <target> SELECT * FROM <source> ...` into a new statement variant.
2. Build the source query using the existing query planner.
3. Read matching source documents.
4. Convert each source document field map into a destination document with an auto-generated ID.
5. Write documents in batches using the existing batch size and parallelism model.

Error handling:

- Unsupported source shapes return `Unsupported`.
- Firestore write failures use the existing partial failure behavior.
- Empty query results return `{ "affected": 0 }`.

## Phase 2: ID-Preserving Copy

Supported form:

```sql
INSERT INTO archived_users (__name__, name, age)
SELECT __name__, name, age FROM users WHERE disabled = true;
```

Rules:

- `__name__` in the destination column list means the copied row supplies the destination document ID.
- `__name__` must also appear in the SELECT projection when used as a destination column.
- Destination writes use create semantics: an existing destination document is an error.
- Without `__name__`, explicit destination columns still use auto IDs.
- Source and destination column counts must match.

This phase should reuse the Phase 1 read and batch-write path, with an additional projection-to-field mapping step.

## Tests

Unit tests:

- Parse Phase 1 `INSERT INTO target SELECT * FROM source WHERE ...`.
- Reject `INSERT INTO target SELECT COUNT(*) FROM source`.
- Reject `INSERT INTO target SELECT * FROM collection_group('items')`.
- Parse Phase 2 column lists and `__name__` once Phase 2 is implemented.

Planner/executor tests:

- Build and execute an auto-ID copy against the emulator.
- Verify copied field values preserve Firestore value types.
- Verify empty source query returns `affected: 0`.
- Verify Phase 2 ID preservation creates destination documents with source IDs.
- Verify Phase 2 duplicate destination IDs fail.

## Rollout

Implement Phase 1 first and document it in README and usage docs. Implement Phase 2 after Phase 1 behavior is covered by parser and emulator tests.
