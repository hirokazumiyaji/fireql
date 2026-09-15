# Repository Guidelines

## Project Structure & Module Organization

Fireql is a Rust 2021 CLI and library for querying Google Cloud Firestore with SQL. `src/main.rs` implements the CLI; `src/lib.rs` exposes the library API. SQL parsing and rewriting live in `src/sql/`, planning in `src/planner.rs` and `src/planner/`, and query execution in `src/executor/`. Supporting modules handle joins, values, and output formatting.

Integration tests live in `tests/`; reusable emulator data lives in `fixtures/emulator-e2e.json`. The seeding binary is `src/bin/fireql-emulator-seed.rs`. English is the primary documentation language in `README.md` and `docs/USAGE.md`; maintain their Japanese translations in `README_ja.md` and `docs/USAGE_ja.md` when changing documented behavior.

## Build, Test, and Development Commands

- `mise install`: install the pinned Rust toolchain and Java required by the emulator.
- `cargo build --release`: build optimized binaries.
- `cargo run --bin fireql -- --help`: inspect local CLI options.
- `cargo fmt --all -- --check`: verify formatting; use `cargo fmt --all` to apply it.
- `cargo clippy --all-targets --all-features -- -D warnings`: run CI lint checks.
- `cargo test --all`: run the full test suite.

## Coding Style & Naming Conventions

Use rustfmt defaults, including four-space indentation. Use `snake_case` for functions and modules, `PascalCase` for types, and `SCREAMING_SNAKE_CASE` for constants. Prefer simple, focused changes and existing abstractions. Avoid speculative compatibility paths and defensive checks for states excluded by established invariants. Comments should explain intent rather than restate code.

## Testing Guidelines

Use Rust's built-in tests and `#[tokio::test]` for asynchronous cases. Keep unit tests alongside their modules and name tests after the behavior they verify. Supply realistic dependencies or simple in-memory implementations.

Emulator tests skip when `FIRESTORE_EMULATOR_HOST` is unset. Start the emulator with `gcloud beta emulators firestore start --host-port=localhost:8080`, then run:

```sh
FIRESTORE_EMULATOR_HOST=localhost:8080 FIRESTORE_PROJECT_ID=fireql-emulator \
  GOOGLE_CLOUD_PROJECT=fireql-emulator cargo test --all
```

CI runs formatting, Clippy, and emulator-backed tests. No numeric coverage threshold is configured.

## Commit & Pull Request Guidelines

Follow existing prefixes such as `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`, and `build(deps):`. Keep subjects concise and omit AI attribution trailers. Describe behavior changes, relevant issues, and validation in pull requests. Run CI checks before requesting review. Use `gh` for GitHub operations.

## Security & Configuration

Never commit credentials. Do not inspect `.env*`, `.envrc`, or credential files without explicit authorization. Use the emulator for development tests and consult the usage documentation for authentication configuration.
