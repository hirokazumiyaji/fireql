# Reference Value Relative Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change Reference field JSON serialization to output collection-relative paths instead of full Firestore resource paths.

**Architecture:** Add a helper function `to_relative_path()` to extract the path after `/documents/` from a full Firestore reference path. Use it in the `Serialize` impl for `FireqlValue::Reference`. Internal `Reference(String)` continues to store the full path.

**Tech Stack:** Rust, serde, serde_json (for tests)

---

### Task 1: Add tests for reference path extraction helper

**Files:**
- Modify: `src/value.rs` (add `#[cfg(test)] mod tests` at bottom)

- [ ] **Step 1: Write failing tests for `to_relative_path` helper**

Add a test module at the end of `src/value.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_relative_path_normal() {
        let full = "projects/p/databases/(default)/documents/users/u1";
        assert_eq!(to_relative_path(full), "users/u1");
    }

    #[test]
    fn test_to_relative_path_nested_collection() {
        let full = "projects/p/databases/(default)/documents/users/u1/posts/p1";
        assert_eq!(to_relative_path(full), "users/u1/posts/p1");
    }

    #[test]
    fn test_to_relative_path_no_documents_prefix() {
        let path = "some/other/path";
        assert_eq!(to_relative_path(path), "some/other/path");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib value::tests -- --nocapture`
Expected: FAIL — `to_relative_path` is not defined.

- [ ] **Step 3: Implement `to_relative_path`**

Add this function in `src/value.rs` (above the `Serialize` impl, after the `from_document_fields` method's closing brace on line 59):

```rust
fn to_relative_path(full_path: &str) -> &str {
    const MARKER: &str = "/documents/";
    match full_path.find(MARKER) {
        Some(pos) => &full_path[pos + MARKER.len()..],
        None => full_path,
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib value::tests -- --nocapture`
Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/value.rs
git commit -m "feat: add to_relative_path helper for reference display"
```

---

### Task 2: Update Reference serialization and add serialization tests

**Files:**
- Modify: `src/value.rs:111-115` (Reference serialization)
- Modify: `src/value.rs` (add serialization tests to existing test module)

- [ ] **Step 1: Write failing serialization tests**

Add to the existing `tests` module in `src/value.rs`:

```rust
    #[test]
    fn test_serialize_reference_normal() {
        let val = FireqlValue::Reference(
            "projects/p/databases/(default)/documents/users/u1".to_string(),
        );
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["_firestore_type"], "reference");
        assert_eq!(json["value"], "users/u1");
    }

    #[test]
    fn test_serialize_reference_nested_collection() {
        let val = FireqlValue::Reference(
            "projects/p/databases/(default)/documents/users/u1/posts/p1".to_string(),
        );
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["value"], "users/u1/posts/p1");
    }

    #[test]
    fn test_serialize_reference_fallback() {
        let val = FireqlValue::Reference("some/other/path".to_string());
        let json = serde_json::to_value(&val).unwrap();
        assert_eq!(json["value"], "some/other/path");
    }
```

- [ ] **Step 2: Run tests to verify serialization tests fail**

Run: `cargo test --lib value::tests -- --nocapture`
Expected: The 3 new serialization tests FAIL (value still contains full path). The 3 helper tests still PASS.

- [ ] **Step 3: Update Reference serialization to use `to_relative_path`**

Change lines 111-115 in `src/value.rs` from:

```rust
            Self::Reference(r) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "reference")?;
                map.serialize_entry("value", r)?;
                map.end()
            }
```

To:

```rust
            Self::Reference(r) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("_firestore_type", "reference")?;
                map.serialize_entry("value", to_relative_path(r))?;
                map.end()
            }
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `cargo test --lib value::tests -- --nocapture`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run full project tests and check compilation**

Run: `cargo test`
Expected: All tests PASS, no compilation errors.

- [ ] **Step 6: Commit**

```bash
git add src/value.rs
git commit -m "feat: display reference values as collection-relative paths

Reference fields now show 'users/u1' instead of
'projects/p/databases/(default)/documents/users/u1' in JSON output."
```
