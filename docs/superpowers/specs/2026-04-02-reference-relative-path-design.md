# Reference Value Relative Path Display

## Summary

Firestore reference fields currently display the full resource path (e.g., `projects/p/databases/(default)/documents/users/u1`). The `projects/.../documents/` prefix is common to all references and provides no useful information. Change the serialized `value` to show only the collection-relative path (e.g., `users/u1`).

## Current Behavior

```json
{
  "_firestore_type": "reference",
  "value": "projects/my-project/databases/(default)/documents/users/GFya7Voj8xWRTKBT2w7USvxpXsf2"
}
```

## New Behavior

```json
{
  "_firestore_type": "reference",
  "value": "users/GFya7Voj8xWRTKBT2w7USvxpXsf2"
}
```

## Design

### Change Scope

- **File:** `src/value.rs`, `Reference` variant serialization (lines 111-115)
- **Logic:** Find `/documents/` in the full path string and extract everything after it
- **Fallback:** If `/documents/` is not found, output the full path unchanged

### Internal Representation Unchanged

`FireqlValue::Reference(String)` continues to store the full Firestore resource path internally. This is required by `planner.rs::expand_reference_path()` and other internal logic. Only the serialized JSON output changes.

### Edge Cases

| Case | Input | Output |
|------|-------|--------|
| Normal | `projects/p/databases/(default)/documents/users/u1` | `users/u1` |
| Nested collection | `projects/p/databases/(default)/documents/users/u1/posts/p1` | `users/u1/posts/p1` |
| No `/documents/` found | `some/other/path` | `some/other/path` (fallback) |

## Testing

- Existing serialization tests for Reference type must be updated to expect relative paths
- Add test for nested collection reference
- Add test for fallback case (no `/documents/` in path)
