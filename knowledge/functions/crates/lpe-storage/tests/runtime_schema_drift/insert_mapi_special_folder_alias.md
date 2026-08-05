---
type: Rust Function
title: insert_mapi_special_folder_alias
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2325-L2351
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints
---

# Signature

`async fn insert_mapi_special_folder_alias( pool: &PgPool, fixture: &RuntimeFixture, alias_folder_id: i64, canonical_folder_id: i64, source_key: &[u8], change_number: i64, ) -> std::result::Result<(), sqlx::Error>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [exercise_mapi_special_folder_alias_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints.md)