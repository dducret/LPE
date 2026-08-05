---
type: Rust Function
title: expect_constraint_failure
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L646-L652
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_blob_reference_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_local_replica_range_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_outlook_cache_fidelity_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_identity_allocation
---

# Signature

`fn expect_constraint_failure<T>( label: &str, result: std::result::Result<T, sqlx::Error>, ) -> Result<()>`

# Called by

- [exercise_blob_reference_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_blob_reference_constraints.md)
- [exercise_mapi_local_replica_range_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_local_replica_range_constraints.md)
- [exercise_mapi_outlook_cache_fidelity_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_outlook_cache_fidelity_constraints.md)
- [exercise_change_log_cursor_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints.md)
- [exercise_mapi_special_folder_alias_constraints](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints.md)
- [exercise_canonical_identity_allocation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_identity_allocation.md)