---
type: Rust Method
title: purge_expired_replay_rows
resource: crates/lpe-storage/src/change.rs#L688-L714
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints
---

# Signature

`pub async fn purge_expired_replay_rows(&self) -> Result<u64>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [exercise_change_log_cursor_constraints](../../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints.md)