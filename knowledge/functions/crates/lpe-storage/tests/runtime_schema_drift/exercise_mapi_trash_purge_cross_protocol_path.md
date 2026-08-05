---
type: Rust Function
title: exercise_mapi_trash_purge_cross_protocol_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L5492-L5647
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_mapi_trash_purge_cross_protocol_path( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)