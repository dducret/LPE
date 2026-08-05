---
type: Rust Function
title: exercise_mailbox_move_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L3099-L3554
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_mailbox_move_path( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, submitted: &SubmittedMessage, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [mapi_xid](../../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)