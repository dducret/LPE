---
type: Rust Function
title: lock_inbound_trace_delivery
resource: crates/lpe-storage/src/inbound.rs#L629-L640
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/inbound/inbound_trace_advisory_lock_keys
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`async fn lock_inbound_trace_delivery( tx: &mut sqlx::Transaction<'_, Postgres>, trace_id: &str, ) -> Result<()>`

# Calls

- [inbound_trace_advisory_lock_keys](../../../../../functions/crates/lpe-storage/src/inbound/inbound_trace_advisory_lock_keys.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)