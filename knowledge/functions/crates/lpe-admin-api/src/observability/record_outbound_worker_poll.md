---
type: Rust Function
title: record_outbound_worker_poll
resource: crates/lpe-admin-api/src/observability.rs#L211-L216
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/observability/unix_timestamp_seconds
  called_by:
  - functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll
---

# Signature

`pub fn record_outbound_worker_poll(batch_size: usize)`

# Calls

- [unix_timestamp_seconds](../../../../../functions/crates/lpe-admin-api/src/observability/unix_timestamp_seconds.md)

# Called by

- [observe_outbound_worker_poll](../../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_poll.md)