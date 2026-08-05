---
type: Rust Function
title: record_outbound_dispatch
resource: crates/lpe-admin-api/src/observability.rs#L202-L209
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-admin-api/src/app/observe_outbound_worker_dispatch
---

# Signature

`pub fn record_outbound_dispatch(status: &str)`

# Calls

- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [observe_outbound_worker_dispatch](../../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_dispatch.md)