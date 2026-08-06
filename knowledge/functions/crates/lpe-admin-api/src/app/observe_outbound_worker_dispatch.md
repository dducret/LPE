---
type: Rust Function
title: observe_outbound_worker_dispatch
resource: crates/lpe-admin-api/src/app.rs#L406-L408
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/observability/record_outbound_dispatch
  called_by:
  - functions/crates/lpe-cli/src/dispatch_outbound_message
---

# Signature

`pub fn observe_outbound_worker_dispatch(status: &str)`

# Calls

- [record_outbound_dispatch](../../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_dispatch.md)

# Called by

- [dispatch_outbound_message](../../../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)