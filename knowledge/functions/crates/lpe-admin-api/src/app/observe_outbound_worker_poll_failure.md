---
type: Rust Function
title: observe_outbound_worker_poll_failure
resource: crates/lpe-admin-api/src/app.rs#L411-L413
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll_failure
  called_by:
  - functions/crates/lpe-cli/src/run_outbound_worker
---

# Signature

`pub fn observe_outbound_worker_poll_failure()`

# Calls

- [record_outbound_worker_poll_failure](../../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll_failure.md)

# Called by

- [run_outbound_worker](../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)