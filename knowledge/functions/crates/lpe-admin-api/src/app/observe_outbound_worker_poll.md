---
type: Rust Function
title: observe_outbound_worker_poll
resource: crates/lpe-admin-api/src/app.rs#L407-L409
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll
  called_by:
  - functions/crates/lpe-cli/src/run_outbound_worker
---

# Signature

`pub fn observe_outbound_worker_poll(batch_size: usize)`

# Calls

- [record_outbound_worker_poll](../../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll.md)

# Called by

- [run_outbound_worker](../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)