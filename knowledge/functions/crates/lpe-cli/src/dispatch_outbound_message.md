---
type: Rust Function
title: dispatch_outbound_message
resource: crates/lpe-cli/src/main.rs#L292-L360
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-cli/src/send_outbound_handoff
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
  - functions/crates/lpe-admin-api/src/app/observe_outbound_worker_dispatch
  - functions/crates/lpe-storage/src/outbound/Storage/mark_outbound_queue_attempt_failure
  called_by:
  - functions/crates/lpe-cli/src/run_outbound_worker
---

# Signature

`async fn dispatch_outbound_message( storage: &Storage, client: &reqwest::Client, base_url: &str, integration_key: &str, item: OutboundMessageHandoffRequest, )`

# Calls

- [send_outbound_handoff](../../../../functions/crates/lpe-cli/src/send_outbound_handoff.md)
- [update_outbound_queue_status](../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)
- [observe_outbound_worker_dispatch](../../../../functions/crates/lpe-admin-api/src/app/observe_outbound_worker_dispatch.md)
- [mark_outbound_queue_attempt_failure](../../../../functions/crates/lpe-storage/src/outbound/Storage/mark_outbound_queue_attempt_failure.md)

# Called by

- [run_outbound_worker](../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)