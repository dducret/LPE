---
type: Rust Method
title: mark_outbound_queue_attempt_failure
resource: crates/lpe-storage/src/outbound.rs#L421-L439
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
  called_by:
  - functions/crates/lpe-cli/src/dispatch_outbound_message
---

# Signature

`pub async fn mark_outbound_queue_attempt_failure( &self, queue_id: Uuid, detail: &str, ) -> Result<OutboundQueueStatusUpdate>`

# Calls

- [update_outbound_queue_status](../../../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)

# Called by

- [dispatch_outbound_message](../../../../../../functions/crates/lpe-cli/src/dispatch_outbound_message.md)