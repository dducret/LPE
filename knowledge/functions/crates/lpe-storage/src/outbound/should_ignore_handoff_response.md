---
type: Rust Function
title: should_ignore_handoff_response
resource: crates/lpe-storage/src/outbound.rs#L47-L58
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/outbound/same_trace_id
  - functions/crates/lpe-storage/src/outbound/is_duplicate_terminal_handoff
  - functions/crates/lpe-storage/src/outbound/queue_status_is_terminal
  - functions/crates/lpe-storage/src/outbound/would_regress_queue_status
  called_by:
  - functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status
---

# Signature

`fn should_ignore_handoff_response( current_status: &str, current_trace_id: Option<&str>, current_remote_message_ref: Option<&str>, response: &OutboundMessageHandoffResponse, ) -> bool`

# Calls

- [same_trace_id](../../../../../functions/crates/lpe-storage/src/outbound/same_trace_id.md)
- [is_duplicate_terminal_handoff](../../../../../functions/crates/lpe-storage/src/outbound/is_duplicate_terminal_handoff.md)
- [queue_status_is_terminal](../../../../../functions/crates/lpe-storage/src/outbound/queue_status_is_terminal.md)
- [would_regress_queue_status](../../../../../functions/crates/lpe-storage/src/outbound/would_regress_queue_status.md)

# Called by

- [update_outbound_queue_status](../../../../../functions/crates/lpe-storage/src/outbound/Storage/update_outbound_queue_status.md)