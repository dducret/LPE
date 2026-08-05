---
type: Rust Function
title: response
resource: crates/lpe-storage/src/outbound.rs#L441-L464
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs
  - functions/crates/lpe-storage/src/outbound/duplicate_terminal_handoff_is_recognized_by_remote_reference
  - functions/crates/lpe-storage/src/outbound/same_trace_can_progress_from_deferred_to_relayed
---

# Signature

`fn response( status: TransportDeliveryStatus, trace_id: &str, remote_message_ref: Option<&str>, ) -> OutboundMessageHandoffResponse`

# Called by

- [duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs](../../../../../functions/crates/lpe-storage/src/outbound/duplicate_handoff_is_recognized_by_trace_id_even_when_status_differs.md)
- [duplicate_terminal_handoff_is_recognized_by_remote_reference](../../../../../functions/crates/lpe-storage/src/outbound/duplicate_terminal_handoff_is_recognized_by_remote_reference.md)
- [same_trace_can_progress_from_deferred_to_relayed](../../../../../functions/crates/lpe-storage/src/outbound/same_trace_can_progress_from_deferred_to_relayed.md)