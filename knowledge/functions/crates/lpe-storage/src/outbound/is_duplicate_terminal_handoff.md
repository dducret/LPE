---
type: Rust Function
title: is_duplicate_terminal_handoff
resource: crates/lpe-storage/src/outbound.rs#L26-L41
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/outbound/queue_status_is_terminal
  called_by:
  - functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response
---

# Signature

`fn is_duplicate_terminal_handoff( current_status: &str, current_remote_message_ref: Option<&str>, response: &OutboundMessageHandoffResponse, ) -> bool`

# Calls

- [queue_status_is_terminal](../../../../../functions/crates/lpe-storage/src/outbound/queue_status_is_terminal.md)

# Called by

- [should_ignore_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)