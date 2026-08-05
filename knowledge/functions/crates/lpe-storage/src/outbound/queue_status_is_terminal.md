---
type: Rust Function
title: queue_status_is_terminal
resource: crates/lpe-storage/src/outbound.rs#L15-L17
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/is_duplicate_terminal_handoff
  - functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response
---

# Signature

`fn queue_status_is_terminal(status: &str) -> bool`

# Called by

- [is_duplicate_terminal_handoff](../../../../../functions/crates/lpe-storage/src/outbound/is_duplicate_terminal_handoff.md)
- [should_ignore_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)