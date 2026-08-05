---
type: Rust Function
title: would_regress_queue_status
resource: crates/lpe-storage/src/outbound.rs#L43-L45
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response
---

# Signature

`fn would_regress_queue_status(current_status: &str, next_status: &str) -> bool`

# Called by

- [should_ignore_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)