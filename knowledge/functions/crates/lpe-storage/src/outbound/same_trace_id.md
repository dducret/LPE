---
type: Rust Function
title: same_trace_id
resource: crates/lpe-storage/src/outbound.rs#L19-L24
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response
---

# Signature

`fn same_trace_id(current_trace_id: Option<&str>, response_trace_id: &str) -> bool`

# Called by

- [should_ignore_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/should_ignore_handoff_response.md)