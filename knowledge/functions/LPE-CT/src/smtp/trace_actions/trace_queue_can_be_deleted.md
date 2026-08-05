---
type: Rust Function
title: trace_queue_can_be_deleted
resource: LPE-CT/src/smtp/trace_actions.rs#L134-L139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`fn trace_queue_can_be_deleted(queue: &str) -> bool`

# Called by

- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)