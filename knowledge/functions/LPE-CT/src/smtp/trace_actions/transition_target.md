---
type: Rust Function
title: transition_target
resource: LPE-CT/src/smtp/trace_actions.rs#L114-L132
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`fn transition_target(queue: &str, direction: &str, action: TraceAction) -> Option<&'static str>`

# Called by

- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)