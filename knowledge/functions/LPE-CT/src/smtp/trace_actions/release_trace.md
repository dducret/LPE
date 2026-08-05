---
type: Rust Function
title: release_trace
resource: LPE-CT/src/smtp/trace_actions.rs#L18-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(crate) async fn release_trace( spool_dir: &Path, config: &RuntimeConfig, trace_id: &str, ) -> Result<Option<TraceActionResult>>`

# Calls

- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)