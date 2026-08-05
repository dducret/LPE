---
type: Rust Function
title: remove_quarantine_metadata_or_warn
resource: LPE-CT/src/smtp/quarantine.rs#L337-L344
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata
  called_by:
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(in crate::smtp) async fn remove_quarantine_metadata_or_warn( config: &RuntimeConfig, trace_id: &str, )`

# Calls

- [remove_quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/remove_quarantine_metadata.md)

# Called by

- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)