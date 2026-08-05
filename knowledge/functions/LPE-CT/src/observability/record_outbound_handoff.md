---
type: Rust Function
title: record_outbound_handoff
resource: LPE-CT/src/observability.rs#L127-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/LPE-CT/src/http_routes/outbound_handoff
---

# Signature

`pub fn record_outbound_handoff(status: &str)`

# Calls

- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [outbound_handoff](../../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)