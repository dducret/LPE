---
type: Rust Method
title: envelope_recipients
resource: crates/lpe-domain/src/transport.rs#L96-L103
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route
  - functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle
---

# Signature

`pub fn envelope_recipients(&self) -> Vec<String>`

# Called by

- [process_outbound_handoff](../../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [resolve_outbound_route](../../../../../../functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route.md)
- [evaluate_outbound_throttle](../../../../../../functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle.md)