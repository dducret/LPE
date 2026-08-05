---
type: Rust Function
title: resolve_outbound_route
resource: LPE-CT/src/smtp/outbound_policy.rs#L75-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients
  - functions/LPE-CT/src/smtp/policy/matches_domain
  - functions/LPE-CT/src/smtp/policy/matches_any_domain
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default
---

# Signature

`pub(in crate::smtp) fn resolve_outbound_route( config: &RuntimeConfig, payload: &OutboundMessageHandoffRequest, ) -> TransportRouteDecision`

# Calls

- [envelope_recipients](../../../../../functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients.md)
- [matches_domain](../../../../../functions/LPE-CT/src/smtp/policy/matches_domain.md)
- [matches_any_domain](../../../../../functions/LPE-CT/src/smtp/policy/matches_any_domain.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [outbound_route_without_smart_host_uses_direct_mx_default](../../../../../functions/LPE-CT/src/smtp/tests/outbound_route_without_smart_host_uses_direct_mx_default.md)