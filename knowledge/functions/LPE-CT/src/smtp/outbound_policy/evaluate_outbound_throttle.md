---
type: Rust Function
title: evaluate_outbound_throttle
resource: LPE-CT/src/smtp/outbound_policy.rs#L112-L209
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients
  - functions/LPE-CT/src/smtp/policy/matches_domain
  - functions/LPE-CT/src/smtp/policy/matches_any_domain
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`pub(in crate::smtp) async fn evaluate_outbound_throttle( spool_dir: &Path, config: &RuntimeConfig, payload: &OutboundMessageHandoffRequest, ) -> Result<Option<TransportThrottleStatus>>`

# Calls

- [envelope_recipients](../../../../../functions/crates/lpe-domain/src/transport/OutboundMessageHandoffRequest/envelope_recipients.md)
- [matches_domain](../../../../../functions/LPE-CT/src/smtp/policy/matches_domain.md)
- [matches_any_domain](../../../../../functions/LPE-CT/src/smtp/policy/matches_any_domain.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)