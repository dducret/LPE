---
type: Rust Function
title: outbound_status_from_spool
resource: LPE-CT/src/smtp/outbound_policy.rs#L34-L51
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool
---

# Signature

`fn outbound_status_from_spool(queue: &str, message_status: &str) -> TransportDeliveryStatus`

# Called by

- [outbound_handoff_response_from_spool](../../../../../functions/LPE-CT/src/smtp/outbound_policy/outbound_handoff_response_from_spool.md)