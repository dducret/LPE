---
type: Rust Function
title: synthesized_retry_policy
resource: crates/lpe-storage/src/outbound.rs#L60-L66
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/normalize_handoff_response
---

# Signature

`fn synthesized_retry_policy(response: &OutboundMessageHandoffResponse) -> &'static str`

# Called by

- [normalize_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/normalize_handoff_response.md)