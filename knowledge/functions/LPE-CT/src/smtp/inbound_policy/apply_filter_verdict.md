---
type: Rust Function
title: apply_filter_verdict
resource: LPE-CT/src/smtp/inbound_policy.rs#L400-L412
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
---

# Signature

`pub(in crate::smtp) fn apply_filter_verdict(message: &mut QueuedMessage, verdict: &FilterVerdict)`

# Called by

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)