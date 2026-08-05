---
type: Rust Function
title: normalize_smtp_target
resource: LPE-CT/src/smtp.rs#L1235-L1241
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
---

# Signature

`fn normalize_smtp_target(target: &str) -> String`

# Called by

- [relay_message_to_target_for_recipients](../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)