---
type: Rust Function
title: parse_enhanced_status
resource: LPE-CT/src/smtp/dsn.rs#L159-L174
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
---

# Signature

`pub(super) fn parse_enhanced_status(detail: &str) -> Option<String>`

# Called by

- [relay_message_to_target_for_recipients](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)