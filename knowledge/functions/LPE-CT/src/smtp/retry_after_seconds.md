---
type: Rust Function
title: retry_after_seconds
resource: LPE-CT/src/smtp.rs#L1223-L1226
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/dsn/direct_mx_failure
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
  - functions/LPE-CT/src/smtp/outbound_delivery/deliver_outbound_to_local_recipients
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients
  - functions/LPE-CT/src/smtp/outbound_policy/retry_advice_from_spooled_message
---

# Signature

`fn retry_after_seconds(base: u32, attempt_count: u32) -> u32`

# Called by

- [direct_mx_failure](../../../../functions/LPE-CT/src/smtp/dsn/direct_mx_failure.md)
- [relay_message](../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)
- [deliver_outbound_to_local_recipients](../../../../functions/LPE-CT/src/smtp/outbound_delivery/deliver_outbound_to_local_recipients.md)
- [relay_message_to_target_for_recipients](../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_to_target_for_recipients.md)
- [retry_advice_from_spooled_message](../../../../functions/LPE-CT/src/smtp/outbound_policy/retry_advice_from_spooled_message.md)