---
type: Rust Function
title: inbound_magika_failure_is_quarantined
resource: LPE-CT/src/smtp/tests.rs#L2352-L2396
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/runtime_config
---

# Signature

`async fn inbound_magika_failure_is_quarantined()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)