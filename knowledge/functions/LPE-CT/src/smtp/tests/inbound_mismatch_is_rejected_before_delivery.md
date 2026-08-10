---
type: Rust Function
title: inbound_mismatch_is_rejected_before_delivery
resource: LPE-CT/src/smtp/tests.rs#L2322-L2349
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/classify_inbound_message
---

# Signature

`fn inbound_mismatch_is_rejected_before_delivery()`

# Calls

- [classify_inbound_message](../../../../../functions/LPE-CT/src/smtp/antivirus/classify_inbound_message.md)