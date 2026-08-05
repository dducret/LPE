---
type: Rust Function
title: is_permanent_relay_error
resource: LPE-CT/src/smtp/dsn.rs#L154-L157
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message
---

# Signature

`pub(super) fn is_permanent_relay_error(detail: &str) -> bool`

# Called by

- [relay_message](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message.md)