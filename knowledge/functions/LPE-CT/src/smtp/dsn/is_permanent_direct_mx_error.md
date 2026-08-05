---
type: Rust Function
title: is_permanent_direct_mx_error
resource: LPE-CT/src/smtp/dsn.rs#L145-L152
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
---

# Signature

`pub(super) fn is_permanent_direct_mx_error(detail: &str) -> bool`

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)