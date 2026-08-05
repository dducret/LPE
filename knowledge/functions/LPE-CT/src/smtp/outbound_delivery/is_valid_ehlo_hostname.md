---
type: Rust Function
title: is_valid_ehlo_hostname
resource: LPE-CT/src/smtp/outbound_delivery.rs#L378-L391
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/sanitize_outbound_ehlo_name
---

# Signature

`fn is_valid_ehlo_hostname(value: &str) -> bool`

# Called by

- [sanitize_outbound_ehlo_name](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/sanitize_outbound_ehlo_name.md)