---
type: Rust Function
title: sanitize_outbound_ehlo_name
resource: LPE-CT/src/smtp/outbound_delivery.rs#L369-L376
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/outbound_delivery/is_valid_ehlo_hostname
  called_by:
  - functions/LPE-CT/src/smtp/runtime_config_from_dashboard
---

# Signature

`pub(in crate::smtp) fn sanitize_outbound_ehlo_name(value: &str) -> String`

# Calls

- [is_valid_ehlo_hostname](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/is_valid_ehlo_hostname.md)

# Called by

- [runtime_config_from_dashboard](../../../../../functions/LPE-CT/src/smtp/runtime_config_from_dashboard.md)