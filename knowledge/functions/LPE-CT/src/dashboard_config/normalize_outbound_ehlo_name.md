---
type: Rust Function
title: normalize_outbound_ehlo_name
resource: LPE-CT/src/dashboard_config.rs#L400-L402
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/validate_relay_settings
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site
---

# Signature

`fn normalize_outbound_ehlo_name(value: &str) -> String`

# Called by

- [validate_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/validate_relay_settings.md)
- [default_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name.md)
- [default_outbound_ehlo_name_for_site](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site.md)