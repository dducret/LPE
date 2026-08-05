---
type: Rust Function
title: normalize_relay_settings
resource: LPE-CT/src/dashboard_config.rs#L340-L345
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/validate_relay_settings
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn normalize_relay_settings(settings: &mut RelaySettings, site: &SiteProfile)`

# Calls

- [validate_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/validate_relay_settings.md)
- [default_outbound_ehlo_name_for_site](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)