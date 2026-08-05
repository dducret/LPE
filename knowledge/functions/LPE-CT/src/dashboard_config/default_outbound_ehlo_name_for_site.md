---
type: Rust Function
title: default_outbound_ehlo_name_for_site
resource: LPE-CT/src/dashboard_config.rs#L909-L915
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/is_valid_domain_name
  called_by:
  - functions/LPE-CT/src/dashboard_config/normalize_relay_settings
---

# Signature

`fn default_outbound_ehlo_name_for_site(site: &SiteProfile) -> String`

# Calls

- [normalize_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name.md)
- [is_valid_domain_name](../../../../functions/LPE-CT/src/dashboard_config/is_valid_domain_name.md)

# Called by

- [normalize_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_relay_settings.md)