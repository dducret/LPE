---
type: Rust Function
title: is_valid_domain_name
resource: LPE-CT/src/dashboard_config.rs#L404-L418
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/validate_relay_settings
  - functions/LPE-CT/src/dashboard_config/accepted_domain_from_input
  - functions/LPE-CT/src/dashboard_config/normalize_accepted_domains
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site
---

# Signature

`fn is_valid_domain_name(value: &str) -> bool`

# Called by

- [validate_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/validate_relay_settings.md)
- [accepted_domain_from_input](../../../../functions/LPE-CT/src/dashboard_config/accepted_domain_from_input.md)
- [normalize_accepted_domains](../../../../functions/LPE-CT/src/dashboard_config/normalize_accepted_domains.md)
- [default_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name.md)
- [default_outbound_ehlo_name_for_site](../../../../functions/LPE-CT/src/dashboard_config/default_outbound_ehlo_name_for_site.md)