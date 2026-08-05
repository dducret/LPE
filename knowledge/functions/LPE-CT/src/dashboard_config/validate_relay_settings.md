---
type: Rust Function
title: validate_relay_settings
resource: LPE-CT/src/dashboard_config.rs#L325-L338
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/is_valid_domain_name
  called_by:
  - functions/LPE-CT/src/dashboard_config/normalize_relay_settings
  - functions/LPE-CT/src/http_routes/update_relay
---

# Signature

`pub(crate) fn validate_relay_settings(settings: &mut RelaySettings) -> Result<(), ApiError>`

# Calls

- [normalize_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name.md)
- [is_valid_domain_name](../../../../functions/LPE-CT/src/dashboard_config/is_valid_domain_name.md)

# Called by

- [normalize_relay_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_relay_settings.md)
- [update_relay](../../../../functions/LPE-CT/src/http_routes/update_relay.md)