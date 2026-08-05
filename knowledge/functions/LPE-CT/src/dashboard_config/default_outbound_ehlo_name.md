---
type: Rust Function
title: default_outbound_ehlo_name
resource: LPE-CT/src/dashboard_config.rs#L896-L907
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name
  - functions/LPE-CT/src/dashboard_config/is_valid_domain_name
  called_by:
  - functions/LPE-CT/src/dashboard_config/default_state
---

# Signature

`pub(crate) fn default_outbound_ehlo_name() -> String`

# Calls

- [normalize_outbound_ehlo_name](../../../../functions/LPE-CT/src/dashboard_config/normalize_outbound_ehlo_name.md)
- [is_valid_domain_name](../../../../functions/LPE-CT/src/dashboard_config/is_valid_domain_name.md)

# Called by

- [default_state](../../../../functions/LPE-CT/src/dashboard_config/default_state.md)