---
type: Rust Function
title: default_dkim_headers
resource: LPE-CT/src/dashboard_config.rs#L929-L940
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/normalize_policy_settings
  - functions/LPE-CT/src/dashboard_config/default_dkim_settings
---

# Signature

`pub(crate) fn default_dkim_headers() -> Vec<String>`

# Called by

- [normalize_policy_settings](../../../../functions/LPE-CT/src/dashboard_config/normalize_policy_settings.md)
- [default_dkim_settings](../../../../functions/LPE-CT/src/dashboard_config/default_dkim_settings.md)