---
type: Rust Function
title: default_dkim_settings
resource: LPE-CT/src/dashboard_config.rs#L942-L950
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_dkim_headers
  called_by:
  - functions/LPE-CT/src/dashboard_config/default_state
---

# Signature

`pub(crate) fn default_dkim_settings() -> DkimSettings`

# Calls

- [default_dkim_headers](../../../../functions/LPE-CT/src/dashboard_config/default_dkim_headers.md)

# Called by

- [default_state](../../../../functions/LPE-CT/src/dashboard_config/default_state.md)