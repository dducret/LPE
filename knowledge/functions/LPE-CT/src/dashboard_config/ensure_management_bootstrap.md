---
type: Rust Function
title: ensure_management_bootstrap
resource: LPE-CT/src/dashboard_config.rs#L705-L733
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/required_trimmed_env
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn ensure_management_bootstrap(state: &mut DashboardState) -> Result<()>`

# Calls

- [required_trimmed_env](../../../../functions/LPE-CT/src/dashboard_config/required_trimmed_env.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)