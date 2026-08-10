---
type: Rust Function
title: load_or_initialize_state
resource: LPE-CT/src/main.rs#L1021-L1033
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_state
  - functions/LPE-CT/src/persist_state
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`fn load_or_initialize_state(path: &Path) -> Result<DashboardState>`

# Calls

- [default_state](../../../functions/LPE-CT/src/dashboard_config/default_state.md)
- [persist_state](../../../functions/LPE-CT/src/persist_state.md)

# Called by

- [main](../../../functions/LPE-CT/src/main.md)