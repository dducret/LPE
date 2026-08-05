---
type: Rust Function
title: load_dashboard_state
resource: LPE-CT/src/storage.rs#L424-L440
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) async fn load_dashboard_state( config: &LocalDbConfig, ) -> Result<Option<crate::DashboardState>>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)