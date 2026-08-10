---
type: Rust Function
title: persist_dashboard_state
resource: LPE-CT/src/storage.rs#L442-L465
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/LPE-CT/src/sync_dashboard_to_postgres
---

# Signature

`pub(crate) async fn persist_dashboard_state( config: &LocalDbConfig, dashboard: &crate::DashboardState, ) -> Result<()>`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [sync_dashboard_to_postgres](../../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)