---
type: Rust Function
title: local_db_config_from_dashboard
resource: LPE-CT/src/main.rs#L928-L936
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/sync_dashboard_to_postgres
---

# Signature

`fn local_db_config_from_dashboard(dashboard: &DashboardState) -> storage::LocalDbConfig`

# Called by

- [main](../../../functions/LPE-CT/src/main.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)