---
type: Rust Function
title: sync_dashboard_to_postgres
resource: LPE-CT/src/main.rs#L945-L950
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/local_db_config_from_dashboard
  - functions/LPE-CT/src/storage/persist_dashboard_state
  - functions/LPE-CT/src/storage/sync_dashboard_configuration
  called_by:
  - functions/LPE-CT/src/http_routes/run_digest_reports
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/mutate_state
  - functions/LPE-CT/src/run_reporting_scheduler
  - functions/LPE-CT/src/sync_technical_store
  - functions/LPE-CT/src/append_audit_event_with_actor
---

# Signature

`async fn sync_dashboard_to_postgres(snapshot: &DashboardState) -> Result<()>`

# Calls

- [local_db_config_from_dashboard](../../../functions/LPE-CT/src/local_db_config_from_dashboard.md)
- [persist_dashboard_state](../../../functions/LPE-CT/src/storage/persist_dashboard_state.md)
- [sync_dashboard_configuration](../../../functions/LPE-CT/src/storage/sync_dashboard_configuration.md)

# Called by

- [run_digest_reports](../../../functions/LPE-CT/src/http_routes/run_digest_reports.md)
- [main](../../../functions/LPE-CT/src/main.md)
- [mutate_state](../../../functions/LPE-CT/src/mutate_state.md)
- [run_reporting_scheduler](../../../functions/LPE-CT/src/run_reporting_scheduler.md)
- [sync_technical_store](../../../functions/LPE-CT/src/sync_technical_store.md)
- [append_audit_event_with_actor](../../../functions/LPE-CT/src/append_audit_event_with_actor.md)