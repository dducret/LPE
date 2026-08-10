---
type: Rust Function
title: persist_state
resource: LPE-CT/src/main.rs#L1035-L1045
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_reporting
  - functions/LPE-CT/src/http_routes/run_digest_reports
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/mutate_state
  - functions/LPE-CT/src/run_reporting_scheduler
  - functions/LPE-CT/src/restore_dashboard_state
  - functions/LPE-CT/src/append_audit_event_with_actor
  - functions/LPE-CT/src/load_or_initialize_state
  - functions/LPE-CT/src/dashboard_response_serializes_runtime_system_without_persisting_it
---

# Signature

`fn persist_state(path: &Path, state: &DashboardState) -> Result<()>`

# Called by

- [create_accepted_domain](../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_reporting](../../../functions/LPE-CT/src/http_routes/update_reporting.md)
- [run_digest_reports](../../../functions/LPE-CT/src/http_routes/run_digest_reports.md)
- [main](../../../functions/LPE-CT/src/main.md)
- [mutate_state](../../../functions/LPE-CT/src/mutate_state.md)
- [run_reporting_scheduler](../../../functions/LPE-CT/src/run_reporting_scheduler.md)
- [restore_dashboard_state](../../../functions/LPE-CT/src/restore_dashboard_state.md)
- [append_audit_event_with_actor](../../../functions/LPE-CT/src/append_audit_event_with_actor.md)
- [load_or_initialize_state](../../../functions/LPE-CT/src/load_or_initialize_state.md)
- [dashboard_response_serializes_runtime_system_without_persisting_it](../../../functions/LPE-CT/src/dashboard_response_serializes_runtime_system_without_persisting_it.md)