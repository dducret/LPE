---
type: Rust Function
title: read_state
resource: LPE-CT/src/main.rs#L935-L941
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/health
  - functions/LPE-CT/src/http_routes/health_ready
  - functions/LPE-CT/src/http_routes/login
  - functions/LPE-CT/src/http_routes/dashboard
  - functions/LPE-CT/src/http_routes/quarantine_items
  - functions/LPE-CT/src/http_routes/mail_history
  - functions/LPE-CT/src/http_routes/trace_history
  - functions/LPE-CT/src/http_routes/retry_trace
  - functions/LPE-CT/src/http_routes/release_trace
  - functions/LPE-CT/src/http_routes/delete_trace
  - functions/LPE-CT/src/http_routes/route_diagnostics
  - functions/LPE-CT/src/http_routes/policy_status
  - functions/LPE-CT/src/http_routes/accepted_domains
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_network
  - functions/LPE-CT/src/http_routes/select_public_tls_profile
  - functions/LPE-CT/src/http_routes/delete_public_tls_profile
  - functions/LPE-CT/src/http_routes/update_policies
  - functions/LPE-CT/src/http_routes/reporting_snapshot
  - functions/LPE-CT/src/http_routes/update_reporting
  - functions/LPE-CT/src/http_routes/run_digest_reports
  - functions/LPE-CT/src/http_routes/system_diagnostic_report
  - functions/LPE-CT/src/http_routes/outbound_handoff
  - functions/LPE-CT/src/sync_technical_store
---

# Signature

`fn read_state(state: &AppState) -> Result<DashboardState, ApiError>`

# Called by

- [health](../../../functions/LPE-CT/src/http_routes/health.md)
- [health_ready](../../../functions/LPE-CT/src/http_routes/health_ready.md)
- [login](../../../functions/LPE-CT/src/http_routes/login.md)
- [dashboard](../../../functions/LPE-CT/src/http_routes/dashboard.md)
- [quarantine_items](../../../functions/LPE-CT/src/http_routes/quarantine_items.md)
- [mail_history](../../../functions/LPE-CT/src/http_routes/mail_history.md)
- [trace_history](../../../functions/LPE-CT/src/http_routes/trace_history.md)
- [retry_trace](../../../functions/LPE-CT/src/http_routes/retry_trace.md)
- [release_trace](../../../functions/LPE-CT/src/http_routes/release_trace.md)
- [delete_trace](../../../functions/LPE-CT/src/http_routes/delete_trace.md)
- [route_diagnostics](../../../functions/LPE-CT/src/http_routes/route_diagnostics.md)
- [policy_status](../../../functions/LPE-CT/src/http_routes/policy_status.md)
- [accepted_domains](../../../functions/LPE-CT/src/http_routes/accepted_domains.md)
- [create_accepted_domain](../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_network](../../../functions/LPE-CT/src/http_routes/update_network.md)
- [select_public_tls_profile](../../../functions/LPE-CT/src/http_routes/select_public_tls_profile.md)
- [delete_public_tls_profile](../../../functions/LPE-CT/src/http_routes/delete_public_tls_profile.md)
- [update_policies](../../../functions/LPE-CT/src/http_routes/update_policies.md)
- [reporting_snapshot](../../../functions/LPE-CT/src/http_routes/reporting_snapshot.md)
- [update_reporting](../../../functions/LPE-CT/src/http_routes/update_reporting.md)
- [run_digest_reports](../../../functions/LPE-CT/src/http_routes/run_digest_reports.md)
- [system_diagnostic_report](../../../functions/LPE-CT/src/http_routes/system_diagnostic_report.md)
- [outbound_handoff](../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)
- [sync_technical_store](../../../functions/LPE-CT/src/sync_technical_store.md)