---
type: Rust Function
title: runtime_config_from_dashboard
resource: LPE-CT/src/smtp.rs#L539-L693
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/outbound_delivery/sanitize_outbound_ehlo_name
  - functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers
  called_by:
  - functions/LPE-CT/src/http_routes/quarantine_items
  - functions/LPE-CT/src/http_routes/mail_history
  - functions/LPE-CT/src/http_routes/trace_history
  - functions/LPE-CT/src/http_routes/retry_trace
  - functions/LPE-CT/src/http_routes/release_trace
  - functions/LPE-CT/src/http_routes/delete_trace
  - functions/LPE-CT/src/http_routes/policy_status
  - functions/LPE-CT/src/http_routes/outbound_handoff
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/run_reporting_scheduler
  - functions/LPE-CT/src/smtp/runtime_config_from_store
---

# Signature

`pub(crate) fn runtime_config_from_dashboard(dashboard: &super::DashboardState) -> RuntimeConfig`

# Calls

- [sanitize_outbound_ehlo_name](../../../../functions/LPE-CT/src/smtp/outbound_delivery/sanitize_outbound_ehlo_name.md)
- [load_antivirus_providers](../../../../functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers.md)

# Called by

- [quarantine_items](../../../../functions/LPE-CT/src/http_routes/quarantine_items.md)
- [mail_history](../../../../functions/LPE-CT/src/http_routes/mail_history.md)
- [trace_history](../../../../functions/LPE-CT/src/http_routes/trace_history.md)
- [retry_trace](../../../../functions/LPE-CT/src/http_routes/retry_trace.md)
- [release_trace](../../../../functions/LPE-CT/src/http_routes/release_trace.md)
- [delete_trace](../../../../functions/LPE-CT/src/http_routes/delete_trace.md)
- [policy_status](../../../../functions/LPE-CT/src/http_routes/policy_status.md)
- [outbound_handoff](../../../../functions/LPE-CT/src/http_routes/outbound_handoff.md)
- [main](../../../../functions/LPE-CT/src/main.md)
- [run_reporting_scheduler](../../../../functions/LPE-CT/src/run_reporting_scheduler.md)
- [runtime_config_from_store](../../../../functions/LPE-CT/src/smtp/runtime_config_from_store.md)