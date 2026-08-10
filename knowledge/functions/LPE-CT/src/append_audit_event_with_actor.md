---
type: Rust Function
title: append_audit_event_with_actor
resource: LPE-CT/src/main.rs#L991-L1019
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/persist_state
  - functions/LPE-CT/src/sync_dashboard_to_postgres
  called_by:
  - functions/LPE-CT/src/http_routes/login
  - functions/LPE-CT/src/http_routes/logout
  - functions/LPE-CT/src/http_routes/retry_trace
  - functions/LPE-CT/src/http_routes/release_trace
  - functions/LPE-CT/src/http_routes/delete_trace
  - functions/LPE-CT/src/http_routes/delete_host_log
  - functions/LPE-CT/src/http_routes/system_diagnostic_service_action
  - functions/LPE-CT/src/http_routes/system_health_check
  - functions/LPE-CT/src/http_routes/run_system_tool
  - functions/LPE-CT/src/http_routes/run_spam_test
  - functions/LPE-CT/src/http_routes/connect_lpe_support
  - functions/LPE-CT/src/http_routes/flush_mail_queue
---

# Signature

`async fn append_audit_event_with_actor( state: &AppState, actor: &str, action: &str, details: &str, ) -> Result<(), ApiError>`

# Calls

- [persist_state](../../../functions/LPE-CT/src/persist_state.md)
- [sync_dashboard_to_postgres](../../../functions/LPE-CT/src/sync_dashboard_to_postgres.md)

# Called by

- [login](../../../functions/LPE-CT/src/http_routes/login.md)
- [logout](../../../functions/LPE-CT/src/http_routes/logout.md)
- [retry_trace](../../../functions/LPE-CT/src/http_routes/retry_trace.md)
- [release_trace](../../../functions/LPE-CT/src/http_routes/release_trace.md)
- [delete_trace](../../../functions/LPE-CT/src/http_routes/delete_trace.md)
- [delete_host_log](../../../functions/LPE-CT/src/http_routes/delete_host_log.md)
- [system_diagnostic_service_action](../../../functions/LPE-CT/src/http_routes/system_diagnostic_service_action.md)
- [system_health_check](../../../functions/LPE-CT/src/http_routes/system_health_check.md)
- [run_system_tool](../../../functions/LPE-CT/src/http_routes/run_system_tool.md)
- [run_spam_test](../../../functions/LPE-CT/src/http_routes/run_spam_test.md)
- [connect_lpe_support](../../../functions/LPE-CT/src/http_routes/connect_lpe_support.md)
- [flush_mail_queue](../../../functions/LPE-CT/src/http_routes/flush_mail_queue.md)