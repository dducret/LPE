---
type: Rust Function
title: require_management_admin
resource: LPE-CT/src/management_auth.rs#L39-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/http_routes/me
  - functions/LPE-CT/src/http_routes/dashboard
  - functions/LPE-CT/src/http_routes/quarantine_items
  - functions/LPE-CT/src/http_routes/mail_history
  - functions/LPE-CT/src/http_routes/trace_history
  - functions/LPE-CT/src/http_routes/trace_details
  - functions/LPE-CT/src/http_routes/retry_trace
  - functions/LPE-CT/src/http_routes/release_trace
  - functions/LPE-CT/src/http_routes/delete_trace
  - functions/LPE-CT/src/http_routes/host_logs_list
  - functions/LPE-CT/src/http_routes/host_log_content
  - functions/LPE-CT/src/http_routes/download_host_log
  - functions/LPE-CT/src/http_routes/delete_host_log
  - functions/LPE-CT/src/http_routes/route_diagnostics
  - functions/LPE-CT/src/http_routes/policy_status
  - functions/LPE-CT/src/http_routes/accepted_domains
  - functions/LPE-CT/src/http_routes/create_accepted_domain
  - functions/LPE-CT/src/http_routes/update_accepted_domain
  - functions/LPE-CT/src/http_routes/delete_accepted_domain
  - functions/LPE-CT/src/http_routes/import_accepted_domains
  - functions/LPE-CT/src/http_routes/test_accepted_domain
  - functions/LPE-CT/src/http_routes/update_site
  - functions/LPE-CT/src/http_routes/update_relay
  - functions/LPE-CT/src/http_routes/update_network
  - functions/LPE-CT/src/http_routes/update_system_ntp
  - functions/LPE-CT/src/http_routes/sync_system_ntp
  - functions/LPE-CT/src/http_routes/run_apt_update_upgrade
  - functions/LPE-CT/src/http_routes/run_system_power_action
  - functions/LPE-CT/src/http_routes/upload_public_tls_profile
  - functions/LPE-CT/src/http_routes/select_public_tls_profile
  - functions/LPE-CT/src/http_routes/delete_public_tls_profile
  - functions/LPE-CT/src/http_routes/update_policies
  - functions/LPE-CT/src/http_routes/update_updates
  - functions/LPE-CT/src/http_routes/reporting_snapshot
  - functions/LPE-CT/src/http_routes/update_reporting
  - functions/LPE-CT/src/http_routes/run_digest_reports
  - functions/LPE-CT/src/http_routes/digest_reports
  - functions/LPE-CT/src/http_routes/digest_report_details
  - functions/LPE-CT/src/http_routes/system_diagnostic_services
  - functions/LPE-CT/src/http_routes/system_diagnostic_service_action
  - functions/LPE-CT/src/http_routes/system_diagnostic_report
  - functions/LPE-CT/src/http_routes/system_health_check
  - functions/LPE-CT/src/http_routes/run_system_tool
  - functions/LPE-CT/src/http_routes/run_spam_test
  - functions/LPE-CT/src/http_routes/connect_lpe_support
  - functions/LPE-CT/src/http_routes/flush_mail_queue
---

# Signature

`pub(crate) fn require_management_admin( state: &AppState, headers: &HeaderMap, ) -> Result<ManagementSession, ApiError>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [me](../../../../functions/LPE-CT/src/http_routes/me.md)
- [dashboard](../../../../functions/LPE-CT/src/http_routes/dashboard.md)
- [quarantine_items](../../../../functions/LPE-CT/src/http_routes/quarantine_items.md)
- [mail_history](../../../../functions/LPE-CT/src/http_routes/mail_history.md)
- [trace_history](../../../../functions/LPE-CT/src/http_routes/trace_history.md)
- [trace_details](../../../../functions/LPE-CT/src/http_routes/trace_details.md)
- [retry_trace](../../../../functions/LPE-CT/src/http_routes/retry_trace.md)
- [release_trace](../../../../functions/LPE-CT/src/http_routes/release_trace.md)
- [delete_trace](../../../../functions/LPE-CT/src/http_routes/delete_trace.md)
- [host_logs_list](../../../../functions/LPE-CT/src/http_routes/host_logs_list.md)
- [host_log_content](../../../../functions/LPE-CT/src/http_routes/host_log_content.md)
- [download_host_log](../../../../functions/LPE-CT/src/http_routes/download_host_log.md)
- [delete_host_log](../../../../functions/LPE-CT/src/http_routes/delete_host_log.md)
- [route_diagnostics](../../../../functions/LPE-CT/src/http_routes/route_diagnostics.md)
- [policy_status](../../../../functions/LPE-CT/src/http_routes/policy_status.md)
- [accepted_domains](../../../../functions/LPE-CT/src/http_routes/accepted_domains.md)
- [create_accepted_domain](../../../../functions/LPE-CT/src/http_routes/create_accepted_domain.md)
- [update_accepted_domain](../../../../functions/LPE-CT/src/http_routes/update_accepted_domain.md)
- [delete_accepted_domain](../../../../functions/LPE-CT/src/http_routes/delete_accepted_domain.md)
- [import_accepted_domains](../../../../functions/LPE-CT/src/http_routes/import_accepted_domains.md)
- [test_accepted_domain](../../../../functions/LPE-CT/src/http_routes/test_accepted_domain.md)
- [update_site](../../../../functions/LPE-CT/src/http_routes/update_site.md)
- [update_relay](../../../../functions/LPE-CT/src/http_routes/update_relay.md)
- [update_network](../../../../functions/LPE-CT/src/http_routes/update_network.md)
- [update_system_ntp](../../../../functions/LPE-CT/src/http_routes/update_system_ntp.md)
- [sync_system_ntp](../../../../functions/LPE-CT/src/http_routes/sync_system_ntp.md)
- [run_apt_update_upgrade](../../../../functions/LPE-CT/src/http_routes/run_apt_update_upgrade.md)
- [run_system_power_action](../../../../functions/LPE-CT/src/http_routes/run_system_power_action.md)
- [upload_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/upload_public_tls_profile.md)
- [select_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/select_public_tls_profile.md)
- [delete_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/delete_public_tls_profile.md)
- [update_policies](../../../../functions/LPE-CT/src/http_routes/update_policies.md)
- [update_updates](../../../../functions/LPE-CT/src/http_routes/update_updates.md)
- [reporting_snapshot](../../../../functions/LPE-CT/src/http_routes/reporting_snapshot.md)
- [update_reporting](../../../../functions/LPE-CT/src/http_routes/update_reporting.md)
- [run_digest_reports](../../../../functions/LPE-CT/src/http_routes/run_digest_reports.md)
- [digest_reports](../../../../functions/LPE-CT/src/http_routes/digest_reports.md)
- [digest_report_details](../../../../functions/LPE-CT/src/http_routes/digest_report_details.md)
- [system_diagnostic_services](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_services.md)
- [system_diagnostic_service_action](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_service_action.md)
- [system_diagnostic_report](../../../../functions/LPE-CT/src/http_routes/system_diagnostic_report.md)
- [system_health_check](../../../../functions/LPE-CT/src/http_routes/system_health_check.md)
- [run_system_tool](../../../../functions/LPE-CT/src/http_routes/run_system_tool.md)
- [run_spam_test](../../../../functions/LPE-CT/src/http_routes/run_spam_test.md)
- [connect_lpe_support](../../../../functions/LPE-CT/src/http_routes/connect_lpe_support.md)
- [flush_mail_queue](../../../../functions/LPE-CT/src/http_routes/flush_mail_queue.md)