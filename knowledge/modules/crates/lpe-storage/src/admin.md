---
type: Rust Module
title: admin
resource: crates/lpe-storage/src/admin.rs#L1-L1418
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-core-sieve-parse-script
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-env-bind-address-env-hostname-normalize-admin-permissions-normalize-email-permission-summary-permissions-from-storage-validate-sieve-script-content-validate-sieve-script-name-antispamsettings-auditentryinput-canonicalchangecategory-dashboardupdate-emailtraceresult-emailtracerow-emailtracesearchinput-filterrule-localaisettings-mailflowentry-mailflowrow-mailboxrule-newserveradministrator-outlookprofilestate-securitysettings-serveradministrator-serveradministratorrow-serversettings-sievescriptdocument-sievescriptsummary-storage-max-sieve-scripts-per-account-platform-tenant-id
  - external/helpers-count-from-row-mailbox-rule-summaries-map-email-trace-row-map-mail-flow-row-unsupported-client-local-profile-state-unsupported-exchange-rule-features
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [record_platform_audit](../../../../functions/crates/lpe-storage/src/admin/Storage/record_platform_audit.md)
- [create_server_administrator](../../../../functions/crates/lpe-storage/src/admin/Storage/create_server_administrator.md)
- [find_server_administrator_by_email](../../../../functions/crates/lpe-storage/src/admin/Storage/find_server_administrator_by_email.md)
- [append_audit_event](../../../../functions/crates/lpe-storage/src/admin/Storage/append_audit_event.md)
- [list_sieve_scripts](../../../../functions/crates/lpe-storage/src/admin/Storage/list_sieve_scripts.md)
- [list_mailbox_rules](../../../../functions/crates/lpe-storage/src/admin/Storage/list_mailbox_rules.md)
- [fetch_outlook_profile_state](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_outlook_profile_state.md)
- [fetch_mapi_ipm_subtree_ost_id](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_mapi_ipm_subtree_ost_id.md)
- [store_mapi_ipm_subtree_ost_id](../../../../functions/crates/lpe-storage/src/admin/Storage/store_mapi_ipm_subtree_ost_id.md)
- [get_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/get_sieve_script.md)
- [put_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/put_sieve_script.md)
- [delete_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/delete_sieve_script.md)
- [rename_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/rename_sieve_script.md)
- [set_active_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/set_active_sieve_script.md)
- [fetch_active_sieve_script](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_active_sieve_script.md)
- [create_filter_rule](../../../../functions/crates/lpe-storage/src/admin/Storage/create_filter_rule.md)
- [update_settings](../../../../functions/crates/lpe-storage/src/admin/Storage/update_settings.md)
- [fetch_mail_flow_entries](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_mail_flow_entries.md)
- [search_email_trace](../../../../functions/crates/lpe-storage/src/admin/Storage/search_email_trace.md)
- [fetch_server_settings](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings.md)
- [fetch_security_settings](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_security_settings.md)
- [fetch_local_ai_settings](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_local_ai_settings.md)
- [fetch_antispam_settings](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_settings.md)
- [fetch_server_administrators](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_administrators.md)
- [fetch_antispam_rules](../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_antispam_rules.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_core::sieve::parse_script`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    env_bind_address, env_hostname, normalize_admin_permissions, normalize_email,
    permission_summary, permissions_from_storage, validate_sieve_script_content,
    validate_sieve_script_name, AntispamSettings, AuditEntryInput, CanonicalChangeCategory,
    DashboardUpdate, EmailTraceResult, EmailTraceRow, EmailTraceSearchInput, FilterRule,
    LocalAiSettings, MailFlowEntry, MailFlowRow, MailboxRule, NewServerAdministrator,
    OutlookProfileState, SecuritySettings, ServerAdministrator, ServerAdministratorRow,
    ServerSettings, SieveScriptDocument, SieveScriptSummary, Storage,
    MAX_SIEVE_SCRIPTS_PER_ACCOUNT, PLATFORM_TENANT_ID,
}`
- `helpers::{
    count_from_row, mailbox_rule_summaries, map_email_trace_row, map_mail_flow_row,
    unsupported_client_local_profile_state, unsupported_exchange_rule_features,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)