---
type: Rust Module
title: console
resource: crates/lpe-admin-api/src/console.rs#L1-L758
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-multipart-path-as-axumpath-state-http-headermap-statuscode-json
  - external/lpe-storage-normalize-mailbox-email-accountcredentialinput-admincredentialinput-admindashboard-auditentryinput-dashboardupdate-emailtraceresult-emailtracesearchinput-localaisettings-newaccount-newalias-newdomain-newmailbox-newpsttransferjob-newserveradministrator-pstjobexecutionsummary-securitysettings-serversettings-storage-updateaccount-updatedomain
  - external/tokio-io-asyncwriteext
  - external/uuid-uuid
  - external/crate-http-bad-request-error-internal-error-pst-pst-import-dir-sanitize-upload-filename-validate-uploaded-pst-file-require-admin-security-hash-password-types-apiresult-attachmentsupportresponse-createaccountrequest-createaliasrequest-createdomainrequest-createfilterrulerequest-createmailboxrequest-createpsttransferjobrequest-createserveradministratorrequest-emailtracesearchrequest-localaihealthresponse-mailflowresponse-updateaccountrequest-updateantispamsettingsrequest-updatedomainrequest-updatelocalaisettingsrequest-updatesecuritysettingsrequest-updateserversettingsrequest-util-ensure-admin-can-manage-email-mailbox-account-email
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [local_ai_health](../../../../functions/crates/lpe-admin-api/src/console/local_ai_health.md)
- [attachment_support](../../../../functions/crates/lpe-admin-api/src/console/attachment_support.md)
- [dashboard](../../../../functions/crates/lpe-admin-api/src/console/dashboard.md)
- [create_account](../../../../functions/crates/lpe-admin-api/src/console/create_account.md)
- [update_account](../../../../functions/crates/lpe-admin-api/src/console/update_account.md)
- [create_mailbox](../../../../functions/crates/lpe-admin-api/src/console/create_mailbox.md)
- [create_pst_transfer_job](../../../../functions/crates/lpe-admin-api/src/console/create_pst_transfer_job.md)
- [upload_pst_import](../../../../functions/crates/lpe-admin-api/src/console/upload_pst_import.md)
- [create_domain](../../../../functions/crates/lpe-admin-api/src/console/create_domain.md)
- [update_domain](../../../../functions/crates/lpe-admin-api/src/console/update_domain.md)
- [create_alias](../../../../functions/crates/lpe-admin-api/src/console/create_alias.md)
- [update_server_settings](../../../../functions/crates/lpe-admin-api/src/console/update_server_settings.md)
- [update_security_settings](../../../../functions/crates/lpe-admin-api/src/console/update_security_settings.md)
- [update_local_ai_settings](../../../../functions/crates/lpe-admin-api/src/console/update_local_ai_settings.md)
- [update_antispam_settings](../../../../functions/crates/lpe-admin-api/src/console/update_antispam_settings.md)
- [create_server_administrator](../../../../functions/crates/lpe-admin-api/src/console/create_server_administrator.md)
- [create_filter_rule](../../../../functions/crates/lpe-admin-api/src/console/create_filter_rule.md)
- [search_email_trace](../../../../functions/crates/lpe-admin-api/src/console/search_email_trace.md)
- [run_pst_jobs](../../../../functions/crates/lpe-admin-api/src/console/run_pst_jobs.md)
- [mail_flow](../../../../functions/crates/lpe-admin-api/src/console/mail_flow.md)

# Imports

- `axum::{
    extract::{Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_storage::{
    normalize_mailbox_email, AccountCredentialInput, AdminCredentialInput, AdminDashboard,
    AuditEntryInput, DashboardUpdate, EmailTraceResult, EmailTraceSearchInput, LocalAiSettings,
    NewAccount, NewAlias, NewDomain, NewMailbox, NewPstTransferJob, NewServerAdministrator,
    PstJobExecutionSummary, SecuritySettings, ServerSettings, Storage, UpdateAccount, UpdateDomain,
}`
- `tokio::io::AsyncWriteExt`
- `uuid::Uuid`
- `crate::{
    http::{bad_request_error, internal_error},
    pst::{pst_import_dir, sanitize_upload_filename, validate_uploaded_pst_file},
    require_admin,
    security::hash_password,
    types::{
        ApiResult, AttachmentSupportResponse, CreateAccountRequest, CreateAliasRequest,
        CreateDomainRequest, CreateFilterRuleRequest, CreateMailboxRequest,
        CreatePstTransferJobRequest, CreateServerAdministratorRequest, EmailTraceSearchRequest,
        LocalAiHealthResponse, MailFlowResponse, UpdateAccountRequest,
        UpdateAntispamSettingsRequest, UpdateDomainRequest, UpdateLocalAiSettingsRequest,
        UpdateSecuritySettingsRequest, UpdateServerSettingsRequest,
    },
    util::{ensure_admin_can_manage_email, mailbox_account_email},
}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)