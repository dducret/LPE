---
type: Rust Module
title: sieve
resource: crates/lpe-admin-api/src/sieve.rs#L1-L158
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-bad-request-error-http-internal-error-require-account-types-apiresult-renamesievescriptrequest-setactivesievescriptrequest-sieveoverviewresponse-upsertsievescriptrequest
  - external/axum-extract-path-as-axumpath-state-http-headermap-statuscode-json
  - external/lpe-storage-auditentryinput-healthresponse-mailboxrule-sievescriptdocument-storage
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [list_mailbox_rules](../../../../functions/crates/lpe-admin-api/src/sieve/list_mailbox_rules.md)
- [get_sieve_overview](../../../../functions/crates/lpe-admin-api/src/sieve/get_sieve_overview.md)
- [get_sieve_script](../../../../functions/crates/lpe-admin-api/src/sieve/get_sieve_script.md)
- [put_sieve_script](../../../../functions/crates/lpe-admin-api/src/sieve/put_sieve_script.md)
- [rename_sieve_script](../../../../functions/crates/lpe-admin-api/src/sieve/rename_sieve_script.md)
- [set_active_sieve_script](../../../../functions/crates/lpe-admin-api/src/sieve/set_active_sieve_script.md)
- [delete_sieve_script](../../../../functions/crates/lpe-admin-api/src/sieve/delete_sieve_script.md)

# Imports

- `crate::{
    bad_request_error,
    http::internal_error,
    require_account,
    types::{
        ApiResult, RenameSieveScriptRequest, SetActiveSieveScriptRequest, SieveOverviewResponse,
        UpsertSieveScriptRequest,
    },
}`
- `axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_storage::{AuditEntryInput, HealthResponse, MailboxRule, SieveScriptDocument, Storage}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)