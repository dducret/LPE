---
type: Rust Module
title: delegation
resource: crates/lpe-admin-api/src/delegation.rs#L1-L416
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-bad-request-error-http-internal-error-parse-collaboration-kind-parse-sender-delegation-right-require-account-types-apiresult-collaborationoverviewresponse-freebusyquery-freebusyresponse-mailboxdelegationresponse-upsertcollaborationgrantrequest-upsertmailboxdelegationgrantrequest-upsertsenderdelegationgrantrequest-upserttasklistgrantrequest
  - external/axum-extract-path-as-axumpath-query-state-http-headermap-json
  - external/lpe-storage-auditentryinput-collaborationgrantinput-collaborationresourcekind-healthresponse-mailboxdelegationgrantinput-senderdelegationgrant-senderdelegationgrantinput-storage-tasklistgrantinput
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [list_collaboration_overview](../../../../functions/crates/lpe-admin-api/src/delegation/list_collaboration_overview.md)
- [upsert_collaboration_grant](../../../../functions/crates/lpe-admin-api/src/delegation/upsert_collaboration_grant.md)
- [upsert_calendar_collection_grant](../../../../functions/crates/lpe-admin-api/src/delegation/upsert_calendar_collection_grant.md)
- [delete_collaboration_grant](../../../../functions/crates/lpe-admin-api/src/delegation/delete_collaboration_grant.md)
- [delete_calendar_collection_grant](../../../../functions/crates/lpe-admin-api/src/delegation/delete_calendar_collection_grant.md)
- [upsert_task_list_grant](../../../../functions/crates/lpe-admin-api/src/delegation/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../functions/crates/lpe-admin-api/src/delegation/delete_task_list_grant.md)
- [get_mailbox_delegation](../../../../functions/crates/lpe-admin-api/src/delegation/get_mailbox_delegation.md)
- [get_free_busy](../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)
- [upsert_mailbox_delegation_grant](../../../../functions/crates/lpe-admin-api/src/delegation/upsert_mailbox_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../functions/crates/lpe-admin-api/src/delegation/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../functions/crates/lpe-admin-api/src/delegation/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../functions/crates/lpe-admin-api/src/delegation/delete_sender_delegation_grant.md)

# Imports

- `crate::{
    bad_request_error,
    http::internal_error,
    parse_collaboration_kind, parse_sender_delegation_right, require_account,
    types::{
        ApiResult, CollaborationOverviewResponse, FreeBusyQuery, FreeBusyResponse,
        MailboxDelegationResponse, UpsertCollaborationGrantRequest,
        UpsertMailboxDelegationGrantRequest, UpsertSenderDelegationGrantRequest,
        UpsertTaskListGrantRequest,
    },
}`
- `axum::{
    extract::{Path as AxumPath, Query, State},
    http::HeaderMap,
    Json,
}`
- `lpe_storage::{
    AuditEntryInput, CollaborationGrantInput, CollaborationResourceKind, HealthResponse,
    MailboxDelegationGrantInput, SenderDelegationGrant, SenderDelegationGrantInput, Storage,
    TaskListGrantInput,
}`
- `uuid::Uuid`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)