use crate::{
    bad_request_error,
    http::internal_error,
    parse_collaboration_kind, parse_sender_delegation_right, require_account,
    types::{
        ApiResult, CollaborationOverviewResponse, MailboxDelegationResponse,
        UpsertCollaborationGrantRequest, UpsertMailboxDelegationGrantRequest,
        UpsertSenderDelegationGrantRequest, UpsertTaskListGrantRequest,
    },
};
use axum::{
    extract::{Path as AxumPath, State},
    http::HeaderMap,
    Json,
};
use lpe_storage::{
    AuditEntryInput, CollaborationGrantInput, CollaborationResourceKind, HealthResponse,
    MailboxDelegationGrantInput, SenderDelegationGrant, SenderDelegationGrantInput, Storage,
    TaskListGrantInput,
};
use uuid::Uuid;

pub(crate) async fn list_collaboration_overview(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<CollaborationOverviewResponse> {
    let account = require_account(&storage, &headers).await?;
    let incoming_contact_collections = storage
        .fetch_accessible_contact_collections(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|collection| !collection.is_owned)
        .collect();
    let incoming_calendar_collections = storage
        .fetch_accessible_calendar_collections(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|collection| !collection.is_owned)
        .collect();
    let incoming_task_list_collections = storage
        .fetch_accessible_task_list_collections(account.account_id)
        .await
        .map_err(internal_error)?;

    Ok(Json(CollaborationOverviewResponse {
        outgoing_contacts: storage
            .fetch_outgoing_collaboration_grants(
                account.account_id,
                CollaborationResourceKind::Contacts,
            )
            .await
            .map_err(internal_error)?,
        outgoing_calendars: storage
            .fetch_outgoing_collaboration_grants(
                account.account_id,
                CollaborationResourceKind::Calendar,
            )
            .await
            .map_err(internal_error)?,
        outgoing_task_lists: storage
            .fetch_outgoing_task_list_grants(account.account_id)
            .await
            .map_err(internal_error)?,
        incoming_contact_collections,
        incoming_calendar_collections,
        incoming_task_list_collections,
    }))
}

pub(crate) async fn upsert_collaboration_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertCollaborationGrantRequest>,
) -> ApiResult<lpe_storage::CollaborationGrant> {
    let account = require_account(&storage, &headers).await?;
    let kind = parse_collaboration_kind(&request.kind).map_err(bad_request_error)?;
    Ok(Json(
        storage
            .upsert_collaboration_grant(
                CollaborationGrantInput {
                    kind,
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                    may_read: request.may_read,
                    may_write: request.may_write,
                    may_delete: request.may_delete,
                    may_share: request.may_share,
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: format!("collaboration-share-upsert:{}", kind.as_str()),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_collaboration_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((kind, grantee_account_id)): AxumPath<(String, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let kind = parse_collaboration_kind(&kind).map_err(bad_request_error)?;
    storage
        .delete_collaboration_grant(
            account.account_id,
            kind,
            grantee_account_id,
            AuditEntryInput {
                actor: account.email,
                action: format!("collaboration-share-delete:{}", kind.as_str()),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn upsert_task_list_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(task_list_id): AxumPath<Uuid>,
    Json(request): Json<UpsertTaskListGrantRequest>,
) -> ApiResult<lpe_storage::TaskListGrant> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_task_list_grant(
                TaskListGrantInput {
                    owner_account_id: account.account_id,
                    task_list_id,
                    grantee_email: request.grantee_email.clone(),
                    may_read: request.may_read,
                    may_write: request.may_write,
                    may_delete: request.may_delete,
                    may_share: request.may_share,
                },
                AuditEntryInput {
                    actor: account.email,
                    action: "task-list-share-upsert".to_string(),
                    subject: format!("{task_list_id}:{}", request.grantee_email),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_task_list_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((task_list_id, grantee_account_id)): AxumPath<(Uuid, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_task_list_grant(
            account.account_id,
            task_list_id,
            grantee_account_id,
            AuditEntryInput {
                actor: account.email,
                action: "task-list-share-delete".to_string(),
                subject: format!("{task_list_id}:{grantee_account_id}"),
            },
        )
        .await
        .map_err(bad_request_error)?;

    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn get_mailbox_delegation(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<MailboxDelegationResponse> {
    let account = require_account(&storage, &headers).await?;
    let incoming_mailboxes = storage
        .fetch_accessible_mailbox_accounts(account.account_id)
        .await
        .map_err(internal_error)?
        .into_iter()
        .filter(|entry| !entry.is_owned)
        .collect();
    let overview = lpe_storage::MailboxDelegationOverview {
        outgoing_mailboxes: storage
            .fetch_outgoing_mailbox_delegation_grants(account.account_id)
            .await
            .map_err(internal_error)?,
        incoming_mailboxes,
        outgoing_sender_rights: storage
            .fetch_outgoing_sender_delegation_grants(account.account_id)
            .await
            .map_err(internal_error)?,
    };
    Ok(Json(MailboxDelegationResponse { overview }))
}

pub(crate) async fn upsert_mailbox_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertMailboxDelegationGrantRequest>,
) -> ApiResult<lpe_storage::MailboxDelegationGrant> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .upsert_mailbox_delegation_grant(
                MailboxDelegationGrantInput {
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                    may_write: request.may_write,
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: "mailbox-delegation-upsert".to_string(),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_mailbox_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(grantee_account_id): AxumPath<Uuid>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    storage
        .delete_mailbox_delegation_grant(
            account.account_id,
            grantee_account_id,
            AuditEntryInput {
                actor: account.email,
                action: "mailbox-delegation-delete".to_string(),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn upsert_sender_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertSenderDelegationGrantRequest>,
) -> ApiResult<SenderDelegationGrant> {
    let account = require_account(&storage, &headers).await?;
    let sender_right =
        parse_sender_delegation_right(&request.sender_right).map_err(bad_request_error)?;
    Ok(Json(
        storage
            .upsert_sender_delegation_grant(
                SenderDelegationGrantInput {
                    owner_account_id: account.account_id,
                    grantee_email: request.grantee_email.clone(),
                    sender_right,
                },
                AuditEntryInput {
                    actor: account.email.clone(),
                    action: format!("sender-delegation-upsert:{}", sender_right.as_str()),
                    subject: request.grantee_email,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn delete_sender_delegation_grant(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath((sender_right, grantee_account_id)): AxumPath<(String, Uuid)>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let sender_right = parse_sender_delegation_right(&sender_right).map_err(bad_request_error)?;
    storage
        .delete_sender_delegation_grant(
            account.account_id,
            grantee_account_id,
            sender_right,
            AuditEntryInput {
                actor: account.email,
                action: format!("sender-delegation-delete:{}", sender_right.as_str()),
                subject: grantee_account_id.to_string(),
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}
