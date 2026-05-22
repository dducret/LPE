use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use lpe_domain::{MailboxDisplayName, MailboxNamePolicy, MailboxPath};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    shared::allocate_uid_validity,
    submission,
    submission::{AttachmentUploadInput, SubmittedRecipientInput},
    util::{canonical_system_mailbox_display_name, system_mailbox_role_for_display_name},
    AccountQuotaRow, ActiveSyncDeviceRow, ActiveSyncSyncStateRow, AuditEntryInput,
    CanonicalChangeCategory, ImapEmailRow, JmapEmailRecipientRow, JmapEmailRow,
    JmapEmailSubmissionRow, JmapMailboxRow, JmapUploadBlobRow, MessageBccRecipientRecordRow,
    SearchFolderRow, Storage, DEFAULT_TASK_LIST_ROLE,
};

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncDeviceState {
    pub account_id: Uuid,
    pub device_id: String,
    pub device_type: String,
    pub policy_key: Option<String>,
    pub pending_policy_key: Option<String>,
    pub provision_status: String,
    pub wipe_status: String,
    pub account_wipe_status: String,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncSyncState {
    pub sync_key: String,
    pub snapshot_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncItemState {
    pub id: Uuid,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachment {
    pub id: Uuid,
    pub message_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub size_octets: u64,
    pub file_reference: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachmentContent {
    pub file_reference: String,
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapMailbox {
    pub id: Uuid,
    pub parent_id: Option<Uuid>,
    pub role: String,
    pub name: String,
    pub sort_order: i32,
    pub modseq: u64,
    pub total_emails: u32,
    pub unread_emails: u32,
    pub is_subscribed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchFolderDefinition {
    pub id: Uuid,
    pub account_id: Uuid,
    pub role: String,
    pub display_name: String,
    pub definition_kind: String,
    pub result_object_kind: String,
    pub scope_json: Value,
    pub restriction_json: Value,
    pub excluded_folder_roles: Vec<String>,
    pub is_builtin: bool,
}

struct BuiltinSearchFolderDefinition {
    role: &'static str,
    display_name: &'static str,
    result_object_kind: &'static str,
    scope_json: Value,
    restriction_json: Value,
    excluded_folder_roles: Vec<String>,
}

fn exchange_builtin_search_folder_definitions() -> Vec<BuiltinSearchFolderDefinition> {
    let top_ipm_scope = serde_json::json!({
        "scope": "top_of_personal_folders",
        "recursive": true
    });
    let excluded_mail_roles = vec![
        "trash",
        "junk",
        "drafts",
        "outbox",
        "conflicts",
        "local_failures",
        "server_failures",
        "sync_issues",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();

    vec![
        BuiltinSearchFolderDefinition {
            role: "reminders",
            display_name: "Reminders",
            result_object_kind: "mixed",
            scope_json: top_ipm_scope.clone(),
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "match": "reminder_set_or_recurring"
            }),
            excluded_folder_roles: excluded_mail_roles.clone(),
        },
        BuiltinSearchFolderDefinition {
            role: "todo_search",
            display_name: "To-Do",
            result_object_kind: "mixed",
            scope_json: top_ipm_scope.clone(),
            restriction_json: serde_json::json!({
                "kind": "exchange_todo"
            }),
            excluded_folder_roles: excluded_mail_roles.clone(),
        },
        BuiltinSearchFolderDefinition {
            role: "contacts_search",
            display_name: "Contacts Search",
            result_object_kind: "contact",
            scope_json: serde_json::json!({
                "scope": "contacts_folders",
                "recursive": false
            }),
            restriction_json: serde_json::json!({
                "kind": "exchange_contacts_search"
            }),
            excluded_folder_roles: Vec::new(),
        },
        BuiltinSearchFolderDefinition {
            role: "tracked_mail_processing",
            display_name: "Tracked Mail Processing",
            result_object_kind: "message",
            scope_json: top_ipm_scope,
            restriction_json: serde_json::json!({
                "kind": "exchange_tracked_mail_processing"
            }),
            excluded_folder_roles: excluded_mail_roles,
        },
    ]
}

fn map_search_folder(row: SearchFolderRow) -> SearchFolderDefinition {
    SearchFolderDefinition {
        id: row.id,
        account_id: row.account_id,
        role: row.role,
        display_name: row.display_name,
        definition_kind: row.definition_kind,
        result_object_kind: row.result_object_kind,
        scope_json: row.scope_json,
        restriction_json: row.restriction_json,
        excluded_folder_roles: row.excluded_folder_roles,
        is_builtin: row.is_builtin,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailAddress {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmail {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub mailbox_ids: Vec<Uuid>,
    pub mailbox_states: Vec<JmapEmailMailboxState>,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub modseq: u64,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
    pub sender_authorization_kind: String,
    pub submitted_by_account_id: Uuid,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub followup_flag_status: String,
    pub followup_icon: i32,
    pub todo_item_flags: i32,
    pub followup_request: String,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub swapped_todo_store_id: Option<Uuid>,
    pub swapped_todo_data: Option<Vec<u8>>,
    pub categories: Vec<String>,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Default)]
pub struct JmapEmailFollowupUpdate {
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
    pub followup_flag_status: Option<String>,
    pub followup_icon: Option<i32>,
    pub todo_item_flags: Option<i32>,
    pub followup_request: Option<String>,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: Option<bool>,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub swapped_todo_store_id: Option<Uuid>,
    pub swapped_todo_data: Option<Vec<u8>>,
    pub categories: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailMailboxState {
    pub mailbox_id: Uuid,
    pub role: String,
    pub name: String,
    pub modseq: u64,
    pub unread: bool,
    pub flagged: bool,
    pub followup_flag_status: String,
    pub followup_icon: i32,
    pub todo_item_flags: i32,
    pub followup_request: String,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub swapped_todo_store_id: Option<Uuid>,
    pub swapped_todo_data: Option<Vec<u8>>,
    pub categories: Vec<String>,
    pub draft: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapEmail {
    pub id: Uuid,
    pub uid: u32,
    pub modseq: u64,
    pub thread_id: Uuid,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub deleted: bool,
    pub keywords: Vec<String>,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub delivery_status: String,
    pub mime_parts: Vec<ImapMimePart>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapMimePart {
    pub part_path: String,
    pub content_type: String,
    pub content_disposition: Option<String>,
    pub content_id: Option<String>,
    pub file_name: Option<String>,
    pub transfer_encoding: Option<String>,
    pub charset_name: Option<String>,
    pub size_octets: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapMailboxState {
    pub uid_validity: u32,
    pub uid_next: u32,
    pub highest_modseq: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapThreadQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapStoredQueryState {
    pub id: Uuid,
    pub account_id: Uuid,
    pub method_name: String,
    pub filter_hash: String,
    pub sort_hash: String,
    pub last_change_sequence: i64,
    pub snapshot_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JmapMailObjectChange {
    pub cursor: i64,
    pub object_id: Uuid,
    pub change_kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JmapStringObjectChange {
    pub cursor: i64,
    pub object_id: String,
    pub change_kind: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailSubmission {
    pub id: Uuid,
    pub email_id: Uuid,
    pub thread_id: Uuid,
    pub identity_id: String,
    pub identity_email: String,
    pub envelope_mail_from: String,
    pub envelope_rcpt_to: Vec<String>,
    pub send_at: String,
    pub undo_status: String,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapQuota {
    pub id: String,
    pub name: String,
    pub used: u64,
    pub hard_limit: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapUploadBlob {
    pub id: Uuid,
    pub account_id: Uuid,
    pub media_type: String,
    pub octet_size: u64,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxCreateInput {
    pub account_id: Uuid,
    pub name: String,
    pub parent_id: Option<Uuid>,
    pub sort_order: Option<i32>,
    pub is_subscribed: bool,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxUpdateInput {
    pub account_id: Uuid,
    pub mailbox_id: Uuid,
    pub name: Option<String>,
    pub parent_id: Option<Option<Uuid>>,
    pub sort_order: Option<i32>,
    pub is_subscribed: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct JmapImportedEmailInput {
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub mailbox_id: Uuid,
    pub source: String,
    pub raw_message: Option<Vec<u8>>,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: String,
    pub size_octets: i64,
    pub received_at: Option<String>,
    pub thread_id: Option<Uuid>,
    pub attachments: Vec<AttachmentUploadInput>,
}

impl Storage {
    pub async fn fetch_jmap_mail_change_cursor(&self, account_id: Uuid) -> Result<Option<i64>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MAX(cursor)
            FROM mail_change_log
            WHERE tenant_id = $1
              AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn replay_jmap_mail_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let earliest_retained_cursor = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(cursor)
            FROM mail_change_log
            WHERE tenant_id = $1
              AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_one(&self.pool)
        .await?;
        if after_cursor > 0
            && earliest_retained_cursor.is_some_and(|cursor| after_cursor < cursor - 1)
        {
            return Ok(None);
        }

        let rows = sqlx::query(
            r#"
            SELECT cursor, object_kind, object_id, mailbox_id, change_kind, summary_json
            FROM mail_change_log
            WHERE tenant_id = $1
              AND cursor > $2
              AND (account_id = $3 OR affected_principal_ids @> ARRAY[$3]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            ORDER BY cursor ASC
            LIMIT $4
            "#,
        )
        .bind(&tenant_id)
        .bind(after_cursor)
        .bind(account_id)
        .bind((max_rows + 1) as i64)
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > max_rows as usize {
            return Ok(None);
        }

        let mut changes = Vec::new();
        for row in rows {
            let cursor: i64 = row.try_get("cursor")?;
            let object_kind: String = row.try_get("object_kind")?;
            let object_id: Uuid = row.try_get("object_id")?;
            let mailbox_id: Option<Uuid> = row.try_get("mailbox_id")?;
            let change_kind: String = row.try_get("change_kind")?;
            let summary_json: Value = row.try_get("summary_json")?;
            match data_type {
                "Email" => {
                    if object_kind != "mailbox_message" {
                        return Ok(None);
                    }
                    let Some(message_id) = summary_json
                        .get("messageId")
                        .and_then(Value::as_str)
                        .and_then(|value| Uuid::parse_str(value).ok())
                    else {
                        return Ok(None);
                    };
                    changes.push(JmapMailObjectChange {
                        cursor,
                        object_id: message_id,
                        change_kind: jmap_change_kind(&change_kind),
                    });
                }
                "Thread" => {
                    if object_kind != "mailbox_message" {
                        return Ok(None);
                    }
                    let Some(thread_id) = summary_json
                        .get("threadId")
                        .and_then(Value::as_str)
                        .and_then(|value| Uuid::parse_str(value).ok())
                    else {
                        return Ok(None);
                    };
                    changes.push(JmapMailObjectChange {
                        cursor,
                        object_id: thread_id,
                        change_kind: jmap_change_kind(&change_kind),
                    });
                }
                "Mailbox" => match object_kind.as_str() {
                    "mailbox" => changes.push(JmapMailObjectChange {
                        cursor,
                        object_id,
                        change_kind: jmap_change_kind(&change_kind),
                    }),
                    "mailbox_message" => {
                        let Some(target_mailbox_id) = mailbox_id else {
                            return Ok(None);
                        };
                        if let Some(source_mailbox_id) = summary_json
                            .get("sourceMailboxId")
                            .and_then(Value::as_str)
                            .and_then(|value| Uuid::parse_str(value).ok())
                        {
                            changes.push(JmapMailObjectChange {
                                cursor,
                                object_id: source_mailbox_id,
                                change_kind: "updated".to_string(),
                            });
                        }
                        changes.push(JmapMailObjectChange {
                            cursor,
                            object_id: target_mailbox_id,
                            change_kind: "updated".to_string(),
                        });
                    }
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            }
        }

        Ok(Some(changes))
    }

    pub async fn fetch_jmap_object_change_cursor(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Option<i64>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let Some(object_kinds) = jmap_object_replay_kinds(data_type) else {
            return Ok(None);
        };
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MAX(cursor)
            FROM mail_change_log
            WHERE tenant_id = $1
              AND object_kind = ANY($3)
              AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(object_kinds)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn replay_jmap_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let Some(object_kinds) = jmap_object_replay_kinds(data_type) else {
            return Ok(None);
        };
        if jmap_exact_object_kind(data_type).is_none() {
            return Ok(None);
        }
        let earliest_retained_cursor = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(cursor)
            FROM mail_change_log
            WHERE tenant_id = $1
              AND object_kind = ANY($3)
              AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(object_kinds.clone())
        .fetch_one(&self.pool)
        .await?;
        if after_cursor > 0
            && earliest_retained_cursor.is_some_and(|cursor| after_cursor < cursor - 1)
        {
            return Ok(None);
        }

        let rows = sqlx::query(
            r#"
            SELECT cursor, object_kind, object_id, change_kind, summary_json
            FROM mail_change_log
            WHERE tenant_id = $1
              AND cursor > $2
              AND object_kind = ANY($4)
              AND (account_id = $3 OR affected_principal_ids @> ARRAY[$3]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            ORDER BY cursor ASC
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(after_cursor)
        .bind(account_id)
        .bind(object_kinds)
        .bind((max_rows + 1) as i64)
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > max_rows as usize {
            return Ok(None);
        }

        let mut changes = Vec::new();
        for row in rows {
            let object_kind: String = row.try_get("object_kind")?;
            let summary_json: Value = row.try_get("summary_json")?;
            if let Some(replay_object_id) = jmap_replay_object_id(
                data_type,
                &object_kind,
                row.try_get("object_id")?,
                &summary_json,
            ) {
                changes.push(JmapMailObjectChange {
                    cursor: row.try_get("cursor")?,
                    object_id: replay_object_id,
                    change_kind: jmap_change_kind(&row.try_get::<String, _>("change_kind")?),
                });
                continue;
            }

            let Some(replay_object_ids) = self
                .expand_jmap_dependency_change(
                    &tenant_id,
                    data_type,
                    &object_kind,
                    row.try_get("object_id")?,
                    &summary_json,
                )
                .await?
            else {
                return Ok(None);
            };
            let change_kind = jmap_change_kind(&row.try_get::<String, _>("change_kind")?);
            for replay_object_id in replay_object_ids {
                changes.push(JmapMailObjectChange {
                    cursor: row.try_get("cursor")?,
                    object_id: replay_object_id,
                    change_kind: change_kind.clone(),
                });
            }
        }

        Ok(Some(changes))
    }

    pub async fn replay_jmap_string_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapStringObjectChange>>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let Some(object_kinds) = jmap_object_replay_kinds(data_type) else {
            return Ok(None);
        };
        let earliest_retained_cursor = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT MIN(cursor)
            FROM mail_change_log
            WHERE tenant_id = $1
              AND object_kind = ANY($3)
              AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(object_kinds.clone())
        .fetch_one(&self.pool)
        .await?;
        if after_cursor > 0
            && earliest_retained_cursor.is_some_and(|cursor| after_cursor < cursor - 1)
        {
            return Ok(None);
        }

        let rows = sqlx::query(
            r#"
            SELECT cursor, object_kind, object_id, change_kind, summary_json
            FROM mail_change_log
            WHERE tenant_id = $1
              AND cursor > $2
              AND object_kind = ANY($4)
              AND (account_id = $3 OR affected_principal_ids @> ARRAY[$3]::uuid[])
              AND (retained_until IS NULL OR retained_until > NOW())
            ORDER BY cursor ASC
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(after_cursor)
        .bind(account_id)
        .bind(object_kinds)
        .bind((max_rows + 1) as i64)
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > max_rows as usize {
            return Ok(None);
        }

        let mut changes = Vec::new();
        for row in rows {
            let object_kind: String = row.try_get("object_kind")?;
            let summary_json: Value = row.try_get("summary_json")?;
            let object_id = row.try_get("object_id")?;
            let Some(replay_object_id) = self
                .jmap_string_replay_object_id(
                    &tenant_id,
                    data_type,
                    &object_kind,
                    object_id,
                    &summary_json,
                )
                .await?
            else {
                continue;
            };
            changes.push(JmapStringObjectChange {
                cursor: row.try_get("cursor")?,
                object_id: replay_object_id,
                change_kind: jmap_change_kind(&row.try_get::<String, _>("change_kind")?),
            });
        }

        Ok(Some(changes))
    }

    async fn expand_jmap_dependency_change(
        &self,
        tenant_id: &Uuid,
        data_type: &str,
        object_kind: &str,
        object_id: Uuid,
        summary_json: &Value,
    ) -> Result<Option<Vec<Uuid>>> {
        let collection_id = match object_kind {
            "contact_book" | "calendar" | "task_list" => object_id,
            "contact_book_grant" | "calendar_grant" | "task_list_grant" => {
                let Some(collection_id) = summary_json
                    .get("collectionId")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                else {
                    return Ok(None);
                };
                collection_id
            }
            _ => return Ok(None),
        };

        let rows = match (data_type, object_kind) {
            ("ContactCard", "contact_book" | "contact_book_grant") => {
                sqlx::query_scalar::<_, Uuid>(
                    r#"
                    SELECT id
                    FROM contacts
                    WHERE tenant_id = $1
                      AND contact_book_id = $2
                    ORDER BY id ASC
                    "#,
                )
                .bind(tenant_id)
                .bind(collection_id)
                .fetch_all(&self.pool)
                .await?
            }
            ("CalendarEvent", "calendar" | "calendar_grant") => {
                sqlx::query_scalar::<_, Uuid>(
                    r#"
                    SELECT id
                    FROM calendar_events
                    WHERE tenant_id = $1
                      AND calendar_id = $2
                    ORDER BY id ASC
                    "#,
                )
                .bind(tenant_id)
                .bind(collection_id)
                .fetch_all(&self.pool)
                .await?
            }
            ("Task", "task_list" | "task_list_grant") => {
                sqlx::query_scalar::<_, Uuid>(
                    r#"
                    SELECT id
                    FROM tasks
                    WHERE tenant_id = $1
                      AND task_list_id = $2
                    ORDER BY id ASC
                    "#,
                )
                .bind(tenant_id)
                .bind(collection_id)
                .fetch_all(&self.pool)
                .await?
            }
            _ => return Ok(None),
        };
        Ok(Some(rows))
    }

    async fn jmap_string_replay_object_id(
        &self,
        tenant_id: &Uuid,
        data_type: &str,
        object_kind: &str,
        object_id: Uuid,
        summary_json: &Value,
    ) -> Result<Option<String>> {
        match (data_type, object_kind) {
            ("Share", "mailbox_delegation_grant") => Ok(Some(format!("mailbox:{object_id}"))),
            ("Share", "sender_right") => Ok(Some(format!("sender:{object_id}"))),
            ("Share", "contact_book_grant") => Ok(Some(format!("contacts:{object_id}"))),
            ("Share", "calendar_grant") => Ok(Some(format!("calendar:{object_id}"))),
            ("Share", "task_list_grant") => {
                let share_type = if let Some(collection_id) = summary_json
                    .get("collectionId")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                {
                    self.task_share_type_for_collection(tenant_id, collection_id)
                        .await?
                } else {
                    "taskList"
                };
                Ok(Some(format!("{share_type}:{object_id}")))
            }
            ("Reminder", "task") if summary_json_reminder_changed(summary_json) => {
                Ok(Some(format!("task:{object_id}")))
            }
            ("Reminder", "calendar_event") if summary_json_reminder_changed(summary_json) => {
                Ok(Some(format!("calendar:{object_id}")))
            }
            ("Reminder", "mailbox_message") if summary_json_reminder_changed(summary_json) => {
                let Some(message_id) = summary_json
                    .get("messageId")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                else {
                    return Ok(None);
                };
                Ok(Some(format!("mail:{message_id}")))
            }
            _ => Ok(None),
        }
    }

    async fn task_share_type_for_collection(
        &self,
        tenant_id: &Uuid,
        task_list_id: Uuid,
    ) -> Result<&'static str> {
        let role = sqlx::query_scalar::<_, Option<String>>(
            r#"
            SELECT role
            FROM task_lists
            WHERE tenant_id = $1
              AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(task_list_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(if role.as_deref() == Some(DEFAULT_TASK_LIST_ROLE) {
            "tasks"
        } else {
            "taskList"
        })
    }

    pub async fn save_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
        last_change_sequence: i64,
        snapshot_ids: &[String],
    ) -> Result<Uuid> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let state_id = Uuid::new_v4();
        let snapshot_json = serde_json::to_value(snapshot_ids)?;
        sqlx::query(
            r#"
            INSERT INTO jmap_query_states (
                id, tenant_id, account_id, method_name, filter_hash, sort_hash,
                last_change_sequence, snapshot_ids, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(state_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(method_name)
        .bind(jmap_query_hash(filter.as_ref())?)
        .bind(jmap_query_hash(sort.as_ref())?)
        .bind(last_change_sequence.max(0))
        .bind(snapshot_json)
        .execute(&self.pool)
        .await?;

        Ok(state_id)
    }

    pub async fn fetch_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        state_id: Uuid,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
    ) -> Result<Option<JmapStoredQueryState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let Some(row) = sqlx::query(
            r#"
            SELECT id, account_id, method_name, filter_hash, sort_hash,
                   last_change_sequence, snapshot_ids
            FROM jmap_query_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND method_name = $3
              AND id = $4
              AND filter_hash = $5
              AND sort_hash = $6
              AND expires_at > NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(method_name)
        .bind(state_id)
        .bind(jmap_query_hash(filter.as_ref())?)
        .bind(jmap_query_hash(sort.as_ref())?)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        let snapshot_json: Value = row.try_get("snapshot_ids")?;
        Ok(Some(JmapStoredQueryState {
            id: row.try_get("id")?,
            account_id: row.try_get("account_id")?,
            method_name: row.try_get("method_name")?,
            filter_hash: row.try_get("filter_hash")?,
            sort_hash: row.try_get("sort_hash")?,
            last_change_sequence: row.try_get("last_change_sequence")?,
            snapshot_ids: serde_json::from_value(snapshot_json)?,
        }))
    }

    pub async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapMailboxRow>(
            r#"
            SELECT
                mb.id,
                mb.parent_mailbox_id,
                mb.role,
                mb.display_name,
                mb.sort_order,
                mb.modseq,
                mb.total_messages::bigint AS total_emails,
                mb.unread_messages::bigint AS unread_emails,
                COALESCE(ms.is_subscribed, TRUE) AS is_subscribed
            FROM mailboxes mb
            LEFT JOIN mailbox_subscriptions ms
              ON ms.tenant_id = mb.tenant_id
             AND ms.mailbox_account_id = mb.account_id
             AND ms.mailbox_id = mb.id
             AND ms.subscriber_account_id = $2
            WHERE mb.tenant_id = $1
              AND mb.account_id = $2
            ORDER BY mb.sort_order ASC, lower(mb.display_name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(JmapMailbox {
                    id: row.id,
                    parent_id: row.parent_mailbox_id,
                    role: row.role,
                    name: row.display_name,
                    sort_order: row.sort_order,
                    modseq: u64::try_from(row.modseq)
                        .map_err(|_| anyhow!("mailbox modseq is out of range"))?,
                    total_emails: row.total_emails.max(0) as u32,
                    unread_emails: row.unread_emails.max(0) as u32,
                    is_subscribed: row.is_subscribed,
                })
            })
            .collect()
    }

    pub async fn fetch_search_folders(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<SearchFolderDefinition>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, SearchFolderRow>(
            r#"
            SELECT
                id,
                account_id,
                role,
                display_name,
                definition_kind,
                result_object_kind,
                scope_json,
                restriction_json,
                excluded_folder_roles,
                is_builtin
            FROM search_folders
            WHERE tenant_id = $1
              AND account_id = $2
            ORDER BY is_builtin DESC, display_name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_search_folder).collect())
    }

    pub async fn ensure_imap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;
        self.ensure_exchange_search_folders(&mut tx, &tenant_id, account_id)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "inbox", "INBOX", 0, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "drafts", "Drafts", 10, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "sent", "Sent", 20, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "trash", "Trash", 30, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "junk", "Junk", 40, 365)
            .await?;
        self.ensure_mailbox(
            &mut tx, &tenant_id, account_id, "archive", "Archive", 50, 365,
        )
        .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "outbox", "Outbox", 60, 365)
            .await?;
        self.ensure_mailbox(
            &mut tx,
            &tenant_id,
            account_id,
            "conversation_history",
            "Conversation History",
            70,
            365,
        )
        .await?;
        self.ensure_mailbox(
            &mut tx,
            &tenant_id,
            account_id,
            "rss_feeds",
            "RSS Feeds",
            80,
            365,
        )
        .await?;
        let sync_issues = self
            .ensure_mailbox(
                &mut tx,
                &tenant_id,
                account_id,
                "sync_issues",
                "Sync Issues",
                90,
                365,
            )
            .await?;
        for (role, display_name, sort_order) in [
            ("conflicts", "Conflicts", 91),
            ("local_failures", "Local Failures", 92),
            ("server_failures", "Server Failures", 93),
        ] {
            let mailbox_id = self
                .ensure_mailbox(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    role,
                    display_name,
                    sort_order,
                    365,
                )
                .await?;
            sqlx::query(
                r#"
                UPDATE mailboxes
                SET parent_mailbox_id = $4
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(sync_issues)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        self.fetch_jmap_mailboxes(account_id).await
    }

    async fn ensure_exchange_search_folders(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
    ) -> Result<()> {
        for definition in exchange_builtin_search_folder_definitions() {
            let changed = sqlx::query(
                r#"
                INSERT INTO search_folders (
                    id, tenant_id, account_id, role, display_name, definition_kind,
                    result_object_kind, scope_json, restriction_json, excluded_folder_roles,
                    is_builtin
                )
                VALUES ($1, $2, $3, $4, $5, 'exchange_builtin', $6, $7, $8, $9, TRUE)
                ON CONFLICT (tenant_id, account_id, role) WHERE is_builtin
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    definition_kind = EXCLUDED.definition_kind,
                    result_object_kind = EXCLUDED.result_object_kind,
                    scope_json = EXCLUDED.scope_json,
                    restriction_json = EXCLUDED.restriction_json,
                    excluded_folder_roles = EXCLUDED.excluded_folder_roles,
                    updated_at = NOW()
                WHERE search_folders.display_name IS DISTINCT FROM EXCLUDED.display_name
                   OR search_folders.definition_kind IS DISTINCT FROM EXCLUDED.definition_kind
                   OR search_folders.result_object_kind IS DISTINCT FROM EXCLUDED.result_object_kind
                   OR search_folders.scope_json IS DISTINCT FROM EXCLUDED.scope_json
                   OR search_folders.restriction_json IS DISTINCT FROM EXCLUDED.restriction_json
                   OR search_folders.excluded_folder_roles IS DISTINCT FROM EXCLUDED.excluded_folder_roles
                RETURNING id, (xmax = 0) AS inserted
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(account_id)
            .bind(definition.role)
            .bind(definition.display_name)
            .bind(definition.result_object_kind)
            .bind(definition.scope_json)
            .bind(definition.restriction_json)
            .bind(definition.excluded_folder_roles)
            .fetch_optional(&mut **tx)
            .await?;

            if let Some(row) = changed {
                let search_folder_id: Uuid = row.try_get("id")?;
                let change_kind = if row.try_get::<bool, _>("inserted")? {
                    "created"
                } else {
                    "updated"
                };
                let modseq = self
                    .allocate_account_modseq_in_tx(
                        tx,
                        tenant_id,
                        account_id,
                        CanonicalChangeCategory::Search.as_str(),
                    )
                    .await?;
                Self::insert_mail_change_log_in_tx(
                    tx,
                    tenant_id,
                    Some(account_id),
                    None,
                    "search_folder_definition",
                    search_folder_id,
                    change_kind,
                    modseq,
                    &[account_id],
                    serde_json::json!({
                        "role": definition.role,
                        "definitionKind": "exchange_builtin",
                        "resultObjectKind": definition.result_object_kind
                    }),
                )
                .await?;
                Self::emit_account_scoped_change(
                    tx,
                    tenant_id,
                    CanonicalChangeCategory::Search,
                    account_id,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn fetch_imap_highest_modseq(&self, account_id: Uuid) -> Result<u64> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let modseq = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT current_modseq
            FROM account_sync_state
            WHERE tenant_id = $1 AND account_id = $2 AND category = 'mail'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(1);

        u64::try_from(modseq).map_err(|_| anyhow!("mail sync modseq is out of range"))
    }

    pub async fn fetch_imap_mailbox_state(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<ImapMailboxState> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT uid_validity, uid_next, modseq
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;
        Ok(ImapMailboxState {
            uid_validity: u32::try_from(row.try_get::<i64, _>("uid_validity")?)
                .map_err(|_| anyhow!("mailbox UIDVALIDITY is out of range"))?,
            uid_next: u32::try_from(row.try_get::<i64, _>("uid_next")?)
                .map_err(|_| anyhow!("mailbox UIDNEXT is out of range"))?,
            highest_modseq: u64::try_from(row.try_get::<i64, _>("modseq")?)
                .map_err(|_| anyhow!("mailbox modseq is out of range"))?,
        })
    }

    pub async fn fetch_jmap_mailbox_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        Ok(self
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .map(|mailbox| mailbox.id)
            .collect())
    }

    pub(crate) async fn ensure_mailbox_name_available_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        parent_id: Option<Uuid>,
        display_name: &str,
        except_mailbox_id: Option<Uuid>,
    ) -> Result<()> {
        let requested_key = MailboxNamePolicy::canonical_key(display_name);
        let rows = sqlx::query(
            r#"
            SELECT id, display_name
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
              AND parent_mailbox_id IS NOT DISTINCT FROM $3
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(parent_id)
        .fetch_all(&mut **tx)
        .await?;

        for row in rows {
            let mailbox_id = row.try_get::<Uuid, _>("id")?;
            if except_mailbox_id.is_some_and(|except| except == mailbox_id) {
                continue;
            }
            let existing_name = row.try_get::<String, _>("display_name")?;
            let existing_key = MailboxNamePolicy::canonical_key(&existing_name);
            if requested_key.collides_with(&existing_key) {
                bail!("mailbox already exists");
            }
        }

        Ok(())
    }

    async fn find_mailbox_by_name_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        parent_id: Option<Uuid>,
        display_name: &str,
    ) -> Result<Option<Uuid>> {
        let requested_key = MailboxNamePolicy::canonical_key(display_name);
        let rows = sqlx::query(
            r#"
            SELECT id, display_name
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
              AND parent_mailbox_id IS NOT DISTINCT FROM $3
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(parent_id)
        .fetch_all(&mut **tx)
        .await?;

        for row in rows {
            let existing_name = row.try_get::<String, _>("display_name")?;
            let existing_key = MailboxNamePolicy::canonical_key(&existing_name);
            if requested_key.as_str() == existing_key.as_str() {
                return Ok(Some(row.try_get::<Uuid, _>("id")?));
            }
            if requested_key.collides_with(&existing_key) {
                bail!("mailbox already exists");
            }
        }

        Ok(None)
    }

    async fn insert_imap_custom_mailbox_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        parent_id: Option<Uuid>,
        display_name: &str,
    ) -> Result<(Uuid, i64)> {
        Self::ensure_mailbox_name_available_in_tx(
            tx,
            tenant_id,
            account_id,
            parent_id,
            display_name,
            None,
        )
        .await?;
        let next_sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_one(&mut **tx)
        .await?;

        let mailbox_id = Uuid::new_v4();
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, parent_mailbox_id, role, display_name, sort_order, uid_validity
            )
            VALUES ($1, $2, $3, $4, 'custom', $5, $6, $7)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(parent_id)
        .bind(display_name)
        .bind(next_sort_order)
        .bind(allocate_uid_validity())
        .execute(&mut **tx)
        .await?;
        Self::set_mailbox_subscription_in_tx(
            tx, tenant_id, account_id, mailbox_id, account_id, true,
        )
        .await?;

        Ok((mailbox_id, modseq))
    }

    async fn ensure_mailbox_parent_valid_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        parent_id: Option<Uuid>,
    ) -> Result<()> {
        let Some(parent_id) = parent_id else {
            return Ok(());
        };
        if mailbox_id.is_some_and(|mailbox_id| mailbox_id == parent_id) {
            bail!("mailbox parentId creates a cycle");
        }

        let mut current = Some(parent_id);
        while let Some(candidate_id) = current {
            let row = sqlx::query(
                r#"
                SELECT parent_mailbox_id
                FROM mailboxes
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(candidate_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(|| {
                anyhow!("mailbox parentId must reference a mailbox in the same account")
            })?;

            current = row.try_get("parent_mailbox_id")?;
            if mailbox_id.is_some_and(|mailbox_id| Some(mailbox_id) == current) {
                bail!("mailbox parentId creates a cycle");
            }
        }

        Ok(())
    }

    async fn set_mailbox_subscription_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        mailbox_account_id: Uuid,
        mailbox_id: Uuid,
        subscriber_account_id: Uuid,
        is_subscribed: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO mailbox_subscriptions (
                tenant_id, mailbox_account_id, mailbox_id, subscriber_account_id, is_subscribed
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, mailbox_account_id, mailbox_id, subscriber_account_id)
            DO UPDATE
            SET is_subscribed = EXCLUDED.is_subscribed,
                updated_at = NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(mailbox_account_id)
        .bind(mailbox_id)
        .bind(subscriber_account_id)
        .bind(is_subscribed)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn create_jmap_mailbox(
        &self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;

        if let Some(role) = system_mailbox_role_for_display_name(&input.name) {
            if input.parent_id.is_some() {
                bail!("system mailbox cannot have parentId");
            }
            let display_name = canonical_system_mailbox_display_name(role)
                .ok_or_else(|| anyhow!("system mailbox display name not found"))?;
            let sort_order = match role {
                "inbox" => 0,
                "drafts" => 10,
                "sent" => 20,
                "trash" => 30,
                _ => input.sort_order.unwrap_or(30),
            };
            let mailbox_id = self
                .ensure_mailbox(
                    &mut tx,
                    &tenant_id,
                    input.account_id,
                    role,
                    display_name,
                    sort_order,
                    365,
                )
                .await?;
            Self::set_mailbox_subscription_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                mailbox_id,
                input.account_id,
                input.is_subscribed,
            )
            .await?;
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            tx.commit().await?;

            return self
                .fetch_jmap_mailboxes(input.account_id)
                .await?
                .into_iter()
                .find(|mailbox| mailbox.id == mailbox_id)
                .ok_or_else(|| anyhow!("mailbox creation failed"));
        }

        let name = MailboxDisplayName::new(&input.name)?.into_string();
        Self::ensure_mailbox_parent_valid_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            None,
            input.parent_id,
        )
        .await?;
        Self::ensure_mailbox_name_available_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            input.parent_id,
            &name,
            None,
        )
        .await?;

        let next_sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_one(&mut *tx)
        .await?;

        let mailbox_id = Uuid::new_v4();
        let sort_order = input.sort_order.unwrap_or(next_sort_order);
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, parent_mailbox_id, role, display_name, sort_order, uid_validity
            )
            VALUES ($1, $2, $3, $4, 'custom', $5, $6, $7)
            "#,
        )
        .bind(mailbox_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.parent_id)
        .bind(&name)
        .bind(sort_order)
        .bind(allocate_uid_validity())
        .execute(&mut *tx)
        .await?;
        Self::set_mailbox_subscription_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            mailbox_id,
            input.account_id,
            input.is_subscribed,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, input.account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            Some(mailbox_id),
            "mailbox",
            mailbox_id,
            "created",
            modseq,
            &principals,
            serde_json::json!({"name": name, "parentId": input.parent_id}),
        )
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow!("mailbox creation failed"))
    }

    pub async fn create_imap_mailbox(
        &self,
        account_id: Uuid,
        name: &str,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        if let Some(role) = system_mailbox_role_for_display_name(name) {
            let display_name = canonical_system_mailbox_display_name(role)
                .ok_or_else(|| anyhow!("system mailbox display name not found"))?;
            let sort_order = match role {
                "inbox" => 0,
                "drafts" => 10,
                "sent" => 20,
                "trash" => 30,
                _ => 30,
            };
            let mailbox_id = self
                .ensure_mailbox(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    role,
                    display_name,
                    sort_order,
                    365,
                )
                .await?;
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            tx.commit().await?;

            return self
                .fetch_jmap_mailboxes(account_id)
                .await?
                .into_iter()
                .find(|mailbox| mailbox.id == mailbox_id)
                .ok_or_else(|| anyhow!("mailbox creation failed"));
        }

        let path = MailboxPath::parse(name)?;
        let mut parent_id = None;
        let mut created = Vec::new();
        for segment in path.segments() {
            let display_name = segment.as_str();
            if let Some(existing_id) = Self::find_mailbox_by_name_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                parent_id,
                display_name,
            )
            .await?
            {
                parent_id = Some(existing_id);
                continue;
            }
            let (mailbox_id, modseq) = self
                .insert_imap_custom_mailbox_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    parent_id,
                    display_name,
                )
                .await?;
            created.push((mailbox_id, display_name.to_string(), parent_id, modseq));
            parent_id = Some(mailbox_id);
        }
        if created.is_empty() {
            bail!("mailbox already exists");
        }
        let (mailbox_id, _, _, _) = created
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("mailbox creation failed"))?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for (created_id, created_name, created_parent_id, modseq) in created {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(created_id),
                "mailbox",
                created_id,
                "created",
                modseq,
                &principals,
                serde_json::json!({"name": created_name, "parentId": created_parent_id}),
            )
            .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow!("mailbox creation failed"))
    }

    pub async fn update_jmap_mailbox(
        &self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role, display_name, parent_mailbox_id, sort_order
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if is_system_mailbox_role(&role) {
            bail!("system mailbox cannot be modified through JMAP");
        }

        let current_name = current.try_get::<String, _>("display_name")?;
        let current_parent_id = current.try_get::<Option<Uuid>, _>("parent_mailbox_id")?;
        let current_sort_order = current.try_get::<i32, _>("sort_order")?;
        let name = match input.name.as_deref() {
            Some(name) => MailboxDisplayName::new(name)?.into_string(),
            None => current_name,
        };
        let parent_id = input.parent_id.unwrap_or(current_parent_id);
        Self::ensure_mailbox_parent_valid_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            Some(input.mailbox_id),
            parent_id,
        )
        .await?;
        Self::ensure_mailbox_name_available_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            parent_id,
            &name,
            Some(input.mailbox_id),
        )
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        sqlx::query(
            r#"
            UPDATE mailboxes
            SET parent_mailbox_id = $4,
                display_name = $5,
                sort_order = $6,
                modseq = GREATEST(modseq + 1, $7),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(parent_id)
        .bind(&name)
        .bind(input.sort_order.unwrap_or(current_sort_order))
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        if let Some(is_subscribed) = input.is_subscribed {
            Self::set_mailbox_subscription_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                input.mailbox_id,
                input.account_id,
                is_subscribed,
            )
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, input.account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            Some(input.mailbox_id),
            "mailbox",
            input.mailbox_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({"name": name, "parentId": parent_id}),
        )
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == input.mailbox_id)
            .ok_or_else(|| anyhow!("mailbox update failed"))
    }

    pub async fn rename_imap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        name: &str,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role, sort_order
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if is_system_mailbox_role(&role) {
            bail!("system mailbox cannot be modified through IMAP");
        }

        let path = MailboxPath::parse(name)?;
        let mut parent_id = None;
        let mut created = Vec::new();
        let final_index = path.segments().len().saturating_sub(1);
        for segment in path.segments().iter().take(final_index) {
            let display_name = segment.as_str();
            if let Some(existing_id) = Self::find_mailbox_by_name_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                parent_id,
                display_name,
            )
            .await?
            {
                parent_id = Some(existing_id);
                continue;
            }
            let (created_id, modseq) = self
                .insert_imap_custom_mailbox_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    parent_id,
                    display_name,
                )
                .await?;
            created.push((created_id, display_name.to_string(), parent_id, modseq));
            parent_id = Some(created_id);
        }
        let name = path
            .segments()
            .last()
            .ok_or_else(|| anyhow!("mailbox name is required"))?
            .as_str()
            .to_string();
        Self::ensure_mailbox_parent_valid_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            Some(mailbox_id),
            parent_id,
        )
        .await?;
        Self::ensure_mailbox_name_available_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            parent_id,
            &name,
            Some(mailbox_id),
        )
        .await?;

        let sort_order = current.try_get::<i32, _>("sort_order")?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        sqlx::query(
            r#"
            UPDATE mailboxes
            SET parent_mailbox_id = $4,
                display_name = $5,
                sort_order = $6,
                modseq = GREATEST(modseq + 1, $7),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(parent_id)
        .bind(&name)
        .bind(sort_order)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for (created_id, created_name, created_parent_id, created_modseq) in created {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(created_id),
                "mailbox",
                created_id,
                "created",
                created_modseq,
                &principals,
                serde_json::json!({"name": created_name, "parentId": created_parent_id}),
            )
            .await?;
        }
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(mailbox_id),
            "mailbox",
            mailbox_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({"name": name, "parentId": parent_id}),
        )
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow!("mailbox update failed"))
    }

    pub async fn set_mailbox_subscription(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        is_subscribed: bool,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let exists = sqlx::query(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if exists.is_none() {
            bail!("mailbox not found");
        }

        Self::set_mailbox_subscription_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            mailbox_id,
            account_id,
            is_subscribed,
        )
        .await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(mailbox_id),
            "mailbox",
            mailbox_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({"isSubscribed": is_subscribed}),
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn destroy_jmap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if is_system_mailbox_role(&role) {
            bail!("system mailbox cannot be deleted through JMAP");
        }

        let message_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND visibility <> 'expunged'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_one(&mut *tx)
        .await?;
        if message_count > 0 {
            bail!("mailbox is not empty");
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(mailbox_id),
            "mailbox",
            mailbox_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({"reason": "delete"}),
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'mailbox', $4, $5, $6, 'delete')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(modseq)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_messages AS (
                SELECT
                    s.message_id AS id,
                    MAX(mm.received_at) AS latest_received_at
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY s.message_id
            )
            SELECT id
            FROM matched_messages
            ORDER BY latest_received_at DESC, id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT s.message_id
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY s.message_id
            ) matched_messages
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapEmailQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT m.id
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.visibility = 'visible'
            ORDER BY m.id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(Into::into))
            .collect()
    }

    pub async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT thread_id
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.visibility = 'visible'
            ORDER BY thread_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("thread_id").map_err(Into::into))
            .collect()
    }

    pub async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_threads AS (
                SELECT
                    m.thread_id,
                    MAX(mm.received_at) AS latest_received_at
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                JOIN messages m ON m.tenant_id = s.tenant_id AND m.id = s.message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            )
            SELECT thread_id
            FROM matched_threads
            ORDER BY latest_received_at DESC, thread_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("thread_id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT m.thread_id
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                JOIN messages m ON m.tenant_id = s.tenant_id AND m.id = s.message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            ) matched_threads
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapThreadQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_jmap_emails(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, JmapEmailRow>(
            r#"
            WITH visible_memberships AS (
                SELECT
                    mm.id,
                    mm.account_id,
                    mm.message_id,
                    mm.thread_id,
                    mm.is_seen,
                    mm.is_flagged,
                    mm.followup_flag_status,
                    mm.followup_icon,
                    mm.todo_item_flags,
                    mm.followup_request,
                    CASE WHEN mm.followup_start_at IS NULL THEN NULL ELSE to_char(mm.followup_start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS followup_start_at,
                    CASE WHEN mm.followup_due_at IS NULL THEN NULL ELSE to_char(mm.followup_due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS followup_due_at,
                    CASE WHEN mm.followup_completed_at IS NULL THEN NULL ELSE to_char(mm.followup_completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS followup_completed_at,
                    mm.reminder_set,
                    CASE WHEN mm.reminder_at IS NULL THEN NULL ELSE to_char(mm.reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS reminder_at,
                    CASE WHEN mm.reminder_dismissed_at IS NULL THEN NULL ELSE to_char(mm.reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS reminder_dismissed_at,
                    mm.swapped_todo_store_id,
                    mm.swapped_todo_data,
                    mm.keywords,
                    mm.is_draft,
                    mm.modseq,
                    mm.updated_at,
                    mb.id AS mailbox_id,
                    mb.role AS mailbox_role,
                    mb.display_name AS mailbox_name,
                    mb.sort_order AS mailbox_sort_order
                FROM mailbox_messages mm
                JOIN mailboxes mb
                  ON mb.tenant_id = mm.tenant_id
                 AND mb.account_id = mm.account_id
                 AND mb.id = mm.mailbox_id
                WHERE mm.tenant_id = $1
                  AND mm.account_id = $2
                  AND mm.message_id = ANY($3)
                  AND mm.visibility = 'visible'
            ),
            membership_rollup AS (
                SELECT
                    message_id,
                    COALESCE((array_agg(thread_id ORDER BY updated_at DESC))[1], message_id) AS thread_id,
                    array_agg(mailbox_id ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_ids,
                    array_agg(mailbox_role ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_roles,
                    array_agg(mailbox_name ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_names,
                    array_agg(modseq ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_modseqs,
                    array_agg(NOT is_seen ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_unreads,
                    array_agg(is_flagged ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_flaggeds,
                    array_agg(followup_flag_status ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_flag_statuses,
                    array_agg(followup_icon ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_icons,
                    array_agg(todo_item_flags ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_todo_item_flags,
                    array_agg(followup_request ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_requests,
                    array_agg(followup_start_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_start_ats,
                    array_agg(followup_due_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_due_ats,
                    array_agg(followup_completed_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_followup_completed_ats,
                    array_agg(reminder_set ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_reminder_sets,
                    array_agg(reminder_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_reminder_ats,
                    array_agg(reminder_dismissed_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_reminder_dismissed_ats,
                    array_agg(swapped_todo_store_id ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_swapped_todo_store_ids,
                    array_agg(swapped_todo_data ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_swapped_todo_datas,
                    array_agg(to_json(keywords)::text ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_categories_json,
                    array_agg(is_draft ORDER BY mailbox_sort_order, mailbox_name, mailbox_id) AS mailbox_drafts,
                    array_agg(id) AS mailbox_message_ids,
                    BOOL_OR(NOT is_seen) AS unread,
                    BOOL_OR(is_flagged) AS flagged,
                    COALESCE((array_agg(followup_flag_status ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1], 'none') AS followup_flag_status,
                    COALESCE((array_agg(followup_icon ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1], 0) AS followup_icon,
                    COALESCE((array_agg(todo_item_flags ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1], 0) AS todo_item_flags,
                    COALESCE((array_agg(followup_request ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1], '') AS followup_request,
                    (array_agg(followup_start_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS followup_start_at,
                    (array_agg(followup_due_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS followup_due_at,
                    (array_agg(followup_completed_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS followup_completed_at,
                    COALESCE((array_agg(reminder_set ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1], FALSE) AS reminder_set,
                    (array_agg(reminder_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS reminder_at,
                    (array_agg(reminder_dismissed_at ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS reminder_dismissed_at,
                    (array_agg(swapped_todo_store_id ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS swapped_todo_store_id,
                    (array_agg(swapped_todo_data ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1] AS swapped_todo_data,
                    COALESCE(
                        ARRAY(
                            SELECT jsonb_array_elements_text(
                                (array_agg(to_jsonb(keywords) ORDER BY mailbox_sort_order, mailbox_name, mailbox_id))[1]
                            )
                        ),
                        ARRAY[]::TEXT[]
                    ) AS categories,
                    BOOL_OR(is_draft) AS draft
                FROM visible_memberships
                GROUP BY message_id
            )
            SELECT
                m.id,
                rollup.thread_id,
                rollup.mailbox_ids,
                rollup.mailbox_roles,
                rollup.mailbox_names,
                rollup.mailbox_modseqs,
                rollup.mailbox_unreads,
                rollup.mailbox_flaggeds,
                rollup.mailbox_followup_flag_statuses,
                rollup.mailbox_followup_icons,
                rollup.mailbox_todo_item_flags,
                rollup.mailbox_followup_requests,
                rollup.mailbox_followup_start_ats,
                rollup.mailbox_followup_due_ats,
                rollup.mailbox_followup_completed_ats,
                rollup.mailbox_reminder_sets,
                rollup.mailbox_reminder_ats,
                rollup.mailbox_reminder_dismissed_ats,
                rollup.mailbox_swapped_todo_store_ids,
                rollup.mailbox_swapped_todo_datas,
                rollup.mailbox_categories_json,
                rollup.mailbox_drafts,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                COALESCE(fr.address, '') AS from_address,
                NULLIF(fr.display_name, '') AS from_display,
                NULLIF(sr.address, '') AS sender_address,
                NULLIF(sr.display_name, '') AS sender_display,
                'self' AS sender_authorization_kind,
                account.id AS submitted_by_account_id,
                m.normalized_subject AS subject,
                LEFT(COALESCE(tb.body_text, hb.body_text, ''), 160) AS preview,
                COALESCE(tb.body_text, '') AS body_text,
                hb.sanitized_html AS body_html_sanitized,
                rollup.unread,
                rollup.flagged,
                rollup.followup_flag_status,
                rollup.followup_icon,
                rollup.todo_item_flags,
                rollup.followup_request,
                rollup.followup_start_at,
                rollup.followup_due_at,
                rollup.followup_completed_at,
                rollup.reminder_set,
                rollup.reminder_at,
                rollup.reminder_dismissed_at,
                rollup.swapped_todo_store_id,
                rollup.swapped_todo_data,
                rollup.categories,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                ('message:' || m.id::text) AS mime_blob_ref,
                COALESCE(sq.status, CASE WHEN rollup.draft THEN 'draft' ELSE 'stored' END) AS delivery_status
            FROM messages m
            JOIN membership_rollup rollup
              ON rollup.message_id = m.id
            JOIN accounts account
              ON account.tenant_id = m.tenant_id
             AND account.id = $2
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id AND fr.message_id = m.id AND fr.role = 'from'
            LEFT JOIN message_recipients sr
              ON sr.tenant_id = m.tenant_id AND sr.message_id = m.id AND sr.role = 'sender'
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'text'
                ORDER BY id ASC
                LIMIT 1
            ) tb ON TRUE
            LEFT JOIN LATERAL (
                SELECT body_text, sanitized_html
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'html'
                ORDER BY id ASC
                LIMIT 1
            ) hb ON TRUE
            LEFT JOIN LATERAL (
                SELECT status
                FROM submission_queue q
                WHERE q.tenant_id = m.tenant_id
                  AND q.account_id = $2
                  AND q.sent_mailbox_message_id = ANY(rollup.mailbox_message_ids)
                ORDER BY q.created_at DESC
                LIMIT 1
            ) sq ON TRUE
            WHERE m.tenant_id = $1
              AND m.id = ANY($3)
            ORDER BY m.received_at DESC, m.id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.role AS kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = r.tenant_id
                    AND mm.account_id = $2
                    AND mm.message_id = r.message_id
                    AND mm.visibility = 'visible'
              )
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.role ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut emails = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(row) = rows.iter().find(|row| row.id == *id) {
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let mailbox_states = row
                    .mailbox_ids
                    .iter()
                    .enumerate()
                    .map(|(index, mailbox_id)| JmapEmailMailboxState {
                        mailbox_id: *mailbox_id,
                        role: row.mailbox_roles.get(index).cloned().unwrap_or_default(),
                        name: row.mailbox_names.get(index).cloned().unwrap_or_default(),
                        modseq: row
                            .mailbox_modseqs
                            .get(index)
                            .copied()
                            .and_then(|modseq| u64::try_from(modseq).ok())
                            .unwrap_or(1),
                        unread: row.mailbox_unreads.get(index).copied().unwrap_or(false),
                        flagged: row.mailbox_flaggeds.get(index).copied().unwrap_or(false),
                        followup_flag_status: row
                            .mailbox_followup_flag_statuses
                            .get(index)
                            .cloned()
                            .unwrap_or_else(|| "none".to_string()),
                        followup_icon: row.mailbox_followup_icons.get(index).copied().unwrap_or(0),
                        todo_item_flags: row
                            .mailbox_todo_item_flags
                            .get(index)
                            .copied()
                            .unwrap_or(0),
                        followup_request: row
                            .mailbox_followup_requests
                            .get(index)
                            .cloned()
                            .unwrap_or_default(),
                        followup_start_at: row
                            .mailbox_followup_start_ats
                            .get(index)
                            .cloned()
                            .unwrap_or(None),
                        followup_due_at: row
                            .mailbox_followup_due_ats
                            .get(index)
                            .cloned()
                            .unwrap_or(None),
                        followup_completed_at: row
                            .mailbox_followup_completed_ats
                            .get(index)
                            .cloned()
                            .unwrap_or(None),
                        reminder_set: row
                            .mailbox_reminder_sets
                            .get(index)
                            .copied()
                            .unwrap_or(false),
                        reminder_at: row.mailbox_reminder_ats.get(index).cloned().unwrap_or(None),
                        reminder_dismissed_at: row
                            .mailbox_reminder_dismissed_ats
                            .get(index)
                            .cloned()
                            .unwrap_or(None),
                        swapped_todo_store_id: row
                            .mailbox_swapped_todo_store_ids
                            .get(index)
                            .copied()
                            .unwrap_or(None),
                        swapped_todo_data: row
                            .mailbox_swapped_todo_datas
                            .get(index)
                            .cloned()
                            .unwrap_or(None),
                        categories: row
                            .mailbox_categories_json
                            .get(index)
                            .and_then(|value| serde_json::from_str(value).ok())
                            .unwrap_or_default(),
                        draft: row.mailbox_drafts.get(index).copied().unwrap_or(false),
                    })
                    .collect::<Vec<_>>();
                let primary_mailbox = mailbox_states
                    .first()
                    .ok_or_else(|| anyhow!("JMAP email row has no visible mailbox"))?;
                let primary_mailbox_id = primary_mailbox.mailbox_id;
                let primary_mailbox_role = primary_mailbox.role.clone();
                let primary_mailbox_name = primary_mailbox.name.clone();
                let primary_modseq = primary_mailbox.modseq;

                emails.push(JmapEmail {
                    id: row.id,
                    thread_id: row.thread_id,
                    mailbox_ids: row.mailbox_ids.clone(),
                    mailbox_states,
                    mailbox_id: primary_mailbox_id,
                    mailbox_role: primary_mailbox_role,
                    mailbox_name: primary_mailbox_name,
                    modseq: primary_modseq,
                    received_at: row.received_at.clone(),
                    sent_at: row.sent_at.clone(),
                    from_address: row.from_address.clone(),
                    from_display: row.from_display.clone(),
                    sender_address: row.sender_address.clone(),
                    sender_display: row.sender_display.clone(),
                    sender_authorization_kind: row.sender_authorization_kind.clone(),
                    submitted_by_account_id: row.submitted_by_account_id,
                    to,
                    cc,
                    bcc: Vec::new(),
                    subject: row.subject.clone(),
                    preview: row.preview.clone(),
                    body_text: row.body_text.clone(),
                    body_html_sanitized: row.body_html_sanitized.clone(),
                    unread: row.unread,
                    flagged: row.flagged,
                    followup_flag_status: row.followup_flag_status.clone(),
                    followup_icon: row.followup_icon,
                    todo_item_flags: row.todo_item_flags,
                    followup_request: row.followup_request.clone(),
                    followup_start_at: row.followup_start_at.clone(),
                    followup_due_at: row.followup_due_at.clone(),
                    followup_completed_at: row.followup_completed_at.clone(),
                    reminder_set: row.reminder_set,
                    reminder_at: row.reminder_at.clone(),
                    reminder_dismissed_at: row.reminder_dismissed_at.clone(),
                    swapped_todo_store_id: row.swapped_todo_store_id,
                    swapped_todo_data: row.swapped_todo_data.clone(),
                    categories: row.categories.clone(),
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id.clone(),
                    mime_blob_ref: row.mime_blob_ref.clone(),
                    delivery_status: row.delivery_status.clone(),
                });
            }
        }

        Ok(emails)
    }

    pub async fn fetch_jmap_emails_with_protected_bcc(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>> {
        let mut emails = self.fetch_jmap_emails(account_id, ids).await?;
        if emails.is_empty() {
            return Ok(emails);
        }

        let visible_ids = emails.iter().map(|email| email.id).collect::<Vec<_>>();
        let protected_bcc = self
            .fetch_visible_protected_bcc_recipients(account_id, &visible_ids)
            .await?;
        for email in &mut emails {
            email.bcc = protected_bcc.get(&email.id).cloned().unwrap_or_default();
        }
        Ok(emails)
    }

    async fn fetch_visible_protected_bcc_recipients(
        &self,
        account_id: Uuid,
        message_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<JmapEmailAddress>>> {
        if message_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, MessageBccRecipientRecordRow>(
            r#"
            SELECT r.message_id, r.address, r.display_name
            FROM protected_bcc_recipients r
            WHERE r.tenant_id = $1
              AND r.message_id = ANY($3)
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = r.tenant_id
                    AND mm.account_id = $2
                    AND mm.message_id = r.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY r.message_id ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut recipients: HashMap<Uuid, Vec<JmapEmailAddress>> = HashMap::new();
        for row in rows {
            recipients
                .entry(row.message_id)
                .or_default()
                .push(JmapEmailAddress {
                    address: row.address,
                    display_name: row.display_name,
                });
        }
        Ok(recipients)
    }

    pub async fn fetch_imap_emails(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<Vec<ImapEmail>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ImapEmailRow>(
            r#"
            SELECT
                m.id,
                mm.imap_uid,
                mm.modseq AS imap_modseq,
                COALESCE(mm.thread_id, m.id) AS thread_id,
                mm.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(mm.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                COALESCE(fr.address, '') AS from_address,
                NULLIF(fr.display_name, '') AS from_display,
                m.normalized_subject AS subject,
                LEFT(COALESCE(tb.body_text, hb.body_text, ''), 160) AS preview,
                COALESCE(tb.body_text, '') AS body_text,
                hb.sanitized_html AS body_html_sanitized,
                NOT mm.is_seen AS unread,
                mm.is_flagged AS flagged,
                mm.is_deleted AS imap_deleted,
                mm.keywords,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END) AS delivery_status
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id AND fr.message_id = m.id AND fr.role = 'from'
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'text'
                ORDER BY id ASC
                LIMIT 1
            ) tb ON TRUE
            LEFT JOIN LATERAL (
                SELECT body_text, sanitized_html
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'html'
                ORDER BY id ASC
                LIMIT 1
            ) hb ON TRUE
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.mailbox_id = $3
              AND mm.visibility = 'visible'
            ORDER BY mm.imap_uid ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.role AS kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = r.tenant_id
                    AND mm.account_id = $2
                    AND mm.message_id = r.message_id
                    AND mm.visibility = 'visible'
              )
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.role ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        let part_rows = sqlx::query(
            r#"
            SELECT
                mp.message_id,
                mp.part_path,
                mp.content_type,
                mp.content_disposition,
                mp.content_id,
                mp.file_name,
                mp.transfer_encoding,
                mp.charset_name,
                mp.size_octets
            FROM mime_parts mp
            WHERE mp.tenant_id = $1
              AND mp.message_id = ANY($2)
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = mp.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = mp.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY mp.message_id ASC, mp.ordinal ASC, mp.part_path ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(&message_ids)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        let mut mime_parts_by_message: HashMap<Uuid, Vec<ImapMimePart>> = HashMap::new();
        for part in part_rows {
            let message_id: Uuid = part.try_get("message_id")?;
            mime_parts_by_message
                .entry(message_id)
                .or_default()
                .push(ImapMimePart {
                    part_path: part.try_get("part_path")?,
                    content_type: part.try_get("content_type")?,
                    content_disposition: part.try_get("content_disposition")?,
                    content_id: part.try_get("content_id")?,
                    file_name: part.try_get("file_name")?,
                    transfer_encoding: part.try_get("transfer_encoding")?,
                    charset_name: part.try_get("charset_name")?,
                    size_octets: part.try_get("size_octets")?,
                });
        }

        rows.into_iter()
            .map(|row| {
                let uid = u32::try_from(row.imap_uid)
                    .map_err(|_| anyhow!("message IMAP UID is out of range"))?;
                let modseq = u64::try_from(row.imap_modseq)
                    .map_err(|_| anyhow!("message IMAP modseq is out of range"))?;
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                Ok(ImapEmail {
                    id: row.id,
                    uid,
                    modseq,
                    thread_id: row.thread_id,
                    mailbox_id: row.mailbox_id,
                    mailbox_role: row.mailbox_role,
                    mailbox_name: row.mailbox_name,
                    received_at: row.received_at,
                    sent_at: row.sent_at,
                    from_address: row.from_address,
                    from_display: row.from_display,
                    to,
                    cc,
                    bcc: Vec::new(),
                    subject: row.subject,
                    preview: row.preview,
                    body_text: row.body_text,
                    body_html_sanitized: row.body_html_sanitized,
                    unread: row.unread,
                    flagged: row.flagged,
                    deleted: row.imap_deleted,
                    keywords: row.keywords,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id,
                    delivery_status: row.delivery_status,
                    mime_parts: mime_parts_by_message.remove(&row.id).unwrap_or_default(),
                })
            })
            .collect()
    }

    pub async fn update_imap_flags(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &[Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
        deleted: Option<bool>,
        unchanged_since: Option<u64>,
    ) -> Result<Vec<Uuid>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let unchanged_since_i64 = unchanged_since
            .map(i64::try_from)
            .transpose()
            .map_err(|_| anyhow!("UNCHANGEDSINCE is out of range"))?;
        let modified_ids = if let Some(limit) = unchanged_since_i64 {
            sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT message_id
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = ANY($4)
                  AND visibility = 'visible'
                  AND modseq > $5
                ORDER BY imap_uid ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(message_ids)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?
        } else {
            Vec::new()
        };
        if modified_ids.len() == message_ids.len() {
            tx.rollback().await?;
            return Ok(modified_ids);
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;

        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET
                is_seen = COALESCE(NOT $4, is_seen),
                is_flagged = COALESCE($5, is_flagged),
                followup_flag_status = CASE
                    WHEN $5::bool IS NULL THEN followup_flag_status
                    WHEN $5 THEN 'flagged'
                    ELSE 'none'
                END,
                followup_icon = CASE
                    WHEN $5::bool IS NULL THEN followup_icon
                    WHEN $5 AND followup_icon = 0 THEN 6
                    WHEN NOT $5 THEN 0
                    ELSE followup_icon
                END,
                todo_item_flags = CASE
                    WHEN $5::bool IS NULL THEN todo_item_flags
                    WHEN $5 AND todo_item_flags = 0 THEN 8
                    WHEN NOT $5 THEN 0
                    ELSE todo_item_flags
                END,
                followup_completed_at = CASE
                    WHEN $5 = FALSE THEN NULL
                    ELSE followup_completed_at
                END,
                is_deleted = COALESCE($6, is_deleted),
                deleted_at = CASE
                    WHEN COALESCE($6, is_deleted) THEN COALESCE(deleted_at, NOW())
                    ELSE NULL
                END,
                modseq = $7,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND message_id = ANY($8)
              AND visibility = 'visible'
              AND ($9::bigint IS NULL OR modseq <= $9)
            RETURNING id, message_id, thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(unread)
        .bind(flagged)
        .bind(deleted)
        .bind(modseq)
        .bind(message_ids)
        .bind(unchanged_since_i64)
        .fetch_all(&mut *tx)
        .await?;

        if !rows.is_empty() {
            let principals =
                Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
            for row in rows {
                let membership_id: Uuid = row.try_get("id")?;
                let message_id: Uuid = row.try_get("message_id")?;
                Self::insert_mail_change_log_in_tx(
                    &mut tx,
                    &tenant_id,
                    Some(account_id),
                    Some(mailbox_id),
                    "mailbox_message",
                    membership_id,
                    "updated",
                    modseq,
                    &principals,
                    serde_json::json!({
                        "messageId": message_id,
                        "threadId": row.try_get::<Uuid, _>("thread_id")?,
                        "imapUid": row.try_get::<i64, _>("imap_uid")?,
                        "flagsChanged": true
                    }),
                )
                .await?;
            }
        }

        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(modified_ids)
    }

    pub async fn expunge_imap_deleted(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &[Uuid],
        audit: AuditEntryInput,
    ) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;

        let expunge_rows = sqlx::query(
            r#"
            SELECT id, message_id, thread_id, imap_uid, is_seen
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND message_id = ANY($4)
              AND visibility = 'visible'
              AND is_deleted = TRUE
            ORDER BY imap_uid ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(message_ids)
        .fetch_all(&mut *tx)
        .await?;

        if !expunge_rows.is_empty() {
            let modseq = self
                .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
                .await?;
            let principals =
                Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
            for row in &expunge_rows {
                let membership_id: Uuid = row.try_get("id")?;
                let message_id: Uuid = row.try_get("message_id")?;
                let imap_uid: i64 = row.try_get("imap_uid")?;
                let cursor = Self::insert_mail_change_log_in_tx(
                    &mut tx,
                    &tenant_id,
                    Some(account_id),
                    Some(mailbox_id),
                    "mailbox_message",
                    membership_id,
                    "expunged",
                    modseq,
                    &principals,
                    serde_json::json!({
                        "messageId": message_id,
                        "threadId": row.try_get::<Uuid, _>("thread_id")?,
                        "imapUid": imap_uid
                    }),
                )
                .await?;
                sqlx::query(
                    r#"
                    INSERT INTO tombstones (
                        id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                        message_id, mailbox_message_id, imap_uid, deleted_modseq,
                        change_cursor, reason
                    )
                    VALUES (
                        $1, $2, $3, $4, 'mailbox_message', $5,
                        $6, $5, $7, $8, $9, 'expunge'
                    )
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(&tenant_id)
                .bind(account_id)
                .bind(mailbox_id)
                .bind(membership_id)
                .bind(message_id)
                .bind(imap_uid)
                .bind(modseq)
                .bind(cursor)
                .execute(&mut *tx)
                .await?;
            }
            let unread_removed = expunge_rows
                .iter()
                .filter(|row| !row.try_get::<bool, _>("is_seen").unwrap_or(true))
                .count() as i32;
            sqlx::query(
                r#"
                UPDATE mailbox_messages
                SET visibility = 'expunged',
                    expunged_at = NOW(),
                    modseq = $5,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND id = ANY($4)
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(
                &expunge_rows
                    .iter()
                    .filter_map(|row| row.try_get::<Uuid, _>("id").ok())
                    .collect::<Vec<_>>(),
            )
            .bind(modseq)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                UPDATE mailboxes
                SET total_messages = GREATEST(0, total_messages - $4),
                    unread_messages = GREATEST(0, unread_messages - $5),
                    modseq = GREATEST(modseq + 1, $6),
                    updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(expunge_rows.len() as i32)
            .bind(unread_removed)
            .bind(modseq)
            .execute(&mut *tx)
            .await?;
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_custom_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT mm.id
            FROM mailbox_messages mm
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.message_id = $3
              AND mm.visibility = 'visible'
              AND mb.tenant_id = $1
              AND mb.account_id = $2
              AND mb.role = 'custom'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            bail!("custom mailbox message not found");
        }

        drop(rows);
        tx.rollback().await?;
        self.delete_jmap_email(account_id, message_id, audit)
            .await?;
        return Ok(());
    }

    pub async fn delete_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.delete_jmap_email_memberships(account_id, None, message_id, audit)
            .await
    }

    pub async fn delete_jmap_email_from_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.delete_jmap_email_memberships(account_id, Some(mailbox_id), message_id, audit)
            .await
    }

    async fn delete_jmap_email_memberships(
        &self,
        account_id: Uuid,
        mailbox_id_filter: Option<Uuid>,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT id, mailbox_id, thread_id, imap_uid, is_seen
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND ($4::uuid IS NULL OR mailbox_id = $4)
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(mailbox_id_filter)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            bail!("message not found");
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in &rows {
            let membership_id: Uuid = row.try_get("id")?;
            let mailbox_id: Uuid = row.try_get("mailbox_id")?;
            let imap_uid: i64 = row.try_get("imap_uid")?;
            let cursor = Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(mailbox_id),
                "mailbox_message",
                membership_id,
                "destroyed",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": row.try_get::<Uuid, _>("thread_id")?,
                    "imapUid": imap_uid
                }),
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO tombstones (
                    id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                    message_id, mailbox_message_id, imap_uid, deleted_modseq,
                    change_cursor, reason
                )
                VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, 'delete')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(membership_id)
            .bind(message_id)
            .bind(imap_uid)
            .bind(modseq)
            .bind(cursor)
            .execute(&mut *tx)
            .await?;
        }

        let membership_ids = rows
            .iter()
            .map(|row| row.try_get::<Uuid, _>("id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET visibility = 'expunged',
                expunged_at = NOW(),
                modseq = $4,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&membership_ids)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM mail_search_documents
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_message_id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&membership_ids)
        .execute(&mut *tx)
        .await?;

        let mut removed_by_mailbox: HashMap<Uuid, (i32, i32)> = HashMap::new();
        for row in &rows {
            let mailbox_id: Uuid = row.try_get("mailbox_id")?;
            let unread_removed = if row.try_get::<bool, _>("is_seen")? {
                0
            } else {
                1
            };
            let entry = removed_by_mailbox.entry(mailbox_id).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += unread_removed;
        }
        for (mailbox_id, (removed, unread_removed)) in removed_by_mailbox {
            sqlx::query(
                r#"
                UPDATE mailboxes
                SET total_messages = GREATEST(0, total_messages - $4),
                    unread_messages = GREATEST(0, unread_messages - $5),
                    modseq = GREATEST(modseq + 1, $6),
                    updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(removed)
            .bind(unread_removed)
            .bind(modseq)
            .execute(&mut *tx)
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
        let emails = self.fetch_jmap_emails(account_id, &[id]).await?;
        Ok(emails.into_iter().find(|email| {
            email
                .mailbox_states
                .iter()
                .any(|state| state.role == "drafts")
        }))
    }

    pub async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapEmailSubmissionRow>(
            r#"
            SELECT
                q.id,
                mm.message_id AS email_id,
                COALESCE(mm.thread_id, m.id) AS thread_id,
                q.from_address,
                NULLIF(q.sender_address, '') AS sender_address,
                q.authorization_kind AS sender_authorization_kind,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS send_at,
                q.status AS queue_status,
                q.status AS delivery_status
            FROM submission_queue q
            JOIN mailbox_messages mm
              ON mm.tenant_id = q.tenant_id
             AND mm.account_id = q.account_id
             AND mm.id = q.sent_mailbox_message_id
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            WHERE q.tenant_id = $1
              AND q.account_id = $2
              AND ($3::uuid[] IS NULL OR q.id = ANY($3))
            ORDER BY q.created_at DESC, q.id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(if ids.is_empty() {
            None::<Vec<Uuid>>
        } else {
            Some(ids.to_vec())
        })
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.email_id).collect::<Vec<_>>();
        let recipient_rows = if message_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, JmapEmailRecipientRow>(
                r#"
                SELECT
                    mm.message_id,
                    r.role AS kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM submission_recipients r
                JOIN submission_queue q
                  ON q.tenant_id = r.tenant_id
                 AND q.id = r.submission_queue_id
                JOIN mailbox_messages mm
                  ON mm.tenant_id = q.tenant_id
                 AND mm.account_id = q.account_id
                 AND mm.id = q.sent_mailbox_message_id
                WHERE r.tenant_id = $1
                  AND mm.message_id = ANY($2)
                  AND r.role <> 'bcc'
                ORDER BY mm.message_id ASC, r.role ASC, r.ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(&message_ids)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| JmapEmailSubmission {
                id: row.id,
                email_id: row.email_id,
                thread_id: row.thread_id,
                identity_id: submission::sender_identity_id(
                    submission::sender_authorization_kind_from_str(&row.sender_authorization_kind),
                    account_id,
                ),
                identity_email: row.from_address.clone(),
                envelope_mail_from: row
                    .sender_address
                    .clone()
                    .unwrap_or_else(|| row.from_address.clone()),
                envelope_rcpt_to: recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.email_id)
                    .map(|recipient| recipient.address.clone())
                    .collect(),
                send_at: row.send_at,
                undo_status: "final".to_string(),
                delivery_status: if row.delivery_status.trim().is_empty() {
                    row.queue_status
                } else {
                    row.delivery_status
                },
            })
            .collect())
    }

    pub async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, AccountQuotaRow>(
            r#"
            SELECT
                a.quota_mb,
                COALESCE((
                    SELECT SUM(logical_messages.size_octets)::BIGINT
                    FROM (
                        SELECT DISTINCT m.id, m.size_octets
                        FROM mailbox_messages mm
                        JOIN messages m
                          ON m.tenant_id = mm.tenant_id
                         AND m.id = mm.message_id
                        WHERE mm.tenant_id = a.tenant_id
                          AND mm.account_id = a.id
                          AND mm.visibility <> 'expunged'
                    ) logical_messages
                ), 0)::BIGINT AS quota_used_octets
            FROM accounts a
            WHERE a.tenant_id = $1 AND a.id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(JmapQuota {
            id: "mail".to_string(),
            name: "Mail".to_string(),
            used: row.quota_used_octets.max(0) as u64,
            hard_limit: (row.quota_mb.max(0) as u64) * 1024 * 1024,
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn fetch_mailbox_logical_quota_used_octets(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<u64> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let used = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(SUM(m.size_octets), 0)::BIGINT
            FROM mailbox_messages mm
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.mailbox_id = $3
              AND mm.visibility <> 'expunged'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(used.max(0) as u64)
    }

    #[allow(dead_code)]
    pub(crate) async fn fetch_domain_logical_quota_used_octets(
        &self,
        tenant_id: &Uuid,
        domain_id: Uuid,
    ) -> Result<u64> {
        let used = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE((
                SELECT SUM(logical_messages.size_octets)::BIGINT
                FROM (
                    SELECT DISTINCT m.id, m.size_octets
                    FROM messages m
                    JOIN mailbox_messages mm
                      ON mm.tenant_id = m.tenant_id
                     AND mm.message_id = m.id
                     AND mm.visibility <> 'expunged'
                    WHERE m.tenant_id = $1
                      AND m.domain_id = $2
                ) logical_messages
            ), 0)::BIGINT
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(used.max(0) as u64)
    }

    pub async fn fetch_jmap_message_blob(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT b.media_type, b.size_octets, b.blob_bytes
            FROM messages m
            JOIN blobs b
              ON b.tenant_id = m.tenant_id
             AND b.domain_id = m.domain_id
             AND b.id = m.blob_id
             AND b.blob_kind = 'raw_message'
            WHERE m.tenant_id = $1
              AND m.id = $2
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = m.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = m.id
                    AND mm.visibility = 'visible'
              )
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let raw_bytes: Vec<u8> = row.try_get("blob_bytes").unwrap_or_default();
            let blob_bytes = strip_protected_bcc_headers(&raw_bytes);
            JmapUploadBlob {
                id: message_id,
                account_id,
                media_type: row
                    .try_get("media_type")
                    .unwrap_or_else(|_| "message/rfc822".to_string()),
                octet_size: blob_bytes.len() as u64,
                blob_bytes,
            }
        }))
    }

    pub async fn fetch_jmap_message_blob_with_protected_headers(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT b.media_type, b.size_octets, b.blob_bytes
            FROM messages m
            JOIN blobs b
              ON b.tenant_id = m.tenant_id
             AND b.domain_id = m.domain_id
             AND b.id = m.blob_id
             AND b.blob_kind = 'raw_message'
            WHERE m.tenant_id = $1
              AND m.id = $2
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = m.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = m.id
                    AND mm.visibility = 'visible'
              )
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: message_id,
            account_id,
            media_type: row
                .try_get("media_type")
                .unwrap_or_else(|_| "message/rfc822".to_string()),
            octet_size: row
                .try_get::<i64, _>("size_octets")
                .unwrap_or_default()
                .max(0) as u64,
            blob_bytes: row.try_get("blob_bytes").unwrap_or_default(),
        }))
    }

    pub async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO jmap_upload_blobs (
                id, tenant_id, account_id, media_type, size_octets,
                content_sha256, blob_bytes, magika_status, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'valid', NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(media_type.trim())
        .bind(blob_bytes.len() as i64)
        .bind(crate::sha256_hex(blob_bytes))
        .bind(blob_bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(JmapUploadBlob {
            id,
            account_id,
            media_type: media_type.trim().to_string(),
            octet_size: blob_bytes.len() as u64,
            blob_bytes: blob_bytes.to_vec(),
        })
    }

    pub async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, JmapUploadBlobRow>(
            r#"
            SELECT id, account_id, media_type, size_octets AS octet_size, blob_bytes
            FROM jmap_upload_blobs
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
              AND consumed_at IS NULL
              AND expires_at > NOW()
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(blob_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: row.id,
            account_id: row.account_id,
            media_type: row.media_type,
            octet_size: row.octet_size.max(0) as u64,
            blob_bytes: row.blob_bytes,
        }))
    }

    pub async fn store_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
        snapshot_json: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = activesync_collection_kind(collection_id);
        let state_json = serde_json::from_str::<Value>(snapshot_json.trim())?;
        let last_change_sequence = self
            .fetch_canonical_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        sqlx::query(
            r#"
            INSERT INTO activesync_sync_cursors (
                id, tenant_id, account_id, device_id, collection_kind, collection_key,
                sync_key, last_change_sequence, state_json, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() + INTERVAL '30 days')
            ON CONFLICT (tenant_id, account_id, device_id, collection_kind, collection_key)
            DO UPDATE SET
                sync_key = EXCLUDED.sync_key,
                last_change_sequence = EXCLUDED.last_change_sequence,
                state_json = EXCLUDED.state_json,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .bind(last_change_sequence)
        .bind(state_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_device(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<Option<ActiveSyncDeviceState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, ActiveSyncDeviceRow>(
            r#"
            SELECT account_id, device_id, device_type, policy_key, pending_policy_key,
                   provision_status, wipe_status, account_wipe_status, last_seen_at::text AS last_seen_at
            FROM activesync_devices
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(active_sync_device_state_from_row))
    }

    pub async fn store_activesync_device_pending_policy(
        &self,
        account_id: Uuid,
        device_id: &str,
        device_type: &str,
        pending_policy_key: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let device_type = normalized_device_type(device_type);
        sqlx::query(
            r#"
            INSERT INTO activesync_devices (
                id, tenant_id, account_id, device_id, device_type, pending_policy_key,
                provision_status, last_seen_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
            ON CONFLICT (tenant_id, account_id, device_id)
            DO UPDATE SET
                device_type = EXCLUDED.device_type,
                pending_policy_key = EXCLUDED.pending_policy_key,
                provision_status = 'pending',
                last_seen_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(device_type)
        .bind(pending_policy_key.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn acknowledge_activesync_device_policy(
        &self,
        account_id: Uuid,
        device_id: &str,
        device_type: &str,
        policy_key: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let device_type = normalized_device_type(device_type);
        sqlx::query(
            r#"
            INSERT INTO activesync_devices (
                id, tenant_id, account_id, device_id, device_type, policy_key,
                provision_status, last_seen_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'active', NOW())
            ON CONFLICT (tenant_id, account_id, device_id)
            DO UPDATE SET
                device_type = EXCLUDED.device_type,
                policy_key = EXCLUDED.policy_key,
                pending_policy_key = NULL,
                provision_status = 'active',
                last_seen_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(device_type)
        .bind(policy_key.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn touch_activesync_device(&self, account_id: Uuid, device_id: &str) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            UPDATE activesync_devices
            SET last_seen_at = NOW(), updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn cleanup_expired_activesync_sync_cursors(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            DELETE FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND expires_at <= NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = activesync_collection_kind(collection_id);
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, state_json::text AS snapshot_json
            FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_kind = $4
              AND collection_key = $5
              AND sync_key = $6
              AND expires_at > NOW()
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }

    pub async fn fetch_activesync_email_states(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.normalized_subject,
                    COALESCE(left(b.body_text, 160), ''),
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN NOT mm.is_seen THEN '1' ELSE '0' END,
                    CASE WHEN mm.is_flagged THEN '1' ELSE '0' END,
                    COALESCE(fr.display_name, ''),
                    COALESCE(fr.address, ''),
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END)
                ) AS fingerprint
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.mailbox_id = $3
             AND mm.visibility <> 'expunged'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC, m.id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_email_states_by_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.normalized_subject,
                    COALESCE(left(b.body_text, 160), ''),
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN NOT mm.is_seen THEN '1' ELSE '0' END,
                    CASE WHEN mm.is_flagged THEN '1' ELSE '0' END,
                    COALESCE(fr.display_name, ''),
                    COALESCE(fr.address, ''),
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END)
                ) AS fingerprint
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.mailbox_id = $3
             AND mm.visibility <> 'expunged'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.id = ANY($4)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2
            ORDER BY display_name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1 AND owner_account_id = $2
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_message_attachments(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<ActiveSyncAttachment>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT a.id, a.message_id, a.file_name, a.domain_id, a.blob_id, a.size_octets
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.account_id = a.account_id
             AND mm.message_id = a.message_id
             AND mm.visibility = 'visible'
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
            ORDER BY a.file_name ASC, a.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_all(&self.pool)
        .await?;

        let blob_store = PostgresBlobStore;
        let mut attachments = Vec::with_capacity(rows.len());
        for row in rows {
            let attachment_id: Uuid = row.try_get("id")?;
            let message_id: Uuid = row.try_get("message_id")?;
            let domain_id: Uuid = row.try_get("domain_id")?;
            let blob_id: Uuid = row.try_get("blob_id")?;
            let Some(blob) = blob_store
                .stat_durable_blob(
                    &self.pool,
                    &tenant_id,
                    domain_id,
                    DurableBlobKind::Attachment,
                    blob_id,
                )
                .await?
            else {
                continue;
            };
            attachments.push(ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: row.try_get("file_name")?,
                media_type: blob.media_type,
                size_octets: row
                    .try_get::<i64, _>("size_octets")
                    .unwrap_or(blob.size_octets)
                    .max(0) as u64,
                file_reference: format!("attachment:{message_id}:{attachment_id}"),
            });
        }
        Ok(attachments)
    }

    pub async fn fetch_activesync_attachment_content(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let Some((message_id, attachment_id)) =
            crate::parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let row = sqlx::query(
            r#"
            SELECT a.file_name, a.domain_id, a.blob_id
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.account_id = a.account_id
             AND mm.message_id = a.message_id
             AND mm.visibility = 'visible'
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND a.account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(
                &self.pool,
                &tenant_id,
                row.try_get("domain_id")?,
                DurableBlobKind::Attachment,
                row.try_get("blob_id")?,
            )
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(ActiveSyncAttachmentContent {
            file_reference: file_reference.trim().to_string(),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: blob.media_type,
            blob_bytes: blob.bytes,
        }))
    }

    pub async fn fetch_message_attachment_content_by_cid(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        content_id: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let content_id = content_id.trim().trim_matches(['<', '>']);
        if content_id.is_empty() {
            return Ok(None);
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT a.id, a.file_name, a.domain_id, a.blob_id
            FROM attachments a
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
              AND lower(btrim(btrim(a.content_id), '<>')) = lower($4)
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY a.ordinal ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(content_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(
                &self.pool,
                &tenant_id,
                row.try_get("domain_id")?,
                DurableBlobKind::Attachment,
                row.try_get("blob_id")?,
            )
            .await?
        else {
            return Ok(None);
        };
        let attachment_id = row.try_get("id").unwrap_or(message_id);
        Ok(Some(ActiveSyncAttachmentContent {
            file_reference: format!("attachment:{message_id}:{attachment_id}"),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: blob.media_type,
            blob_bytes: blob.bytes,
        }))
    }

    pub async fn add_message_attachment(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> Result<Option<(JmapEmail, ActiveSyncAttachment)>> {
        let file_name = attachment.file_name.trim();
        if file_name.is_empty() {
            bail!("attachment file name is required");
        }
        let media_type = attachment.media_type.trim();
        if media_type.is_empty() {
            bail!("attachment media type is required");
        }

        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query(
            r#"
            SELECT id
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(existing_membership) = exists else {
            tx.commit().await?;
            return Ok(None);
        };

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let attachment_ids = self
            .ingest_message_attachments_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                message_id,
                &[attachment],
            )
            .await?;
        let Some(attachment_id) = attachment_ids.into_iter().next() else {
            tx.commit().await?;
            return Ok(None);
        };

        self.assign_message_attachments_membership_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            message_id,
            existing_membership.try_get("id")?,
        )
        .await?;

        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "attachment",
            attachment_id,
            "created",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "attachmentId": attachment_id
            }),
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $3, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $4 AND message_id = $2
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(modseq)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        let email = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("message not found after attachment creation"))?;
        let attachment = self
            .fetch_activesync_message_attachments(account_id, message_id)
            .await?
            .into_iter()
            .find(|attachment| attachment.id == attachment_id)
            .ok_or_else(|| anyhow!("attachment not found after creation"))?;

        Ok(Some((email, attachment)))
    }

    pub async fn delete_message_attachment(
        &self,
        account_id: Uuid,
        file_reference: &str,
        audit: AuditEntryInput,
    ) -> Result<Option<JmapEmail>> {
        let Some((message_id, attachment_id)) =
            crate::parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;

        let deleted = sqlx::query(
            r#"
            DELETE FROM attachments a
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND a.account_id = $4
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            RETURNING a.message_id
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if deleted.is_none() {
            tx.commit().await?;
            return Ok(None);
        }

        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "attachment",
            attachment_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "attachmentId": attachment_id
            }),
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE messages
            SET
                has_attachments = EXISTS (
                    SELECT 1
                    FROM attachments
                    WHERE tenant_id = $1 AND message_id = $2
                )
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $3, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $4 AND message_id = $2
              AND visibility = 'visible'
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(modseq)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next())
    }
}

fn jmap_query_hash<T: Serialize>(value: Option<&T>) -> Result<String> {
    let bytes = serde_json::to_vec(&value)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn is_system_mailbox_role(role: &str) -> bool {
    let role = role.trim();
    !role.is_empty() && !role.eq_ignore_ascii_case("custom")
}

fn strip_protected_bcc_headers(raw: &[u8]) -> Vec<u8> {
    let (header_end, separator_len) =
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            (position, 4)
        } else if let Some(position) = raw.windows(2).position(|window| window == b"\n\n") {
            (position, 2)
        } else {
            return raw.to_vec();
        };

    let header = &raw[..header_end];
    let mut stripped = Vec::with_capacity(raw.len());
    let mut offset = 0;
    let mut skipping_bcc = false;
    while offset < header.len() {
        let relative_line_end = header[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| offset + position + 1)
            .unwrap_or(header.len());
        let line = &header[offset..relative_line_end];
        let line_without_eol = line
            .strip_suffix(b"\r\n")
            .or_else(|| line.strip_suffix(b"\n"))
            .unwrap_or(line);
        let is_continuation = matches!(line_without_eol.first(), Some(b' ' | b'\t'));

        if !is_continuation {
            skipping_bcc = header_name_is_bcc(line_without_eol);
        }
        if !skipping_bcc {
            stripped.extend_from_slice(line);
        }
        offset = relative_line_end;
    }

    stripped.extend_from_slice(&raw[header_end..header_end + separator_len]);
    stripped.extend_from_slice(&raw[header_end + separator_len..]);
    stripped
}

fn header_name_is_bcc(line: &[u8]) -> bool {
    let Some(colon) = line.iter().position(|byte| *byte == b':') else {
        return false;
    };
    line[..colon].trim_ascii().eq_ignore_ascii_case(b"bcc")
}

pub(crate) fn activesync_collection_kind(collection_id: &str) -> &'static str {
    match collection_id.trim() {
        "__folders__" => "folders",
        "contacts" => "contacts",
        "calendar" => "calendar",
        "tasks" => "tasks",
        _ => "mail",
    }
}

fn active_sync_device_state_from_row(row: ActiveSyncDeviceRow) -> ActiveSyncDeviceState {
    ActiveSyncDeviceState {
        account_id: row.account_id,
        device_id: row.device_id,
        device_type: row.device_type,
        policy_key: row.policy_key,
        pending_policy_key: row.pending_policy_key,
        provision_status: row.provision_status,
        wipe_status: row.wipe_status,
        account_wipe_status: row.account_wipe_status,
        last_seen_at: row.last_seen_at,
    }
}

fn normalized_device_type(device_type: &str) -> String {
    let trimmed = device_type.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn jmap_change_kind(change_kind: &str) -> String {
    match change_kind {
        "destroyed" | "expunged" => "destroyed",
        "created" => "created",
        _ => "updated",
    }
    .to_string()
}

fn jmap_exact_object_kind(data_type: &str) -> Option<&'static str> {
    match data_type {
        "AddressBook" => Some("contact_book"),
        "ContactCard" => Some("contact"),
        "Calendar" => Some("calendar"),
        "CalendarEvent" => Some("calendar_event"),
        "TaskList" => Some("task_list"),
        "Task" => Some("task"),
        "Note" => Some("note"),
        "JournalEntry" => Some("journal_entry"),
        "EmailSubmission" => Some("submission"),
        _ => None,
    }
}

fn jmap_replay_object_id(
    data_type: &str,
    object_kind: &str,
    object_id: Uuid,
    summary_json: &Value,
) -> Option<Uuid> {
    if object_kind == jmap_exact_object_kind(data_type)? {
        return Some(object_id);
    }
    match (data_type, object_kind) {
        ("AddressBook", "contact_book_grant")
        | ("Calendar", "calendar_grant")
        | ("TaskList", "task_list_grant") => summary_json
            .get("collectionId")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        _ => None,
    }
}

fn jmap_object_replay_kinds(data_type: &str) -> Option<Vec<&'static str>> {
    match data_type {
        "AddressBook" => Some(vec!["contact_book", "contact_book_grant"]),
        "ContactCard" => Some(vec!["contact", "contact_book", "contact_book_grant"]),
        "Calendar" => Some(vec!["calendar", "calendar_grant"]),
        "CalendarEvent" => Some(vec!["calendar_event", "calendar", "calendar_grant"]),
        "TaskList" => Some(vec!["task_list", "task_list_grant"]),
        "Task" => Some(vec!["task", "task_list", "task_list_grant"]),
        "Note" => Some(vec!["note"]),
        "JournalEntry" => Some(vec!["journal_entry"]),
        "EmailSubmission" => Some(vec!["submission"]),
        "Share" => Some(vec![
            "mailbox_delegation_grant",
            "sender_right",
            "contact_book_grant",
            "calendar_grant",
            "task_list_grant",
        ]),
        "Reminder" => Some(vec!["task", "calendar_event", "mailbox_message"]),
        _ => None,
    }
}

fn summary_json_reminder_changed(summary_json: &Value) -> bool {
    summary_json
        .get("reminderChanged")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        activesync_collection_kind, is_system_mailbox_role, jmap_exact_object_kind,
        jmap_object_replay_kinds, jmap_replay_object_id, strip_protected_bcc_headers,
    };
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn activesync_collection_kind_maps_builtin_and_mail_collections() {
        assert_eq!(activesync_collection_kind("__folders__"), "folders");
        assert_eq!(activesync_collection_kind("contacts"), "contacts");
        assert_eq!(activesync_collection_kind("calendar"), "calendar");
        assert_eq!(
            activesync_collection_kind("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            "mail"
        );
    }

    #[test]
    fn jmap_object_replay_kinds_include_visibility_dependencies() {
        assert_eq!(jmap_exact_object_kind("ContactCard"), Some("contact"));
        assert_eq!(
            jmap_object_replay_kinds("ContactCard").unwrap(),
            vec!["contact", "contact_book", "contact_book_grant"]
        );
        assert_eq!(
            jmap_exact_object_kind("CalendarEvent"),
            Some("calendar_event")
        );
        assert_eq!(
            jmap_object_replay_kinds("CalendarEvent").unwrap(),
            vec!["calendar_event", "calendar", "calendar_grant"]
        );
        assert_eq!(jmap_exact_object_kind("Task"), Some("task"));
        assert_eq!(
            jmap_object_replay_kinds("Task").unwrap(),
            vec!["task", "task_list", "task_list_grant"]
        );
        assert_eq!(jmap_exact_object_kind("Note"), Some("note"));
        assert_eq!(jmap_object_replay_kinds("Note").unwrap(), vec!["note"]);
        assert_eq!(
            jmap_exact_object_kind("JournalEntry"),
            Some("journal_entry")
        );
        assert_eq!(
            jmap_object_replay_kinds("JournalEntry").unwrap(),
            vec!["journal_entry"]
        );
        assert_eq!(
            jmap_exact_object_kind("EmailSubmission"),
            Some("submission")
        );
        assert_eq!(
            jmap_object_replay_kinds("EmailSubmission").unwrap(),
            vec!["submission"]
        );
        assert_eq!(
            jmap_object_replay_kinds("Share").unwrap(),
            vec![
                "mailbox_delegation_grant",
                "sender_right",
                "contact_book_grant",
                "calendar_grant",
                "task_list_grant"
            ]
        );
        assert_eq!(
            jmap_object_replay_kinds("Reminder").unwrap(),
            vec!["task", "calendar_event", "mailbox_message"]
        );
    }

    #[test]
    fn jmap_collection_replay_maps_grant_rows_to_collection_ids() {
        let grant_id = Uuid::new_v4();
        let collection_id = Uuid::new_v4();
        let summary = json!({ "collectionId": collection_id });

        assert_eq!(
            jmap_replay_object_id("AddressBook", "contact_book_grant", grant_id, &summary),
            Some(collection_id)
        );
        assert_eq!(
            jmap_replay_object_id("Calendar", "calendar_grant", grant_id, &summary),
            Some(collection_id)
        );
        assert_eq!(
            jmap_replay_object_id("TaskList", "task_list_grant", grant_id, &summary),
            Some(collection_id)
        );
        assert_eq!(
            jmap_replay_object_id("ContactCard", "contact_book_grant", grant_id, &summary),
            None
        );
    }

    #[test]
    fn jmap_collection_replay_keeps_exact_object_rows() {
        let object_id = Uuid::new_v4();

        assert_eq!(
            jmap_replay_object_id("AddressBook", "contact_book", object_id, &json!({})),
            Some(object_id)
        );
        assert_eq!(
            jmap_replay_object_id("CalendarEvent", "calendar", object_id, &json!({})),
            None
        );
    }

    #[test]
    fn custom_mailbox_role_is_user_managed() {
        assert!(!is_system_mailbox_role(""));
        assert!(!is_system_mailbox_role("custom"));
        assert!(!is_system_mailbox_role(" CUSTOM "));
        assert!(is_system_mailbox_role("inbox"));
        assert!(is_system_mailbox_role("sent"));
        assert!(is_system_mailbox_role("drafts"));
        assert!(is_system_mailbox_role("trash"));
    }

    #[test]
    fn protected_bcc_headers_are_stripped_from_default_message_blobs() {
        let raw = b"From: a@example.test\r\nBcc: hidden@example.test\r\n\tfolded@example.test\r\nTo: b@example.test\r\n\r\nbody";
        let stripped = strip_protected_bcc_headers(raw);
        let rendered = String::from_utf8(stripped).unwrap();

        assert!(!rendered.contains("Bcc:"));
        assert!(!rendered.contains("hidden@example.test"));
        assert!(!rendered.contains("folded@example.test"));
        assert!(rendered.contains("From: a@example.test"));
        assert!(rendered.contains("To: b@example.test"));
        assert!(rendered.ends_with("\r\n\r\nbody"));
    }
}
