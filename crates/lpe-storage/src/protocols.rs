use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    submission,
    submission::{AttachmentUploadInput, SubmittedRecipientInput},
    AuditEntryInput, JmapEmailRecipientRow, JmapEmailRow, JmapEmailSubmissionRow,
    MessageBccRecipientRecordRow, Storage, DEFAULT_TASK_LIST_ROLE,
};

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
            if object_kind == "recoverable_item" {
                continue;
            }
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
        crate::mail_items::update_imap_flags(
            self,
            account_id,
            mailbox_id,
            message_ids,
            unread,
            flagged,
            deleted,
            unchanged_since,
        )
        .await
    }

    pub async fn expunge_imap_deleted(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &[Uuid],
        audit: AuditEntryInput,
    ) -> Result<()> {
        crate::mail_items::expunge_imap_deleted(self, account_id, mailbox_id, message_ids, audit)
            .await
    }

    pub async fn delete_custom_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        crate::mail_items::delete_custom_jmap_email(self, account_id, message_id, audit).await
    }

    pub async fn delete_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        crate::mail_items::delete_jmap_email(self, account_id, message_id, audit).await
    }

    pub async fn delete_jmap_email_from_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        crate::mail_items::delete_jmap_email_from_mailbox(
            self, account_id, mailbox_id, message_id, audit,
        )
        .await
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
        "SearchFolder" => Some("search_folder_definition"),
        "Rule" => Some("sieve_script"),
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
        "SearchFolder" => Some(vec!["search_folder_definition"]),
        "Rule" => Some(vec!["sieve_script"]),
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
    use super::{jmap_exact_object_kind, jmap_object_replay_kinds, jmap_replay_object_id};
    use serde_json::json;
    use uuid::Uuid;

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
            jmap_exact_object_kind("SearchFolder"),
            Some("search_folder_definition")
        );
        assert_eq!(
            jmap_object_replay_kinds("SearchFolder").unwrap(),
            vec!["search_folder_definition"]
        );
        assert_eq!(jmap_exact_object_kind("Rule"), Some("sieve_script"));
        assert_eq!(
            jmap_object_replay_kinds("Rule").unwrap(),
            vec!["sieve_script"]
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
}
