use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{AuditEntryInput, JmapEmail, JmapEmailFollowupUpdate, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageFlagUpdate {
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
}

impl MessageFlagUpdate {
    pub fn into_followup_update(self) -> JmapEmailFollowupUpdate {
        JmapEmailFollowupUpdate {
            unread: self.unread,
            flagged: self.flagged,
            followup_flag_status: self.flagged.map(|flagged| {
                if flagged {
                    "flagged".to_string()
                } else {
                    "none".to_string()
                }
            }),
            ..Default::default()
        }
    }
}

pub async fn update_message_flags(
    storage: &Storage,
    account_id: Uuid,
    message_id: Uuid,
    update: MessageFlagUpdate,
    audit: AuditEntryInput,
) -> Result<JmapEmail> {
    storage
        .update_jmap_email_followup_flags(
            account_id,
            message_id,
            update.into_followup_update(),
            audit,
        )
        .await
}

pub async fn update_imap_flags(
    storage: &Storage,
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
    let tenant_id = storage.tenant_id_for_account_id(account_id).await?;
    let mut tx = storage.pool.begin().await?;
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

    let modseq = storage
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
            Storage::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in rows {
            let membership_id: Uuid = row.try_get("id")?;
            let message_id: Uuid = row.try_get("message_id")?;
            Storage::insert_mail_change_log_in_tx(
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
        Storage::recalculate_mailbox_counts_in_tx(
            &mut tx, &tenant_id, account_id, mailbox_id, modseq,
        )
        .await?;
    }

    Storage::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
    tx.commit().await?;

    Ok(modified_ids)
}

pub async fn expunge_imap_deleted(
    storage: &Storage,
    account_id: Uuid,
    mailbox_id: Uuid,
    message_ids: &[Uuid],
    audit: AuditEntryInput,
) -> Result<()> {
    if message_ids.is_empty() {
        return Ok(());
    }
    let tenant_id = storage.tenant_id_for_account_id(account_id).await?;
    let mut tx = storage.pool.begin().await?;

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
        let modseq = storage
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Storage::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in &expunge_rows {
            let membership_id: Uuid = row.try_get("id")?;
            let message_id: Uuid = row.try_get("message_id")?;
            let imap_uid: i64 = row.try_get("imap_uid")?;
            let cursor = Storage::insert_mail_change_log_in_tx(
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
        Storage::recalculate_mailbox_counts_in_tx(
            &mut tx, &tenant_id, account_id, mailbox_id, modseq,
        )
        .await?;
        storage.insert_audit(&mut tx, &tenant_id, audit).await?;
        Storage::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
    }
    tx.commit().await?;

    Ok(())
}

pub async fn delete_custom_jmap_email(
    storage: &Storage,
    account_id: Uuid,
    message_id: Uuid,
    audit: AuditEntryInput,
) -> Result<()> {
    let tenant_id = storage.tenant_id_for_account_id(account_id).await?;
    let mut tx = storage.pool.begin().await?;
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
    delete_jmap_email(storage, account_id, message_id, audit).await?;
    Ok(())
}

pub async fn delete_jmap_email(
    storage: &Storage,
    account_id: Uuid,
    message_id: Uuid,
    audit: AuditEntryInput,
) -> Result<()> {
    delete_jmap_email_memberships(storage, account_id, None, message_id, audit).await
}

pub async fn delete_jmap_email_from_mailbox(
    storage: &Storage,
    account_id: Uuid,
    mailbox_id: Uuid,
    message_id: Uuid,
    audit: AuditEntryInput,
) -> Result<()> {
    delete_jmap_email_memberships(storage, account_id, Some(mailbox_id), message_id, audit).await
}

async fn delete_jmap_email_memberships(
    storage: &Storage,
    account_id: Uuid,
    mailbox_id_filter: Option<Uuid>,
    message_id: Uuid,
    audit: AuditEntryInput,
) -> Result<()> {
    let tenant_id = storage.tenant_id_for_account_id(account_id).await?;
    let recoverable_created_by_protocol = recoverable_created_by_protocol(&audit.action);
    let mut tx = storage.pool.begin().await?;
    let rows = sqlx::query(
        r#"
            SELECT mm.id, mm.mailbox_id, mm.thread_id, mm.imap_uid, mm.is_seen,
                   COALESCE(mb.recoverable_items_retention_days, a.recoverable_items_retention_days) AS recoverable_retention_days,
                   (m.legal_hold OR a.litigation_hold_enabled) AS recoverable_legal_hold
            FROM mailbox_messages mm
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            JOIN accounts a
              ON a.tenant_id = mm.tenant_id
             AND a.id = mm.account_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.message_id = $3
              AND ($4::uuid IS NULL OR mm.mailbox_id = $4)
              AND mm.visibility = 'visible'
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

    let modseq = storage
        .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
        .await?;
    let principals =
        Storage::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
    for row in &rows {
        let membership_id: Uuid = row.try_get("id")?;
        let mailbox_id: Uuid = row.try_get("mailbox_id")?;
        let imap_uid: i64 = row.try_get("imap_uid")?;
        let thread_id: Uuid = row.try_get("thread_id")?;
        let cursor = Storage::insert_mail_change_log_in_tx(
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
                "threadId": thread_id,
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

        let recoverable_item_id = Uuid::new_v4();
        let recoverable_retention_days: i32 = row.try_get("recoverable_retention_days")?;
        let recoverable_legal_hold: bool = row.try_get("recoverable_legal_hold")?;
        sqlx::query(
            r#"
                INSERT INTO recoverable_items (
                    id, tenant_id, account_id, message_id, source_mailbox_message_id,
                    source_mailbox_id, source_imap_uid, source_thread_id,
                    recoverable_folder, delete_kind, retained_until, legal_hold,
                    created_by_protocol
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8,
                    'deletions', 'hard_delete',
                    CASE WHEN $9::integer = 0 THEN NOW() ELSE NOW() + ($9::integer * INTERVAL '1 day') END,
                    $10,
                    $11
                )
                ON CONFLICT (tenant_id, account_id, source_mailbox_message_id) DO NOTHING
                "#,
        )
        .bind(recoverable_item_id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(membership_id)
        .bind(mailbox_id)
        .bind(imap_uid)
        .bind(thread_id)
        .bind(recoverable_retention_days)
        .bind(recoverable_legal_hold)
        .bind(recoverable_created_by_protocol)
        .execute(&mut *tx)
        .await?;

        Storage::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "recoverable_item",
            recoverable_item_id,
            "created",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "sourceMailboxMessageId": membership_id,
                "recoverableFolder": "deletions",
                "sourceMailboxId": mailbox_id,
                "sourceImapUid": imap_uid
            }),
        )
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
        Storage::recalculate_mailbox_counts_in_tx(
            &mut tx, &tenant_id, account_id, mailbox_id, modseq,
        )
        .await?;
    }

    storage.insert_audit(&mut tx, &tenant_id, audit).await?;
    Storage::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
    tx.commit().await?;

    Ok(())
}

fn recoverable_created_by_protocol(audit_action: &str) -> &'static str {
    match audit_action {
        action if action.starts_with("mapi-") => "mapi",
        action if action.starts_with("ews-") => "ews",
        action if action.starts_with("imap-") => "imap",
        action if action.starts_with("jmap-") => "jmap",
        _ => "api",
    }
}

#[cfg(test)]
mod tests {
    use super::MessageFlagUpdate;

    #[test]
    fn message_flag_update_projects_followup_flag_status() {
        let flagged = MessageFlagUpdate {
            unread: Some(false),
            flagged: Some(true),
        }
        .into_followup_update();
        assert_eq!(flagged.unread, Some(false));
        assert_eq!(flagged.flagged, Some(true));
        assert_eq!(flagged.followup_flag_status.as_deref(), Some("flagged"));

        let unflagged = MessageFlagUpdate {
            unread: None,
            flagged: Some(false),
        }
        .into_followup_update();
        assert_eq!(unflagged.followup_flag_status.as_deref(), Some("none"));

        let read_only = MessageFlagUpdate {
            unread: Some(true),
            flagged: None,
        }
        .into_followup_update();
        assert_eq!(read_only.unread, Some(true));
        assert_eq!(read_only.followup_flag_status, None);
    }
}
