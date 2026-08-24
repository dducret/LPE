use anyhow::{bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    mapi_events::merge_predecessor_change_list,
    mapi_message_identity::{
        rekey_active_mapi_message_identity_for_server_move_in_tx,
        rotate_active_mapi_message_identity_in_tx,
    },
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
        ensure_mapi_store_identity_in_tx, mapi_store_id, MapiMessageIdentityMove,
        MapiMessageImportedMoveIdentity, MapiMessageMoveResult, MAPI_FIRST_GLOBAL_COUNTER,
        MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
    sha256_hex, submission, ActiveSyncSyncState, ActiveSyncSyncStateRow, AuditEntryInput,
    CanonicalChangeCategory, JmapEmail, JmapImportedEmailInput, Storage,
};

impl Storage {
    pub async fn delete_client_contact(&self, account_id: Uuid, contact_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(contact_id)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            bail!("contact not found");
        }

        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            account_id,
            None,
            "contact",
            contact_id,
            None,
            &[account_id],
        )
        .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(contact_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("contact not found");
        }

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_client_event(&self, account_id: Uuid, event_id: Uuid) -> Result<()> {
        self.move_accessible_event_to_deleted_items(account_id, event_id, None)
            .await?;
        Ok(())
    }

    pub async fn copy_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.copy_jmap_email_between_accounts(
            account_id,
            account_id,
            message_id,
            target_mailbox_id,
            audit,
        )
        .await
    }

    pub async fn copy_jmap_email_between_accounts(
        &self,
        source_account_id: Uuid,
        target_account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(source_account_id).await?;
        if self.tenant_id_for_account_id(target_account_id).await? != tenant_id {
            bail!("source and target mailbox accounts must belong to the same tenant");
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(target_account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        self.lock_message_for_mime_graph_in_tx(&mut tx, &tenant_id, message_id)
            .await?;
        let source = sqlx::query(
            r#"
            SELECT id, message_id, thread_id, is_seen, is_flagged,
                   received_at::text AS received_at
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
        .bind(source_account_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        let source_membership_id: Uuid = source.try_get("id")?;
        let target_contains_message = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = $4
                  AND visibility <> 'expunged'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(target_account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
        if target_contains_message {
            bail!("message already exists in target mailbox");
        }

        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                &mut tx,
                &tenant_id,
                target_account_id,
                target_mailbox_id,
                message_id,
                source.try_get("thread_id")?,
                &source.try_get::<String, _>("received_at")?,
                source.try_get("is_seen")?,
                source.try_get("is_flagged")?,
                target_role == "drafts",
                "created",
            )
            .await?;
        self.mark_calendar_mail_classification_applied_for_first_visible_membership_in_tx(
            &mut tx,
            &tenant_id,
            target_account_id,
            message_id,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            SELECT
                tenant_id, $6, $5, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            FROM mail_search_documents
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_message_id = $3
              AND message_id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(source_account_id)
        .bind(source_membership_id)
        .bind(message_id)
        .bind(membership_id)
        .bind(target_account_id)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, target_account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(target_account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("copied message not found"))
    }

    pub async fn move_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.move_jmap_email_membership(
            account_id,
            None,
            message_id,
            target_mailbox_id,
            None,
            audit,
        )
        .await
        .map(|(email, _)| email)
    }

    pub async fn move_jmap_email_from_mailbox(
        &self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.move_jmap_email_membership(
            account_id,
            Some(source_mailbox_id),
            message_id,
            target_mailbox_id,
            None,
            audit,
        )
        .await
        .map(|(email, _)| email)
    }

    pub async fn move_jmap_email_from_mailbox_with_mapi_identity(
        &self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        imported_identity: MapiMessageImportedMoveIdentity,
        audit: AuditEntryInput,
    ) -> Result<MapiMessageMoveResult> {
        let (email, identity) = self
            .move_jmap_email_membership(
                account_id,
                Some(source_mailbox_id),
                message_id,
                target_mailbox_id,
                Some(&imported_identity),
                audit,
            )
            .await?;
        let identity =
            identity.ok_or_else(|| anyhow::anyhow!("MAPI message move did not rekey identity"))?;
        Ok(MapiMessageMoveResult { email, identity })
    }

    async fn move_jmap_email_membership(
        &self,
        account_id: Uuid,
        source_mailbox_id: Option<Uuid>,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        imported_identity: Option<&MapiMessageImportedMoveIdentity>,
        audit: AuditEntryInput,
    ) -> Result<(JmapEmail, Option<MapiMessageIdentityMove>)> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let source = sqlx::query(
            r#"
            SELECT id, mailbox_id, thread_id, imap_uid, is_seen, is_flagged, keywords,
                   received_at::text AS received_at
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND ($4::uuid IS NULL OR mailbox_id = $4)
              AND visibility = 'visible'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(source_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        let source_mailbox_id: Uuid = source.try_get("mailbox_id")?;
        if source_mailbox_id == target_mailbox_id {
            tx.rollback().await?;
            let email = self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("moved message not found"))?;
            return Ok((email, None));
        }
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let target_contains_message = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = $4
                  AND visibility <> 'expunged'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
        if target_contains_message && imported_identity.is_none() {
            bail!("message already exists in target mailbox");
        }
        if target_contains_message {
            let imported_identity = imported_identity
                .ok_or_else(|| anyhow::anyhow!("MAPI move identity is required"))?;
            let source_membership_id: Uuid = source.try_get("id")?;
            let source_imap_uid: i64 = source.try_get("imap_uid")?;
            let thread_id: Uuid = source.try_get("thread_id")?;
            let identity = rekey_mapi_message_identity_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                message_id,
                imported_identity,
            )
            .await?;
            sqlx::query(
                r#"
                UPDATE mailbox_messages
                SET visibility = 'expunged',
                    expunged_at = NOW(),
                    modseq = $4,
                    updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_membership_id)
            .bind(modseq)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                UPDATE mailboxes
                SET total_messages = GREATEST(0, total_messages - 1),
                    unread_messages = GREATEST(0, unread_messages - CASE WHEN $4 THEN 0 ELSE 1 END),
                    modseq = GREATEST(modseq + 1, $5),
                    updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_mailbox_id)
            .bind(source.try_get::<bool, _>("is_seen")?)
            .bind(modseq)
            .execute(&mut *tx)
            .await?;
            Self::recalculate_mailbox_counts_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                source_mailbox_id,
                modseq,
            )
            .await?;
            sqlx::query(
                r#"
                DELETE FROM mail_search_documents
                WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_membership_id)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            let principals =
                Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
            let source_cursor = Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(source_mailbox_id),
                "mailbox_message",
                source_membership_id,
                "expunged",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": thread_id,
                    "imapUid": source_imap_uid,
                    "targetMailboxId": target_mailbox_id,
                    "sourceMailboxMessageId": source_membership_id,
                    "sourceImapUid": source_imap_uid,
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
                VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, 'move')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_mailbox_id)
            .bind(source_membership_id)
            .bind(message_id)
            .bind(source_imap_uid)
            .bind(modseq)
            .bind(source_cursor)
            .execute(&mut *tx)
            .await?;
            Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
            tx.commit().await?;

            let email = self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("moved message not found"))?;
            return Ok((email, Some(identity)));
        }
        let target_uid: i64 = sqlx::query_scalar(
            r#"
            UPDATE mailboxes
            SET uid_next = uid_next + 1,
                total_messages = total_messages + 1,
                unread_messages = unread_messages + CASE WHEN $4 THEN 0 ELSE 1 END,
                modseq = GREATEST(modseq + 1, $5),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            RETURNING uid_next - 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(modseq)
        .fetch_one(&mut *tx)
        .await?;
        let source_membership_id: Uuid = source.try_get("id")?;
        let source_imap_uid: i64 = source.try_get("imap_uid")?;
        let thread_id: Uuid = source.try_get("thread_id")?;
        let target_membership_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailbox_messages (
                id, tenant_id, account_id, mailbox_id, message_id, thread_id,
                imap_uid, modseq, is_seen, is_flagged, is_draft, keywords, received_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, COALESCE($13::timestamptz, NOW())
            )
            "#,
        )
        .bind(target_membership_id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .bind(thread_id)
        .bind(target_uid)
        .bind(modseq)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(source.try_get::<bool, _>("is_flagged")?)
        .bind(target_role == "drafts")
        .bind(source.try_get::<Vec<String>, _>("keywords")?)
        .bind(source.try_get::<String, _>("received_at")?)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET visibility = 'expunged',
                expunged_at = NOW(),
                modseq = $4,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_membership_id)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE mailboxes
            SET total_messages = GREATEST(0, total_messages - 1),
                unread_messages = GREATEST(0, unread_messages - CASE WHEN $4 THEN 0 ELSE 1 END),
                modseq = GREATEST(modseq + 1, $5),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_mailbox_id)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        Self::recalculate_mailbox_counts_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            target_mailbox_id,
            modseq,
        )
        .await?;
        Self::recalculate_mailbox_counts_in_tx(
            &mut tx,
            &tenant_id,
            account_id,
            source_mailbox_id,
            modseq,
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            SELECT
                tenant_id, account_id, $4, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            FROM mail_search_documents
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_membership_id)
        .bind(target_membership_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM mail_search_documents
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_membership_id)
        .execute(&mut *tx)
        .await?;

        // [MS-OXCNOTIF] section 2.2.1.4.1.2 requires the historical source
        // and destination MID pair. Snapshot a stable pair or imported rekey
        // before the durable move row is written.
        let identity = match imported_identity {
            Some(imported_identity) => Some(
                rekey_mapi_message_identity_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    message_id,
                    imported_identity,
                )
                .await?,
            ),
            None => {
                rekey_active_mapi_message_identity_for_server_move_in_tx(
                    &mut tx, &tenant_id, account_id, message_id,
                )
                .await?
            }
        };
        let (old_mapi_object_id, new_mapi_object_id, move_identity_snapshot_complete) =
            if let Some(identity) = identity.as_ref() {
                (
                    Some(identity.old_mapi_object_id),
                    Some(identity.new_mapi_object_id),
                    true,
                )
            } else {
                (None, None, false)
            };
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(target_mailbox_id),
            "mailbox_message",
            target_membership_id,
            "moved",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "sourceMailboxId": source_mailbox_id,
                "targetMailboxId": target_mailbox_id,
                "sourceMailboxMessageId": source_membership_id,
                "targetMailboxMessageId": target_membership_id,
                "threadId": thread_id,
                "imapUid": target_uid,
                "sourceImapUid": source_imap_uid,
                "targetImapUid": target_uid,
                "mapiMoveIdentitySnapshotComplete": move_identity_snapshot_complete,
                "oldMapiObjectId": old_mapi_object_id,
                "newMapiObjectId": new_mapi_object_id
            }),
        )
        .await?;
        let source_cursor = Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(source_mailbox_id),
            "mailbox_message",
            source_membership_id,
            "expunged",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "threadId": thread_id,
                "imapUid": source_imap_uid,
                "targetMailboxId": target_mailbox_id,
                "sourceMailboxMessageId": source_membership_id,
                "targetMailboxMessageId": target_membership_id,
                "sourceImapUid": source_imap_uid,
                "targetImapUid": target_uid
            }),
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                message_id, mailbox_message_id, imap_uid, mapi_object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, $10, 'move')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_mailbox_id)
        .bind(source_membership_id)
        .bind(message_id)
        .bind(source_imap_uid)
        .bind(old_mapi_object_id.map(|object_id| object_id as i64))
        .bind(modseq)
        .bind(source_cursor)
        .execute(&mut *tx)
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        let email = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("moved message not found"))?;
        Ok((email, identity))
    }

    pub async fn update_jmap_email_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        crate::mail_items::update_message_flags(
            self,
            account_id,
            message_id,
            crate::mail_items::MessageFlagUpdate { unread, flagged },
            audit,
        )
        .await
    }

    pub async fn update_jmap_email_followup_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        update: crate::JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        if update.unread.is_none()
            && update.flagged.is_none()
            && update.followup_flag_status.is_none()
            && update.followup_icon.is_none()
            && update.todo_item_flags.is_none()
            && update.followup_request.is_none()
            && update.followup_start_at.is_none()
            && update.followup_due_at.is_none()
            && update.followup_completed_at.is_none()
            && update.reminder_set.is_none()
            && update.reminder_at.is_none()
            && update.reminder_dismissed_at.is_none()
            && update.swapped_todo_store_id.is_none()
            && update.swapped_todo_data.is_none()
            && update.categories.is_none()
        {
            return self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("message not found"));
        }
        crate::mail_followup::validate_followup_update(&update)?;
        let categories = update
            .categories
            .map(crate::mail_followup::normalize_mail_categories);
        let reminder_changed = update.reminder_set.is_some()
            || update.reminder_at.is_some()
            || update.reminder_dismissed_at.is_some();
        let non_read_state_change = update.flagged.is_some()
            || update.followup_flag_status.is_some()
            || update.followup_icon.is_some()
            || update.todo_item_flags.is_some()
            || update.followup_request.is_some()
            || update.followup_start_at.is_some()
            || update.followup_due_at.is_some()
            || update.followup_completed_at.is_some()
            || update.reminder_set.is_some()
            || update.reminder_at.is_some()
            || update.reminder_dismissed_at.is_some()
            || update.swapped_todo_store_id.is_some()
            || update.swapped_todo_data.is_some()
            || categories.is_some();

        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET is_seen = CASE WHEN $4::bool IS NULL THEN is_seen ELSE NOT $4 END,
                is_flagged = CASE
                    WHEN $5::bool IS NOT NULL THEN $5
                    WHEN $6::text IS NULL THEN is_flagged
                    ELSE $6 IN ('flagged', 'complete')
                END,
                followup_flag_status = COALESCE($6, followup_flag_status),
                followup_icon = CASE
                    WHEN $6 = 'none' THEN 0
                    WHEN $7::integer IS NOT NULL THEN $7
                    WHEN $6 = 'flagged' AND followup_icon = 0 THEN 6
                    ELSE followup_icon
                END,
                todo_item_flags = CASE
                    WHEN $6 = 'none' THEN 0
                    WHEN $8::integer IS NOT NULL THEN $8
                    WHEN $6 IN ('flagged', 'complete') AND todo_item_flags = 0 THEN 8
                    ELSE todo_item_flags
                END,
                followup_request = COALESCE($9, followup_request),
                followup_start_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $10::text = '' THEN NULL
                    WHEN $10::text IS NOT NULL THEN $10::timestamptz
                    ELSE followup_start_at
                END,
                followup_due_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $11::text = '' THEN NULL
                    WHEN $11::text IS NOT NULL THEN $11::timestamptz
                    ELSE followup_due_at
                END,
                followup_completed_at = CASE
                    WHEN $6 IN ('none', 'flagged') THEN NULL
                    WHEN $12::text IS NOT NULL THEN $12::timestamptz
                    WHEN $6 = 'complete' THEN COALESCE(followup_completed_at, NOW())
                    ELSE followup_completed_at
                END,
                reminder_set = CASE
                    WHEN $6 = 'none' THEN FALSE
                    WHEN $13::bool IS NOT NULL THEN $13
                    ELSE reminder_set
                END,
                reminder_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $14::text = '' THEN NULL
                    WHEN $14::text IS NOT NULL THEN $14::timestamptz
                    ELSE reminder_at
                END,
                reminder_dismissed_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $15::text = '' THEN NULL
                    WHEN $15::text IS NOT NULL THEN $15::timestamptz
                    ELSE reminder_dismissed_at
                END,
                swapped_todo_store_id = COALESCE($16, swapped_todo_store_id),
                swapped_todo_data = COALESCE($17, swapped_todo_data),
                keywords = COALESCE($18, keywords),
                modseq = $19,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
              AND visibility = 'visible'
            RETURNING id, mailbox_id, thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(update.unread)
        .bind(update.flagged)
        .bind(update.followup_flag_status)
        .bind(update.followup_icon)
        .bind(update.todo_item_flags)
        .bind(update.followup_request)
        .bind(update.followup_start_at)
        .bind(update.followup_due_at)
        .bind(update.followup_completed_at)
        .bind(update.reminder_set)
        .bind(update.reminder_at)
        .bind(update.reminder_dismissed_at)
        .bind(update.swapped_todo_store_id)
        .bind(update.swapped_todo_data)
        .bind(categories)
        .bind(modseq)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            bail!("message not found");
        }
        if non_read_state_change {
            // [MS-OXCFXICS] sections 2.2.1.2.3, 2.2.1.2.7, 2.2.1.2.8, and
            // 3.1.5.3 require a new server change version for direct property
            // changes. Read-state-only updates use the separate CnsetRead state.
            rotate_active_mapi_message_identity_in_tx(&mut tx, &tenant_id, account_id, message_id)
                .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        let mut affected_mailbox_ids = rows
            .iter()
            .map(|row| row.try_get::<Uuid, _>("mailbox_id"))
            .collect::<Result<Vec<_>, _>>()?;
        affected_mailbox_ids.sort_unstable();
        affected_mailbox_ids.dedup();
        for row in &rows {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(row.try_get("mailbox_id")?),
                "mailbox_message",
                row.try_get("id")?,
                "updated",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": row.try_get::<Uuid, _>("thread_id")?,
                    "imapUid": row.try_get::<i64, _>("imap_uid")?,
                    "reminderChanged": reminder_changed
                }),
            )
            .await?;
        }
        if update.unread.is_some() {
            for mailbox_id in affected_mailbox_ids {
                Self::recalculate_mailbox_counts_in_tx(
                    &mut tx, &tenant_id, account_id, mailbox_id, modseq,
                )
                .await?;
            }
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("updated message not found"))
    }

    pub async fn update_jmap_email_content(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        subject: Option<String>,
        body_text: Option<String>,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        if subject.is_none() && body_text.is_none() {
            return self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("message not found"));
        }
        let existing = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        let subject_is_unchanged = subject
            .as_ref()
            .is_none_or(|value| existing.subject == crate::normalize_subject(value));
        let body_is_unchanged = body_text
            .as_ref()
            .is_none_or(|value| existing.body_text == *value);
        if subject_is_unchanged && body_is_unchanged {
            return Ok(existing);
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let message = sqlx::query(
            r#"
            SELECT m.domain_id
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id AND mm.message_id = m.id
            WHERE m.tenant_id = $1 AND mm.account_id = $2 AND m.id = $3
              AND mm.visibility = 'visible'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        let domain_id: Uuid = message.try_get("domain_id")?;

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let memberships = self
            .lock_visible_message_memberships_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        if memberships.is_empty() {
            bail!("message not found");
        }
        self.ensure_message_shared_state_is_private_to_account_in_tx(
            &mut tx, &tenant_id, account_id, message_id,
        )
        .await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        if let Some(subject) = subject {
            sqlx::query(
                r#"
                UPDATE messages
                SET normalized_subject = $4
                WHERE tenant_id = $1 AND domain_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(domain_id)
            .bind(message_id)
            .bind(crate::normalize_subject(&subject))
            .execute(&mut *tx)
            .await?;
        }
        if let Some(body_text) = body_text {
            self.upsert_message_body_in_tx(
                &mut tx, &tenant_id, domain_id, message_id, &body_text, None,
            )
            .await?;
        }
        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET modseq = $4, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
              AND visibility = 'visible'
            RETURNING id, mailbox_id, thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(modseq)
        .fetch_all(&mut *tx)
        .await?;
        if rows.len() != memberships.len() {
            bail!("visible message memberships changed while updating content");
        }
        // [MS-OXCFXICS] sections 2.2.1.2.3, 2.2.1.2.7, and 2.2.1.2.8 require
        // a server-side message change to receive a new local CN/ChangeKey and
        // a PCL that integrates that ChangeKey. Read-state updates do not use
        // this content-mutation path.
        rotate_active_mapi_message_identity_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in rows {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(row.try_get("mailbox_id")?),
                "mailbox_message",
                row.try_get("id")?,
                "updated",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": row.try_get::<Uuid, _>("thread_id")?,
                    "imapUid": row.try_get::<i64, _>("imap_uid")?
                }),
            )
            .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("updated message not found"))
    }

    pub async fn import_jmap_email(
        &self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let target_mailbox = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let target_role = target_mailbox.try_get::<String, _>("role")?;

        let message_id = Uuid::new_v4();
        let thread_id = input.thread_id.unwrap_or_else(Uuid::new_v4);
        let recipients = input
            .to
            .iter()
            .cloned()
            .map(|recipient| ("to", recipient))
            .chain(input.cc.iter().cloned().map(|recipient| ("cc", recipient)))
            .collect::<Vec<_>>();
        let participants = submission::participants_normalized(
            &crate::normalize_email(&input.from_address),
            &recipients,
        );

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let raw_message = input.raw_message.clone().unwrap_or_else(|| {
            format!(
                "From: {}\r\nSubject: {}\r\n\r\n{}",
                crate::normalize_email(&input.from_address),
                input.subject,
                input.body_text
            )
            .into_bytes()
        });
        let blob_id = self
            .store_message_blob_in_tx(
                &mut tx,
                &tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                &raw_message,
            )
            .await?;
        let sent_at = crate::mail::parse_message_date_header(&raw_message);
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id, message_hash,
                normalized_subject, sent_at, received_at, size_octets, has_attachments
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7,
                CASE WHEN $11 THEN NULL ELSE COALESCE($8::timestamptz, $9::timestamptz, NOW()) END,
                COALESCE($9::timestamptz, NOW()),
                $10,
                FALSE
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(input.internet_message_id)
        .bind(sha256_hex(&raw_message))
        .bind(crate::normalize_subject(&input.subject))
        .bind(sent_at.as_deref())
        .bind(input.received_at.as_deref())
        .bind(input.size_octets.max(0))
        .bind(target_role == "drafts")
        .execute(&mut *tx)
        .await?;

        self.replace_message_headers_in_tx(&mut tx, &tenant_id, message_id, &raw_message)
            .await?;

        self.upsert_message_body_in_tx(
            &mut tx,
            &tenant_id,
            domain_id,
            message_id,
            &input.body_text,
            input.body_html_sanitized.as_deref(),
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
            VALUES ($1, $2, $3, 'from', $4, $5, 0)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(message_id)
        .bind(crate::normalize_email(&input.from_address))
        .bind(input.from_display.as_deref())
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in input.to.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'to', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.cc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'cc', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.bcc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (id, tenant_id, message_id, owner_account_id, address, display_name, ordinal, metadata_scope)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(input.account_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;
        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                input.mailbox_id,
                message_id,
                thread_id,
                "",
                true,
                false,
                target_role == "drafts",
                "created",
            )
            .await?;
        self.assign_message_attachments_membership_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            membership_id,
        )
        .await?;
        Self::upsert_mail_search_document_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            membership_id,
            message_id,
            &crate::normalize_subject(&input.subject),
            &participants,
            &input.body_text,
            "",
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(input.account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("imported message not found"))
    }

    pub async fn fetch_latest_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = crate::activesync::activesync_collection_kind(collection_id);
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, state_json::text AS snapshot_json
            FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_kind = $4
              AND collection_key = $5
              AND expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }
}

async fn rekey_mapi_message_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &Uuid,
    account_id: Uuid,
    message_id: Uuid,
    imported_identity: &MapiMessageImportedMoveIdentity,
) -> Result<MapiMessageIdentityMove> {
    let destination_global_counter =
        imported_message_move_destination_global_counter(imported_identity)?;
    let store_identity = ensure_mapi_store_identity_in_tx(tx).await?;
    ensure_mapi_mailbox_replica_in_tx(tx, *tenant_id, account_id, store_identity).await?;
    if imported_identity.destination_source_key.get(..16)
        != Some(store_identity.replica_guid.as_bytes().as_slice())
    {
        bail!("imported message move destination must use the local mailbox replica GUID");
    }
    let destination_reserved = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mapi_local_replica_id_ranges
            WHERE tenant_id = $1
              AND account_id = $2
              AND replica_guid = $3
              AND first_global_counter <= $4
              AND end_global_counter_exclusive > $4
        )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(store_identity.replica_guid)
    .bind(destination_global_counter as i64)
    .fetch_one(&mut **tx)
    .await?;
    if !destination_reserved {
        bail!("imported message move destination SourceKey was not locally reserved");
    }
    let normalized_predecessors = merge_predecessor_change_list(
        &imported_identity.predecessor_change_list,
        &imported_identity.change_key,
    )?;
    if normalized_predecessors != imported_identity.predecessor_change_list {
        bail!("imported message move PCL must canonically contain its ChangeKey");
    }

    let identity = sqlx::query(
        r#"
        SELECT
            mapi_object_id,
            source_key,
            mapi_change_number,
            change_key
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND deleted_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("active MAPI message identity was not found"))?;
    let old_mapi_object_id = u64::try_from(identity.try_get::<i64, _>("mapi_object_id")?)
        .map_err(|_| anyhow::anyhow!("stored MAPI message object id is invalid"))?;
    let old_source_key = identity.try_get::<Vec<u8>, _>("source_key")?;
    let old_change_number = u64::try_from(identity.try_get::<i64, _>("mapi_change_number")?)
        .map_err(|_| anyhow::anyhow!("stored MAPI message change number is invalid"))?;
    let old_change_key = identity.try_get::<Vec<u8>, _>("change_key")?;
    if old_source_key != imported_identity.expected_source_key {
        bail!("active MAPI message SourceKey changed before the imported move");
    }
    let new_mapi_object_id = mapi_store_id(destination_global_counter);
    if old_mapi_object_id == new_mapi_object_id {
        bail!("imported message move destination must differ from the source object");
    }
    let (_, new_change_number) = allocate_mapi_store_global_counter_in_tx(tx).await?;
    if new_change_number > MAPI_MAX_GLOBAL_COUNTER
        || new_change_number >= MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER
    {
        bail!("MAPI dynamic global counter space exhausted");
    }
    let updated = sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_global_counter = $5,
            mapi_object_id = $6,
            source_key = $7,
            change_key = $8,
            instance_key = $7,
            mapi_change_number = $9,
            predecessor_change_list = $10,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND source_key = $4
          AND mapi_object_id = $11
          AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(message_id)
    .bind(&old_source_key)
    .bind(destination_global_counter as i64)
    .bind(new_mapi_object_id as i64)
    .bind(&imported_identity.destination_source_key)
    .bind(&imported_identity.change_key)
    .bind(new_change_number as i64)
    .bind(&imported_identity.predecessor_change_list)
    .bind(old_mapi_object_id as i64)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("active MAPI message identity disappeared during imported move");
    }

    Ok(MapiMessageIdentityMove {
        old_mapi_object_id,
        new_mapi_object_id,
        old_source_key,
        new_source_key: imported_identity.destination_source_key.clone(),
        old_change_number,
        new_change_number,
        old_change_key,
        new_change_key: imported_identity.change_key.clone(),
    })
}

fn imported_message_move_destination_global_counter(
    identity: &MapiMessageImportedMoveIdentity,
) -> Result<u64> {
    if identity.expected_source_key.len() != 22 {
        bail!("imported message move source GID must be exactly 22 bytes");
    }
    if identity.destination_source_key.len() != 22 {
        bail!("imported message move destination GID must be exactly 22 bytes");
    }
    if !(17..=24).contains(&identity.change_key.len()) {
        bail!("imported message move ChangeKey has an invalid length");
    }
    let mut counter_bytes = [0u8; 8];
    counter_bytes[2..].copy_from_slice(&identity.destination_source_key[16..]);
    let global_counter = u64::from_be_bytes(counter_bytes);
    if !(MAPI_FIRST_GLOBAL_COUNTER..MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
        .contains(&global_counter)
    {
        bail!("imported message move destination GLOBCNT is outside the dynamic local range");
    }
    Ok(global_counter)
}
