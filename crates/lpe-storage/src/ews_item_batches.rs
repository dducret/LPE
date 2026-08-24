use anyhow::{bail, Result};
use serde_json::json;
use sqlx::Row;
use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    mapi_message_identity::rekey_active_mapi_message_identity_for_server_move_in_tx,
    public_folders::map_public_folder_item, AuditEntryInput, CanonicalChangeCategory,
    PublicFolderItem, PublicFolderItemRow, Storage,
};

#[derive(Clone, Copy)]
struct PublicFolderBatchAccess {
    tree_admin_owner_account_id: Uuid,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
}

impl Storage {
    /// Copies up to 100 public-folder items in one transaction. Every source
    /// and the target are locked and authorized before the first clone write.
    pub async fn copy_ews_public_folder_items(
        &self,
        account_id: Uuid,
        item_ids: &[Uuid],
        target_folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<Vec<PublicFolderItem>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        validate_public_folder_batch_ids(item_ids)?;
        let mut tx = self.pool.begin().await?;
        let target_access =
            public_folder_batch_access_in_tx(&mut tx, tenant_id, account_id, target_folder_id)
                .await?;
        if !target_access.may_write {
            bail!("public folder write access is not granted");
        }
        let sources =
            load_public_folder_batch_sources(&mut tx, tenant_id, account_id, item_ids, false)
                .await?;
        let mut copied = Vec::with_capacity(sources.len());
        for source in sources {
            let item_id = Uuid::new_v4();
            let row = insert_public_folder_item_clone(
                &mut tx,
                tenant_id,
                account_id,
                target_folder_id,
                item_id,
                &source,
            )
            .await?;
            record_public_folder_batch_change(
                self,
                &mut tx,
                tenant_id,
                target_access.tree_admin_owner_account_id,
                account_id,
                target_folder_id,
                "public_folder_item",
                item_id,
                "created",
                json!({"folderId": target_folder_id}),
            )
            .await?;
            self.insert_audit(&mut tx, &tenant_id, audit.clone())
                .await?;
            copied.push(map_public_folder_item(row));
        }
        tx.commit().await?;
        Ok(copied)
    }

    /// Moves up to 100 public-folder items in one transaction. Target clones,
    /// source destruction, public-folder replay rows, tombstones, and audits
    /// share one commit boundary.
    pub async fn move_ews_public_folder_items(
        &self,
        account_id: Uuid,
        item_ids: &[Uuid],
        target_folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<Vec<PublicFolderItem>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        validate_public_folder_batch_ids(item_ids)?;
        let mut tx = self.pool.begin().await?;
        let target_access =
            public_folder_batch_access_in_tx(&mut tx, tenant_id, account_id, target_folder_id)
                .await?;
        if !target_access.may_write {
            bail!("public folder write access is not granted");
        }
        let sources =
            load_public_folder_batch_sources(&mut tx, tenant_id, account_id, item_ids, true)
                .await?;
        let mut moved = Vec::with_capacity(sources.len());
        for source in sources {
            let source_access = public_folder_batch_access_in_tx(
                &mut tx,
                tenant_id,
                account_id,
                source.public_folder_id,
            )
            .await?;
            let item_id = Uuid::new_v4();
            let row = insert_public_folder_item_clone(
                &mut tx,
                tenant_id,
                account_id,
                target_folder_id,
                item_id,
                &source,
            )
            .await?;
            record_public_folder_batch_change(
                self,
                &mut tx,
                tenant_id,
                target_access.tree_admin_owner_account_id,
                account_id,
                target_folder_id,
                "public_folder_item",
                item_id,
                "created",
                json!({"folderId": target_folder_id}),
            )
            .await?;
            let deleted_modseq = sqlx::query_scalar::<_, i64>(
                r#"
                UPDATE public_folder_items
                SET lifecycle_state = 'deleted',
                    change_counter = change_counter + 1,
                    updated_by_account_id = $4,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND public_folder_id = $2
                  AND id = $3
                  AND lifecycle_state = 'active'
                RETURNING change_counter
                "#,
            )
            .bind(tenant_id)
            .bind(source.public_folder_id)
            .bind(source.id)
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("public folder item not found"))?;
            let cursor = record_public_folder_batch_change(
                self,
                &mut tx,
                tenant_id,
                source_access.tree_admin_owner_account_id,
                account_id,
                source.public_folder_id,
                "public_folder_item",
                source.id,
                "destroyed",
                json!({"folderId": source.public_folder_id}),
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO tombstones (
                    id, tenant_id, account_id, collection_id, object_kind, object_id,
                    deleted_modseq, change_cursor, reason
                )
                VALUES ($1, $2, $3, $4, 'public_folder_item', $5, $6, $7, 'move')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(source_access.tree_admin_owner_account_id)
            .bind(source.public_folder_id)
            .bind(source.id)
            .bind(deleted_modseq)
            .bind(cursor)
            .execute(&mut *tx)
            .await?;
            self.insert_audit(&mut tx, &tenant_id, audit.clone())
                .await?;
            moved.push(map_public_folder_item(row));
        }
        tx.commit().await?;
        Ok(moved)
    }

    /// Copies a bounded EWS item batch in one transaction so an invalid later
    /// source cannot leave earlier target memberships committed.
    pub async fn copy_jmap_emails(
        &self,
        account_id: Uuid,
        message_ids: &[Uuid],
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;

        let mut sources = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            let source = sqlx::query(
                r#"
                SELECT id, thread_id, is_seen, is_flagged, received_at::text AS received_at
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND message_id = $3
                  AND visibility = 'visible'
                ORDER BY updated_at DESC
                LIMIT 1
                FOR UPDATE
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(message_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;
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
            if target_contains_message {
                bail!("message already exists in target mailbox");
            }
            sources.push((
                *message_id,
                source.try_get::<Uuid, _>("id")?,
                source.try_get::<Uuid, _>("thread_id")?,
                source.try_get::<bool, _>("is_seen")?,
                source.try_get::<bool, _>("is_flagged")?,
                source.try_get::<String, _>("received_at")?,
            ));
        }

        for (message_id, source_membership_id, thread_id, is_seen, is_flagged, received_at) in
            sources
        {
            let membership_id = self
                .allocate_mailbox_membership_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    target_mailbox_id,
                    message_id,
                    thread_id,
                    &received_at,
                    is_seen,
                    is_flagged,
                    target_role == "drafts",
                    "created",
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
            .bind(account_id)
            .bind(source_membership_id)
            .bind(message_id)
            .bind(membership_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
            self.insert_audit(&mut tx, &tenant_id, audit.clone())
                .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Moves a bounded EWS item batch in one transaction. The mailbox
    /// membership, search projection, MAPI identity, change rows, tombstones,
    /// and audit records therefore share one commit boundary.
    pub async fn move_jmap_emails(
        &self,
        account_id: Uuid,
        message_ids: &[Uuid],
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let mut sources = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            let source = sqlx::query(
                r#"
                SELECT id, mailbox_id, thread_id, imap_uid, is_seen, is_flagged, keywords,
                       calendar_request_processed,
                       received_at::text AS received_at
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND message_id = $3
                  AND visibility = 'visible'
                ORDER BY updated_at DESC
                LIMIT 1
                FOR UPDATE
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(message_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;
            let source_mailbox_id: Uuid = source.try_get("mailbox_id")?;
            if source_mailbox_id == target_mailbox_id {
                bail!("message already exists in target mailbox");
            }
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
            if target_contains_message {
                bail!("message already exists in target mailbox");
            }
            sources.push((
                *message_id,
                source.try_get::<Uuid, _>("id")?,
                source_mailbox_id,
                source.try_get::<Uuid, _>("thread_id")?,
                source.try_get::<i64, _>("imap_uid")?,
                source.try_get::<bool, _>("is_seen")?,
                source.try_get::<bool, _>("is_flagged")?,
                source.try_get::<Vec<String>, _>("keywords")?,
                source.try_get::<bool, _>("calendar_request_processed")?,
                source.try_get::<String, _>("received_at")?,
            ));
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for (
            message_id,
            source_membership_id,
            source_mailbox_id,
            thread_id,
            source_imap_uid,
            is_seen,
            is_flagged,
            keywords,
            calendar_request_processed,
            received_at,
        ) in sources
        {
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
            .bind(is_seen)
            .bind(modseq)
            .fetch_one(&mut *tx)
            .await?;
            let target_membership_id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO mailbox_messages (
                    id, tenant_id, account_id, mailbox_id, message_id, thread_id,
                    imap_uid, modseq, is_seen, is_flagged, is_draft, keywords,
                    calendar_request_processed, received_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, COALESCE($14::timestamptz, NOW()))
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
            .bind(is_seen)
            .bind(is_flagged)
            .bind(target_role == "drafts")
            .bind(keywords)
            .bind(calendar_request_processed)
            .bind(received_at)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                UPDATE mailbox_messages
                SET visibility = 'expunged', expunged_at = NOW(), modseq = $4, updated_at = NOW()
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
                    modseq = GREATEST(modseq + 1, $5), updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_mailbox_id)
            .bind(is_seen)
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
                SELECT tenant_id, account_id, $4, message_id,
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
                "DELETE FROM mail_search_documents WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3",
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_membership_id)
            .execute(&mut *tx)
            .await?;

            let identity = rekey_active_mapi_message_identity_for_server_move_in_tx(
                &mut tx, &tenant_id, account_id, message_id,
            )
            .await?;
            let (old_mapi_object_id, new_mapi_object_id, move_identity_snapshot_complete) =
                if let Some(identity) = identity {
                    (
                        Some(identity.old_mapi_object_id),
                        Some(identity.new_mapi_object_id),
                        true,
                    )
                } else {
                    (None, None, false)
                };
            self.insert_audit(&mut tx, &tenant_id, audit.clone())
                .await?;
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
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Empties the supplied custom mailbox folders as one EWS request. The
    /// complete membership set is locked and bounded before its first change,
    /// so a later deletion failure rolls the whole operation back.
    pub async fn empty_ews_mailbox_folders(
        &self,
        account_id: Uuid,
        folder_ids: &[Uuid],
        delete_subfolders: bool,
        audit: AuditEntryInput,
    ) -> Result<()> {
        validate_empty_folder_ids(folder_ids)?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let folders = sqlx::query(
            r#"
            SELECT id, role, parent_mailbox_id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = ANY($3)
            FOR UPDATE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(folder_ids)
        .fetch_all(&mut *tx)
        .await?;
        if folders.len() != folder_ids.len() {
            bail!("mailbox folder not found");
        }
        if folders
            .iter()
            .any(|folder| folder.get::<String, _>("role") != "custom")
        {
            bail!("system mailbox cannot be emptied through EWS");
        }
        if !delete_subfolders && folder_ids.len() != 1 {
            bail!("EmptyFolder without DeleteSubFolders accepts one folder");
        }

        let rows = sqlx::query(
            r#"
            SELECT mm.id, mm.mailbox_id, mm.message_id, mm.thread_id, mm.imap_uid, mm.is_seen,
                   COALESCE(mb.recoverable_items_retention_days, a.recoverable_items_retention_days) AS recoverable_retention_days,
                   (m.legal_hold OR a.litigation_hold_enabled) AS recoverable_legal_hold
            FROM mailbox_messages mm
            JOIN messages m ON m.tenant_id = mm.tenant_id AND m.id = mm.message_id
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id AND mb.account_id = mm.account_id AND mb.id = mm.mailbox_id
            JOIN accounts a ON a.tenant_id = mm.tenant_id AND a.id = mm.account_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.mailbox_id = ANY($3)
              AND mm.visibility = 'visible'
            ORDER BY mm.mailbox_id, mm.imap_uid
            FOR UPDATE OF mm
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(folder_ids)
        .fetch_all(&mut *tx)
        .await?;
        validate_empty_folder_count(rows.len())?;
        let has_changes = !rows.is_empty() || (delete_subfolders && folder_ids.len() > 1);
        if !has_changes {
            tx.commit().await?;
            return Ok(());
        }

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        let recoverable_created_by_protocol = recoverable_protocol(&audit.action);
        for row in &rows {
            let membership_id: Uuid = row.try_get("id")?;
            let mailbox_id: Uuid = row.try_get("mailbox_id")?;
            let message_id: Uuid = row.try_get("message_id")?;
            let thread_id: Option<Uuid> = row.try_get("thread_id")?;
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
                json!({"messageId": message_id, "threadId": thread_id, "imapUid": imap_uid}),
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO tombstones (
                    id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                    message_id, mailbox_message_id, imap_uid, deleted_modseq, change_cursor, reason
                ) VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, 'delete')
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
            sqlx::query(
                r#"
                INSERT INTO recoverable_items (
                    id, tenant_id, account_id, message_id, source_mailbox_message_id,
                    source_mailbox_id, source_imap_uid, source_thread_id,
                    recoverable_folder, delete_kind, retained_until, legal_hold, created_by_protocol
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    'deletions', 'hard_delete',
                    CASE WHEN $9::integer = 0 THEN NOW() ELSE NOW() + ($9::integer * INTERVAL '1 day') END,
                    $10, $11
                ) ON CONFLICT (tenant_id, account_id, source_mailbox_message_id) DO NOTHING
                "#,
            )
            .bind(recoverable_item_id).bind(&tenant_id).bind(account_id).bind(message_id)
            .bind(membership_id).bind(mailbox_id).bind(imap_uid).bind(thread_id)
            .bind(row.try_get::<i32, _>("recoverable_retention_days")?)
            .bind(row.try_get::<bool, _>("recoverable_legal_hold")?)
            .bind(recoverable_created_by_protocol)
            .execute(&mut *tx).await?;
            Self::insert_mail_change_log_in_tx(
                &mut tx, &tenant_id, Some(account_id), None, "recoverable_item", recoverable_item_id,
                "created", modseq, &principals,
                json!({"messageId": message_id, "sourceMailboxMessageId": membership_id,
                    "recoverableFolder": "deletions", "sourceMailboxId": mailbox_id, "sourceImapUid": imap_uid}),
            ).await?;
        }
        let membership_ids = rows
            .iter()
            .map(|row| row.try_get::<Uuid, _>("id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if !membership_ids.is_empty() {
            sqlx::query(
                "UPDATE mailbox_messages SET visibility = 'expunged', expunged_at = NOW(), modseq = $4, updated_at = NOW() WHERE tenant_id = $1 AND account_id = $2 AND id = ANY($3)",
            ).bind(&tenant_id).bind(account_id).bind(&membership_ids).bind(modseq).execute(&mut *tx).await?;
            sqlx::query(
                "DELETE FROM mail_search_documents WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = ANY($3)",
            ).bind(&tenant_id).bind(account_id).bind(&membership_ids).execute(&mut *tx).await?;
            for folder_id in folder_ids {
                Self::recalculate_mailbox_counts_in_tx(
                    &mut tx, &tenant_id, account_id, *folder_id, modseq,
                )
                .await?;
            }
        }
        if delete_subfolders {
            for folder in folders
                .iter()
                .filter(|folder| folder.get::<Uuid, _>("id") != folder_ids[0])
            {
                let folder_id: Uuid = folder.get("id");
                let parent_id: Option<Uuid> = folder.get("parent_mailbox_id");
                let cursor = Self::insert_mail_change_log_in_tx(
                    &mut tx,
                    &tenant_id,
                    Some(account_id),
                    Some(folder_id),
                    "mailbox",
                    folder_id,
                    "destroyed",
                    modseq,
                    &principals,
                    json!({"reason": "delete", "parentId": parent_id}),
                )
                .await?;
                sqlx::query(
                    "INSERT INTO tombstones (id, tenant_id, account_id, mailbox_id, object_kind, object_id, deleted_modseq, change_cursor, reason) VALUES ($1, $2, $3, $4, 'mailbox', $4, $5, $6, 'delete')",
                ).bind(Uuid::new_v4()).bind(&tenant_id).bind(account_id).bind(folder_id).bind(modseq).bind(cursor).execute(&mut *tx).await?;
            }
            let removed_folder_ids = &folder_ids[1..];
            if !removed_folder_ids.is_empty() {
                sqlx::query("DELETE FROM mailboxes WHERE tenant_id = $1 AND account_id = $2 AND id = ANY($3)")
                    .bind(&tenant_id).bind(account_id).bind(removed_folder_ids).execute(&mut *tx).await?;
            }
        }
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Empties public folders atomically. Folder and item deletes use the
    /// same replay and tombstone sequence as their single-item counterparts.
    pub async fn empty_ews_public_folders(
        &self,
        account_id: Uuid,
        folder_ids: &[Uuid],
        delete_subfolders: bool,
        audit: AuditEntryInput,
    ) -> Result<()> {
        validate_empty_folder_ids(folder_ids)?;
        if !delete_subfolders && folder_ids.len() != 1 {
            bail!("EmptyFolder without DeleteSubFolders accepts one folder");
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let mut accesses = Vec::with_capacity(folder_ids.len());
        for folder_id in folder_ids {
            let access =
                public_folder_batch_access_in_tx(&mut tx, tenant_id, account_id, *folder_id)
                    .await?;
            if !access.may_delete {
                bail!("public folder delete access is not granted");
            }
            if delete_subfolders && access.tree_admin_owner_account_id != account_id {
                bail!("public folder tree administration is required to delete subfolders");
            }
            accesses.push(access);
        }
        let rows = sqlx::query(
            r#"
            SELECT id, public_folder_id
            FROM public_folder_items
            WHERE tenant_id = $1 AND public_folder_id = ANY($2) AND lifecycle_state = 'active'
            ORDER BY public_folder_id, id
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(folder_ids)
        .fetch_all(&mut *tx)
        .await?;
        validate_empty_folder_count(rows.len())?;
        let has_changes = !rows.is_empty() || (delete_subfolders && folder_ids.len() > 1);
        if !has_changes {
            tx.commit().await?;
            return Ok(());
        }
        for row in &rows {
            let item_id: Uuid = row.get("id");
            let folder_id: Uuid = row.get("public_folder_id");
            let access = accesses[folder_ids
                .iter()
                .position(|id| *id == folder_id)
                .expect("preflighted folder")];
            let deleted_modseq = sqlx::query_scalar::<_, i64>(
                "UPDATE public_folder_items SET lifecycle_state = 'deleted', change_counter = change_counter + 1, updated_by_account_id = $4, updated_at = NOW() WHERE tenant_id = $1 AND public_folder_id = $2 AND id = $3 AND lifecycle_state = 'active' RETURNING change_counter",
            ).bind(tenant_id).bind(folder_id).bind(item_id).bind(account_id).fetch_one(&mut *tx).await?;
            let cursor = record_public_folder_batch_change(
                self,
                &mut tx,
                tenant_id,
                access.tree_admin_owner_account_id,
                account_id,
                folder_id,
                "public_folder_item",
                item_id,
                "destroyed",
                json!({"folderId": folder_id}),
            )
            .await?;
            sqlx::query(
                "INSERT INTO tombstones (id, tenant_id, account_id, collection_id, object_kind, object_id, deleted_modseq, change_cursor, reason) VALUES ($1, $2, $3, $4, 'public_folder_item', $5, $6, $7, 'delete')",
            ).bind(Uuid::new_v4()).bind(tenant_id).bind(access.tree_admin_owner_account_id).bind(folder_id).bind(item_id).bind(deleted_modseq).bind(cursor).execute(&mut *tx).await?;
        }
        if delete_subfolders {
            for (index, folder_id) in folder_ids.iter().enumerate().skip(1) {
                let parent_id = sqlx::query_scalar::<_, Option<Uuid>>(
                    "SELECT parent_folder_id FROM public_folders WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
                ).bind(tenant_id).bind(folder_id).fetch_one(&mut *tx).await?;
                let deleted_modseq = sqlx::query_scalar::<_, i64>(
                    "UPDATE public_folders SET lifecycle_state = 'deleted', change_counter = change_counter + 1, updated_at = NOW() WHERE tenant_id = $1 AND id = $2 AND lifecycle_state <> 'deleted' RETURNING change_counter",
                ).bind(tenant_id).bind(folder_id).fetch_one(&mut *tx).await?;
                let access = accesses[index];
                let cursor = record_public_folder_batch_change(
                    self,
                    &mut tx,
                    tenant_id,
                    access.tree_admin_owner_account_id,
                    account_id,
                    *folder_id,
                    "public_folder",
                    *folder_id,
                    "destroyed",
                    json!({"folderId": folder_id}),
                )
                .await?;
                sqlx::query(
                    "INSERT INTO tombstones (id, tenant_id, account_id, collection_id, object_kind, object_id, deleted_modseq, change_cursor, reason) VALUES ($1, $2, $3, $4, 'public_folder', $5, $6, $7, 'delete')",
                ).bind(Uuid::new_v4()).bind(tenant_id).bind(access.tree_admin_owner_account_id).bind(parent_id).bind(folder_id).bind(deleted_modseq).bind(cursor).execute(&mut *tx).await?;
            }
        }
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }
}

fn validate_empty_folder_ids(folder_ids: &[Uuid]) -> Result<()> {
    if folder_ids.is_empty() {
        bail!("EmptyFolder requires at least one folder");
    }
    if folder_ids.iter().collect::<HashSet<_>>().len() != folder_ids.len() {
        bail!("EmptyFolder folder list contains duplicates");
    }
    Ok(())
}

fn validate_empty_folder_count(count: usize) -> Result<()> {
    if count > 10_000 {
        bail!("EmptyFolder supports at most 10,000 items per request");
    }
    Ok(())
}

fn recoverable_protocol(audit_action: &str) -> &'static str {
    match audit_action {
        action if action.starts_with("mapi-") => "mapi",
        action if action.starts_with("ews-") => "ews",
        action if action.starts_with("imap-") => "imap",
        action if action.starts_with("jmap-") => "jmap",
        _ => "api",
    }
}

fn validate_public_folder_batch_ids(item_ids: &[Uuid]) -> Result<()> {
    if item_ids.is_empty() || item_ids.len() > 100 {
        bail!("public folder item batch must contain between one and 100 items");
    }
    if item_ids.iter().collect::<HashSet<_>>().len() != item_ids.len() {
        bail!("public folder item batch contains duplicate items");
    }
    Ok(())
}

async fn public_folder_batch_access_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    folder_id: Uuid,
) -> Result<PublicFolderBatchAccess> {
    let row = sqlx::query_as::<_, (Uuid, bool, bool, bool)>(
        r#"
        SELECT
            t.admin_owner_account_id,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_read, FALSE) END,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_write, FALSE) END,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_delete, FALSE) END
        FROM public_folders f
        JOIN public_folder_trees t
          ON t.tenant_id = f.tenant_id
         AND t.id = f.tree_id
        LEFT JOIN public_folder_permissions p
          ON p.tenant_id = f.tenant_id
         AND p.public_folder_id = f.id
         AND p.principal_account_id = $2
        WHERE f.tenant_id = $1
          AND f.id = $3
          AND f.lifecycle_state <> 'deleted'
          AND t.lifecycle_state = 'active'
        FOR SHARE OF f, t
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(folder_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow::anyhow!("public folder not found"))?;
    Ok(PublicFolderBatchAccess {
        tree_admin_owner_account_id: row.0,
        may_read: row.1,
        may_write: row.2,
        may_delete: row.3,
    })
}

async fn load_public_folder_batch_sources(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    item_ids: &[Uuid],
    require_delete: bool,
) -> Result<Vec<PublicFolderItemRow>> {
    let mut sources = Vec::with_capacity(item_ids.len());
    for item_id in item_ids {
        let source = sqlx::query_as::<_, PublicFolderItemRow>(
            r#"
            SELECT
                id,
                public_folder_id,
                message_id,
                item_kind,
                message_class,
                subject,
                body_text,
                body_html_sanitized,
                source_payload_json::text AS source_payload_json,
                lifecycle_state,
                change_counter,
                created_by_account_id,
                updated_by_account_id,
                FALSE AS is_read,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_items
            WHERE tenant_id = $1
              AND id = $2
              AND lifecycle_state = 'active'
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(item_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder item not found"))?;
        let access =
            public_folder_batch_access_in_tx(tx, tenant_id, account_id, source.public_folder_id)
                .await?;
        if !access.may_read {
            bail!("public folder read access is not granted");
        }
        if require_delete && !access.may_delete {
            bail!("public folder delete access is not granted");
        }
        sources.push(source);
    }
    Ok(sources)
}

async fn insert_public_folder_item_clone(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    target_folder_id: Uuid,
    item_id: Uuid,
    source: &PublicFolderItemRow,
) -> Result<PublicFolderItemRow> {
    sqlx::query_as::<_, PublicFolderItemRow>(
        r#"
        INSERT INTO public_folder_items (
            id, tenant_id, public_folder_id, item_kind, message_class, subject,
            body_text, body_html_sanitized, source_payload_json,
            created_by_account_id, updated_by_account_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $10)
        RETURNING
            id,
            public_folder_id,
            message_id,
            item_kind,
            message_class,
            subject,
            body_text,
            body_html_sanitized,
            source_payload_json::text AS source_payload_json,
            lifecycle_state,
            change_counter,
            created_by_account_id,
            updated_by_account_id,
            FALSE AS is_read,
            to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
            to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
        "#,
    )
    .bind(item_id)
    .bind(tenant_id)
    .bind(target_folder_id)
    .bind(&source.item_kind)
    .bind(&source.message_class)
    .bind(&source.subject)
    .bind(&source.body_text)
    .bind(&source.body_html_sanitized)
    .bind(&source.source_payload_json)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn record_public_folder_batch_change(
    storage: &Storage,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    owner_account_id: Uuid,
    actor_account_id: Uuid,
    folder_id: Uuid,
    object_kind: &str,
    object_id: Uuid,
    change_kind: &str,
    summary_json: serde_json::Value,
) -> Result<i64> {
    let mut affected = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT principal_account_id
        FROM public_folder_permissions
        WHERE tenant_id = $1 AND public_folder_id = $2 AND may_read
        "#,
    )
    .bind(tenant_id)
    .bind(folder_id)
    .fetch_all(&mut **tx)
    .await?;
    affected.push(owner_account_id);
    affected.push(actor_account_id);
    affected.sort();
    affected.dedup();
    let modseq = storage
        .allocate_account_modseq_in_tx(
            tx,
            &tenant_id,
            owner_account_id,
            CanonicalChangeCategory::PublicFolders.as_str(),
        )
        .await?;
    let cursor = Storage::insert_mail_change_log_in_tx(
        tx,
        &tenant_id,
        Some(owner_account_id),
        None,
        object_kind,
        object_id,
        change_kind,
        modseq,
        &affected,
        summary_json,
    )
    .await?;
    Storage::emit_canonical_change(
        tx,
        &tenant_id,
        CanonicalChangeCategory::PublicFolders,
        &affected,
        &affected,
    )
    .await?;
    Ok(cursor)
}
