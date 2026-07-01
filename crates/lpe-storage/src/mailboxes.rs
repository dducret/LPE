use anyhow::{anyhow, bail, Result};
use lpe_domain::{MailboxDisplayName, MailboxNamePolicy, MailboxPath};
use serde::Serialize;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    imap::ImapMailboxState,
    shared::allocate_uid_validity,
    util::{canonical_system_mailbox_display_name, system_mailbox_role_for_display_name},
    AuditEntryInput, JmapMailboxRow, Storage,
};

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
    pub size_octets: u64,
    pub is_subscribed: bool,
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
pub struct ManagedRetentionFolderCreateInput {
    pub account_id: Uuid,
    pub folder_name: String,
    pub is_subscribed: bool,
}

impl Storage {
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
                COALESCE(folder_sizes.size_octets, 0)::bigint AS size_octets,
                COALESCE(ms.is_subscribed, TRUE) AS is_subscribed
            FROM mailboxes mb
            LEFT JOIN mailbox_subscriptions ms
              ON ms.tenant_id = mb.tenant_id
             AND ms.mailbox_account_id = mb.account_id
             AND ms.mailbox_id = mb.id
             AND ms.subscriber_account_id = $2
            LEFT JOIN LATERAL (
                SELECT SUM(m.size_octets)::bigint AS size_octets
                FROM mailbox_messages mm
                JOIN messages m
                  ON m.tenant_id = mm.tenant_id
                 AND m.id = mm.message_id
                WHERE mm.tenant_id = mb.tenant_id
                  AND mm.account_id = mb.account_id
                  AND mm.mailbox_id = mb.id
                  AND mm.visibility <> 'expunged'
            ) folder_sizes ON TRUE
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
                    size_octets: row.size_octets.max(0) as u64,
                    is_subscribed: row.is_subscribed,
                })
            })
            .collect()
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

    pub async fn create_managed_retention_folder(
        &self,
        input: ManagedRetentionFolderCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let name = MailboxDisplayName::new(&input.folder_name)?.into_string();
        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;

        let tag = sqlx::query(
            r#"
            WITH assignment AS (
                SELECT default_tag_id
                FROM account_retention_policy_assignments
                WHERE tenant_id = $1
                  AND account_id = $2
            )
            SELECT tag.id, tag.display_name, tag.retention_days
            FROM retention_policy_tags tag
            LEFT JOIN assignment ON TRUE
            WHERE tag.tenant_id = $1
              AND tag.lifecycle_state = 'active'
              AND tag.tag_type IN ('custom_folder', 'personal')
              AND lower(tag.display_name) = lower($3)
              AND (tag.is_visible OR tag.id = assignment.default_tag_id)
            ORDER BY
                CASE WHEN tag.id = assignment.default_tag_id THEN 0 ELSE 1 END,
                tag.id
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("managed retention folder tag not found"))?;

        let tag_id = tag.try_get::<Uuid, _>("id")?;
        let tag_display_name = tag.try_get::<String, _>("display_name")?;
        let retention_days = tag
            .try_get::<Option<i32>, _>("retention_days")?
            .unwrap_or(365);
        let display_name = MailboxDisplayName::new(&tag_display_name)?.into_string();
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;

        let existing = sqlx::query(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1
              AND account_id = $2
              AND parent_mailbox_id IS NULL
              AND role = 'custom'
              AND normalized_display_name = lower($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(&display_name)
        .fetch_optional(&mut *tx)
        .await?;

        let (mailbox_id, change_kind) = if let Some(row) = existing {
            let mailbox_id = row.try_get::<Uuid, _>("id")?;
            sqlx::query(
                r#"
                UPDATE mailboxes
                SET retention_policy_tag_id = $4,
                    retention_days = $5,
                    modseq = $6,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND id = $3
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(mailbox_id)
            .bind(tag_id)
            .bind(retention_days)
            .bind(modseq as i64)
            .execute(&mut *tx)
            .await?;
            (mailbox_id, "updated")
        } else {
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
            sqlx::query(
                r#"
                INSERT INTO mailboxes (
                    id, tenant_id, account_id, role, display_name, sort_order,
                    retention_days, retention_policy_tag_id, uid_validity, modseq
                )
                VALUES ($1, $2, $3, 'custom', $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(mailbox_id)
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(&display_name)
            .bind(next_sort_order)
            .bind(retention_days)
            .bind(tag_id)
            .bind(allocate_uid_validity())
            .bind(modseq as i64)
            .execute(&mut *tx)
            .await?;
            (mailbox_id, "created")
        };

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
            change_kind,
            modseq,
            &principals,
            serde_json::json!({
                "name": display_name,
                "retentionPolicyTagId": tag_id,
            }),
        )
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow!("managed retention folder creation failed"))
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
}

fn is_system_mailbox_role(role: &str) -> bool {
    let role = role.trim();
    !role.is_empty() && !role.eq_ignore_ascii_case("custom")
}

#[cfg(test)]
mod tests {
    use super::is_system_mailbox_role;

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
}
