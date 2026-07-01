use anyhow::{anyhow, bail, Result};
use lpe_domain::MailboxDisplayName;
use sqlx::Row;
use uuid::Uuid;

use crate::shared::allocate_uid_validity;
use crate::{
    normalize_directory_kind, normalize_domain_name, normalize_email, normalize_gal_visibility,
    AuditEntryInput, NewAccount, NewAlias, NewDomain, NewMailbox, NewPstTransferJob, Storage,
    UpdateAccount, UpdateDomain, PLATFORM_TENANT_ID,
};

impl Storage {
    pub async fn create_account(&self, input: NewAccount, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let account_id = Uuid::new_v4();
        let email = normalize_email(&input.email);
        let display_name = input.display_name.trim();
        let tenant_id = self.tenant_id_for_account_email(&email).await?;

        let insert_result = sqlx::query(
            r#"
            INSERT INTO accounts (
                id, tenant_id, primary_domain_id, primary_email, display_name,
                quota_mb, quota_used_octets, status
            )
            SELECT $1, $2, d.id, $3, $4, $5, 0, 'active'
            FROM domains d
            WHERE d.tenant_id = $2
              AND d.normalized_name = split_part($3, '@', 2)
            ON CONFLICT (tenant_id, normalized_primary_email) DO NOTHING
            "#,
        )
        .bind(account_id)
        .bind(&tenant_id)
        .bind(&email)
        .bind(display_name)
        .bind(input.quota_mb as i32)
        .execute(&mut *tx)
        .await?;

        if insert_result.rows_affected() > 0 {
            sqlx::query(
                r#"
                UPDATE accounts
                SET gal_visibility = $1, directory_kind = $2
                WHERE tenant_id = $3 AND id = $4
                "#,
            )
            .bind(normalize_gal_visibility(&input.gal_visibility)?)
            .bind(normalize_directory_kind(&input.directory_kind)?)
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO account_email_addresses (
                    id, tenant_id, account_id, domain_id, email, address_kind, is_primary
                )
                SELECT $1, tenant_id, id, primary_domain_id, primary_email, 'primary', TRUE
                FROM accounts
                WHERE tenant_id = $2 AND id = $3
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO account_identities (
                    id, tenant_id, account_id, email_address_id, display_name, may_send, is_default
                )
                SELECT $1, tenant_id, account_id, id, $4, TRUE, TRUE
                FROM account_email_addresses
                WHERE tenant_id = $2 AND account_id = $3 AND is_primary = TRUE
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .bind(display_name)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO mailboxes (
                    id, tenant_id, account_id, role, display_name, sort_order, retention_days, uid_validity
                )
                VALUES ($1, $2, $3, 'inbox', 'INBOX', 0, 365, $4)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .bind(allocate_uid_validity())
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_account(&self, input: UpdateAccount, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let display_name = input.display_name.trim();
        let status = input.status.trim().to_lowercase();
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;

        if display_name.is_empty() {
            bail!("account display name is required");
        }
        if !matches!(status.as_str(), "active" | "disabled" | "suspended") {
            bail!("unsupported account status");
        }

        let account_email = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE accounts
            SET display_name = $1,
                quota_mb = $2,
                status = $3,
                gal_visibility = $4,
                directory_kind = $5
            WHERE tenant_id = $6 AND id = $7
            RETURNING primary_email
            "#,
        )
        .bind(display_name)
        .bind(input.quota_mb.max(256) as i32)
        .bind(&status)
        .bind(normalize_gal_visibility(&input.gal_visibility)?)
        .bind(normalize_directory_kind(&input.directory_kind)?)
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(account_email) = account_email else {
            bail!("account not found");
        };

        if let Some(password_hash) = input.password_hash {
            if password_hash.trim().is_empty() {
                bail!("account password hash is required");
            }

            sqlx::query(
                r#"
                INSERT INTO account_credentials (account_email, tenant_id, password_hash, status)
                VALUES ($1, $2, $3, 'active')
                ON CONFLICT (tenant_id, account_email) DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    status = 'active',
                    updated_at = NOW()
                "#,
            )
            .bind(normalize_email(&account_email))
            .bind(&tenant_id)
            .bind(password_hash)
            .execute(&mut *tx)
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_mailbox(&self, input: NewMailbox, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let display_name = MailboxDisplayName::new(&input.display_name)?.into_string();
        let name_available = Self::ensure_mailbox_name_available_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            None,
            &display_name,
            None,
        )
        .await;

        match name_available {
            Ok(()) => {
                let sort_order = sqlx::query_scalar::<_, i32>(
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

                sqlx::query(
                r#"
                INSERT INTO mailboxes (
                    id, tenant_id, account_id, role, display_name, sort_order, retention_days, uid_validity
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(input.role.trim())
            .bind(&display_name)
            .bind(sort_order)
            .bind(input.retention_days as i32)
            .bind(allocate_uid_validity())
            .execute(&mut *tx)
            .await?;

                self.insert_audit(&mut tx, &tenant_id, audit).await?;
            }
            Err(error) if error.to_string() == "mailbox already exists" => {}
            Err(error) => return Err(error),
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_pst_transfer_job(
        &self,
        input: NewPstTransferJob,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let direction = input.direction.trim().to_lowercase();
        let server_path = input.server_path.trim();
        let requested_by = input.requested_by.trim().to_lowercase();
        let mailbox = sqlx::query(
            r#"
            SELECT tenant_id, account_id
            FROM mailboxes
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;
        let tenant_id: Uuid = mailbox.try_get("tenant_id")?;
        let account_id: Uuid = mailbox.try_get("account_id")?;

        let mailbox_exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?;

        if mailbox_exists.is_some()
            && !server_path.is_empty()
            && !requested_by.is_empty()
            && (direction == "import" || direction == "export")
        {
            sqlx::query(
                r#"
                INSERT INTO mailbox_pst_jobs (
                    id, tenant_id, account_id, mailbox_id, direction, server_path, status, requested_by
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'requested', $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .bind(input.mailbox_id)
            .bind(direction)
            .bind(server_path)
            .bind(requested_by)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_domain(&self, input: NewDomain, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = PLATFORM_TENANT_ID;
        let domain_name = normalize_domain_name(&input.name)?;
        let result = sqlx::query(
            r#"
            INSERT INTO domains (
                id, tenant_id, name, status, inbound_enabled, outbound_enabled, default_quota_mb,
                default_sieve_script, jmap_push_journal_retention_days
            )
            VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8)
            ON CONFLICT (tenant_id, normalized_name) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(domain_name)
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_quota_mb as i32)
        .bind(input.default_sieve_script.trim())
        .bind(input.jmap_push_journal_retention_days.max(1) as i32)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_domain(&self, input: UpdateDomain, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = PLATFORM_TENANT_ID;
        let updated = sqlx::query(
            r#"
            UPDATE domains
            SET default_quota_mb = $1,
                inbound_enabled = $2,
                outbound_enabled = $3,
                default_sieve_script = $4,
                jmap_push_journal_retention_days = $5
            WHERE tenant_id = $6 AND id = $7
            "#,
        )
        .bind(input.default_quota_mb.max(256) as i32)
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_sieve_script.trim())
        .bind(input.jmap_push_journal_retention_days.max(1) as i32)
        .bind(tenant_id)
        .bind(input.domain_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            bail!("domain not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_alias(&self, input: NewAlias, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let source = normalize_email(&input.source);
        let target = normalize_email(&input.target);
        let tenant_id = self.tenant_id_for_account_email(&source).await?;
        let result = sqlx::query(
            r#"
            INSERT INTO aliases (id, tenant_id, source, target, kind, status)
            VALUES ($1, $2, $3, $4, $5, 'active')
            ON CONFLICT (tenant_id, normalized_source) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(source)
        .bind(target)
        .bind(input.kind.trim())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
