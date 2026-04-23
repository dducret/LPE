use anyhow::{anyhow, bail, Result};
use lpe_core::sieve::parse_script;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    env_bind_address, env_hostname, normalize_admin_permissions, normalize_directory_kind,
    normalize_email, normalize_gal_visibility, permission_summary, permissions_from_storage,
    validate_sieve_script_content, validate_sieve_script_name, AccountRow, AdminDashboard,
    AliasRecord, AliasRow, AntispamSettings, AuditEntryInput, AuditEvent, AuditRow,
    DashboardUpdate, DomainRecord, DomainRow, EmailTraceResult, EmailTraceRow,
    EmailTraceSearchInput, FilterRule, FilterRuleRow, HealthResponse, LocalAiSettings,
    MailFlowEntry, MailFlowRow, MailboxRecord, MailboxRow, NewAccount, NewAlias, NewDomain,
    NewMailbox, NewPstTransferJob, NewServerAdministrator, OverviewStats, ProtocolStatus,
    PstTransferJobRecord, PstTransferJobRow, QuarantineItem, QuarantineRow, SecuritySettings,
    ServerAdministrator, ServerAdministratorRow, ServerSettings, SieveScriptDocument,
    SieveScriptSummary, Storage, StorageOverview, UpdateAccount, UpdateDomain,
    MAX_SIEVE_SCRIPTS_PER_ACCOUNT, PLATFORM_TENANT_ID,
};

impl Storage {
    pub async fn fetch_admin_dashboard(&self) -> Result<AdminDashboard> {
        let tenant_id = PLATFORM_TENANT_ID;
        let account_rows = sqlx::query_as::<_, AccountRow>(
            r#"
            SELECT
                id,
                primary_email,
                display_name,
                quota_mb,
                used_mb,
                status,
                gal_visibility,
                directory_kind
            FROM accounts
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let mailbox_rows = sqlx::query_as::<_, MailboxRow>(
            r#"
            SELECT
                mb.id,
                mb.account_id,
                mb.display_name,
                mb.role,
                COALESCE(COUNT(m.id), 0)::BIGINT AS message_count,
                mb.retention_days
            FROM mailboxes mb
            LEFT JOIN messages m ON m.mailbox_id = mb.id
            WHERE mb.tenant_id = $1
            GROUP BY mb.id, mb.account_id, mb.display_name, mb.role, mb.retention_days, mb.created_at
            ORDER BY mb.created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let pst_job_rows = sqlx::query_as::<_, PstTransferJobRow>(
            r#"
            SELECT
                id,
                mailbox_id,
                direction,
                server_path,
                status,
                requested_by,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN completed_at IS NULL THEN NULL
                    ELSE to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                processed_messages,
                error_message
            FROM mailbox_pst_jobs
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let mut accounts = Vec::with_capacity(account_rows.len());
        for row in account_rows {
            let mailboxes = mailbox_rows
                .iter()
                .filter(|mailbox| mailbox.account_id == row.id)
                .map(|mailbox| MailboxRecord {
                    id: mailbox.id,
                    display_name: mailbox.display_name.clone(),
                    role: mailbox.role.clone(),
                    message_count: mailbox.message_count as u32,
                    retention_days: mailbox.retention_days as u16,
                    pst_jobs: pst_job_rows
                        .iter()
                        .filter(|job| job.mailbox_id == mailbox.id)
                        .map(|job| PstTransferJobRecord {
                            id: job.id,
                            direction: job.direction.clone(),
                            server_path: job.server_path.clone(),
                            status: job.status.clone(),
                            requested_by: job.requested_by.clone(),
                            created_at: job.created_at.clone(),
                            completed_at: job.completed_at.clone(),
                            processed_messages: job.processed_messages.max(0) as u32,
                            error_message: job.error_message.clone(),
                        })
                        .collect(),
                })
                .collect::<Vec<_>>();

            accounts.push(crate::AccountRecord {
                id: row.id,
                email: row.primary_email,
                display_name: row.display_name,
                quota_mb: row.quota_mb as u32,
                used_mb: row.used_mb as u32,
                status: row.status,
                gal_visibility: row.gal_visibility,
                directory_kind: row.directory_kind,
                mailboxes,
            });
        }

        let domains = sqlx::query_as::<_, DomainRow>(
            r#"
            SELECT
                id,
                name,
                status,
                inbound_enabled,
                outbound_enabled,
                default_quota_mb,
                default_sieve_script
            FROM domains
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| DomainRecord {
            id: row.id,
            name: row.name,
            status: row.status,
            inbound_enabled: row.inbound_enabled,
            outbound_enabled: row.outbound_enabled,
            default_quota_mb: row.default_quota_mb as u32,
            default_sieve_script: row.default_sieve_script,
        })
        .collect::<Vec<_>>();

        let aliases = sqlx::query_as::<_, AliasRow>(
            r#"
            SELECT id, source, target, kind, status
            FROM aliases
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| AliasRecord {
            id: row.id,
            source: row.source,
            target: row.target,
            kind: row.kind,
            status: row.status,
        })
        .collect::<Vec<_>>();

        let server_settings = self.fetch_server_settings().await?;
        let security_settings = self.fetch_security_settings().await?;
        let local_ai_settings = self.fetch_local_ai_settings().await?;
        let antispam_settings = self.fetch_antispam_settings().await?;
        let server_admins = self.fetch_server_administrators().await?;
        let antispam_rules = self.fetch_antispam_rules().await?;
        let quarantine_items = self.fetch_quarantine_items().await?;

        let audit_log = sqlx::query_as::<_, AuditRow>(
            r#"
            SELECT
                id,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                actor,
                action,
                subject
            FROM audit_events
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT 20
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| AuditEvent {
            id: row.id,
            timestamp: row.timestamp,
            actor: row.actor,
            action: row.action,
            subject: row.subject,
        })
        .collect::<Vec<_>>();

        let total_mailboxes = accounts.iter().map(|account| account.mailboxes.len()).sum();
        let pending_queue_items = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM outbound_message_queue
            WHERE tenant_id = $1
              AND status IN ('queued', 'deferred')
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await?
        .max(0) as u32;

        let protocols = vec![
            ProtocolStatus {
                name: "JMAP".to_string(),
                enabled: true,
                bind_address: server_settings.jmap_bind_address.clone(),
                state: "serving".to_string(),
            },
            ProtocolStatus {
                name: "IMAP".to_string(),
                enabled: true,
                bind_address: server_settings.imap_bind_address.clone(),
                state: "compatibility".to_string(),
            },
            ProtocolStatus {
                name: "SMTP ingress".to_string(),
                enabled: true,
                bind_address: server_settings.smtp_bind_address.clone(),
                state: "receiving".to_string(),
            },
            ProtocolStatus {
                name: "SMTP submission".to_string(),
                enabled: true,
                bind_address: "0.0.0.0:587".to_string(),
                state: "ready".to_string(),
            },
            ProtocolStatus {
                name: "Admin API".to_string(),
                enabled: true,
                bind_address: server_settings.admin_bind_address.clone(),
                state: "healthy".to_string(),
            },
        ];

        Ok(AdminDashboard {
            health: HealthResponse {
                service: "lpe-admin-api",
                status: "ok",
            },
            overview: OverviewStats {
                total_accounts: accounts.len(),
                total_mailboxes,
                total_domains: domains.len(),
                total_aliases: aliases.len(),
                pending_queue_items,
                attachment_formats: 3,
                local_ai_enabled: local_ai_settings.enabled,
            },
            protocols,
            accounts,
            domains,
            aliases,
            server_admins,
            server_settings,
            security_settings,
            local_ai_settings,
            antispam_settings,
            antispam_rules,
            quarantine_items,
            storage: StorageOverview {
                primary_store: "PostgreSQL".to_string(),
                search_engine: "PostgreSQL FTS + pg_trgm".to_string(),
                attachment_formats: vec!["PDF".to_string(), "DOCX".to_string(), "ODT".to_string()],
                replication_mode: "single node bootstrap".to_string(),
            },
            audit_log,
        })
    }

    pub async fn create_account(&self, input: NewAccount, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let account_id = Uuid::new_v4();
        let email = input.email.trim().to_lowercase();
        let display_name = input.display_name.trim();
        let tenant_id = self.tenant_id_for_account_email(&email).await?;

        let insert_result = sqlx::query(
            r#"
            INSERT INTO accounts (id, tenant_id, primary_email, display_name, quota_mb, used_mb, status)
            VALUES ($1, $2, $3, $4, $5, 0, 'active')
            ON CONFLICT (tenant_id, primary_email) DO NOTHING
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
                INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
                VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 365)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
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
        let exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.display_name.trim())
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
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
                INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(input.role.trim())
            .bind(input.display_name.trim())
            .bind(sort_order)
            .bind(input.retention_days as i32)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
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
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM mailboxes
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

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
                    id, tenant_id, mailbox_id, direction, server_path, status, requested_by
                )
                VALUES ($1, $2, $3, $4, $5, 'requested', $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
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
        let result = sqlx::query(
            r#"
            INSERT INTO domains (
                id, tenant_id, name, status, inbound_enabled, outbound_enabled, default_quota_mb,
                default_sieve_script
            )
            VALUES ($1, $2, $3, 'active', $4, $5, $6, $7)
            ON CONFLICT (tenant_id, name) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.name.trim().to_lowercase())
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_quota_mb as i32)
        .bind(input.default_sieve_script.trim())
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
                default_sieve_script = $4
            WHERE tenant_id = $5 AND id = $6
            "#,
        )
        .bind(input.default_quota_mb.max(256) as i32)
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_sieve_script.trim())
        .bind(tenant_id)
        .bind(input.domain_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            bail!("domain not found");
        }

        self.insert_audit(&mut tx, tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_alias(&self, input: NewAlias, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self
            .tenant_id_for_account_email(input.source.trim())
            .await?;
        let result = sqlx::query(
            r#"
            INSERT INTO aliases (id, tenant_id, source, target, kind, status)
            VALUES ($1, $2, $3, $4, $5, 'active')
            ON CONFLICT (tenant_id, source) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.source.trim().to_lowercase())
        .bind(input.target.trim().to_lowercase())
        .bind(input.kind.trim())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_server_administrator(
        &self,
        input: NewServerAdministrator,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = match input.domain_id {
            Some(domain_id) => self.tenant_id_for_domain_id(domain_id).await?,
            None => PLATFORM_TENANT_ID.to_string(),
        };
        let normalized_permissions =
            normalize_admin_permissions(&input.role, &input.rights_summary, &input.permissions);
        sqlx::query(
            r#"
            INSERT INTO server_administrators (
                id, tenant_id, domain_id, email, display_name, role, rights_summary, permissions_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.domain_id)
        .bind(input.email.trim().to_lowercase())
        .bind(input.display_name.trim())
        .bind(input.role.trim())
        .bind(permission_summary(&normalized_permissions))
        .bind(serde_json::to_string(&normalized_permissions)?)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn find_server_administrator_by_email(
        &self,
        email: &str,
    ) -> Result<Option<ServerAdministrator>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        let row = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
              AND lower(sa.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let permissions = permissions_from_storage(
                &row.role,
                Some(&row.rights_summary),
                Some(&row.permissions_json),
            );
            ServerAdministrator {
                id: row.id,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                email: row.email,
                display_name: row.display_name,
                role: row.role,
                rights_summary: permission_summary(&permissions),
                permissions,
            }
        }))
    }

    pub async fn append_audit_event(&self, tenant_id: &str, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.insert_audit(&mut tx, tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_sieve_scripts(&self, account_id: Uuid) -> Result<Vec<SieveScriptSummary>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY lower(name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SieveScriptSummary {
                    name: row.try_get("name")?,
                    is_active: row.try_get("is_active")?,
                    size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn get_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let normalized_name = validate_sieve_script_name(name)?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&normalized_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn put_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        content: &str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptDocument> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let content = validate_sieve_script_content(content)?;
        parse_script(&content)?;

        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let existing_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_one(&mut *tx)
        .await?;

        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sieve_scripts
                WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .fetch_one(&mut *tx)
        .await?;

        if !exists && existing_count >= MAX_SIEVE_SCRIPTS_PER_ACCOUNT {
            bail!("too many sieve scripts for account");
        }

        if activate {
            sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = FALSE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r#"
            INSERT INTO sieve_scripts (id, tenant_id, account_id, name, content, is_active)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, account_id, normalized_name) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                is_active = EXCLUDED.is_active OR sieve_scripts.is_active,
                updated_at = NOW()
            RETURNING
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .bind(&content)
        .bind(activate)
        .fetch_one(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptDocument {
            name: row.try_get("name")?,
            content: row.try_get("content")?,
            is_active: row.try_get("is_active")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn delete_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let mut tx = self.pool.begin().await?;

        let active = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT is_active
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;

        if active {
            bail!("cannot delete the active sieve script");
        }

        sqlx::query(
            r#"
            DELETE FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn rename_sieve_script(
        &self,
        account_id: Uuid,
        old_name: &str,
        new_name: &str,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptSummary> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let old_name = validate_sieve_script_name(old_name)?;
        let new_name = validate_sieve_script_name(new_name)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET name = $4, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            RETURNING
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&old_name)
        .bind(&new_name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptSummary {
            name: row.try_get("name")?,
            is_active: row.try_get("is_active")?,
            size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn set_active_sieve_script(
        &self,
        account_id: Uuid,
        name: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<Option<String>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET is_active = FALSE, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        let active_name = if let Some(name) = name {
            let name = validate_sieve_script_name(name)?;
            let updated = sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = TRUE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
                RETURNING name
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("sieve script not found"))?;
            Some(updated.try_get("name")?)
        } else {
            None
        };

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(active_name)
    }

    pub async fn fetch_active_sieve_script(
        &self,
        account_id: Uuid,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn create_filter_rule(
        &self,
        input: crate::NewFilterRule,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO antispam_filter_rules (id, tenant_id, name, scope, action, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(PLATFORM_TENANT_ID)
        .bind(input.name.trim())
        .bind(input.scope.trim())
        .bind(input.action.trim())
        .bind(input.status.trim())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update_settings(
        &self,
        update: DashboardUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO server_settings (
                tenant_id, primary_hostname, admin_bind_address, smtp_bind_address,
                imap_bind_address, jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id) DO UPDATE SET
                primary_hostname = EXCLUDED.primary_hostname,
                admin_bind_address = EXCLUDED.admin_bind_address,
                smtp_bind_address = EXCLUDED.smtp_bind_address,
                imap_bind_address = EXCLUDED.imap_bind_address,
                jmap_bind_address = EXCLUDED.jmap_bind_address,
                default_locale = EXCLUDED.default_locale,
                max_message_size_mb = EXCLUDED.max_message_size_mb,
                tls_mode = EXCLUDED.tls_mode,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.server_settings.primary_hostname)
        .bind(update.server_settings.admin_bind_address)
        .bind(update.server_settings.smtp_bind_address)
        .bind(update.server_settings.imap_bind_address)
        .bind(update.server_settings.jmap_bind_address)
        .bind(update.server_settings.default_locale)
        .bind(update.server_settings.max_message_size_mb as i32)
        .bind(update.server_settings.tls_mode)
        .execute(&mut *tx)
        .await?;
        // keep rest of update logic in lib for now? no, full body continues
        sqlx::query(
            r#"
            INSERT INTO security_settings (
                tenant_id, password_login_enabled, mfa_required_for_admins,
                session_timeout_minutes, audit_retention_days, oidc_login_enabled,
                oidc_provider_label, oidc_auto_link_by_email,
                mailbox_password_login_enabled, mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label, mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id) DO UPDATE SET
                password_login_enabled = EXCLUDED.password_login_enabled,
                mfa_required_for_admins = EXCLUDED.mfa_required_for_admins,
                session_timeout_minutes = EXCLUDED.session_timeout_minutes,
                audit_retention_days = EXCLUDED.audit_retention_days,
                oidc_login_enabled = EXCLUDED.oidc_login_enabled,
                oidc_provider_label = EXCLUDED.oidc_provider_label,
                oidc_auto_link_by_email = EXCLUDED.oidc_auto_link_by_email,
                mailbox_password_login_enabled = EXCLUDED.mailbox_password_login_enabled,
                mailbox_oidc_login_enabled = EXCLUDED.mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label = EXCLUDED.mailbox_oidc_provider_label,
                mailbox_oidc_auto_link_by_email = EXCLUDED.mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled = EXCLUDED.mailbox_app_passwords_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.password_login_enabled)
        .bind(update.security_settings.mfa_required_for_admins)
        .bind(update.security_settings.session_timeout_minutes as i32)
        .bind(update.security_settings.audit_retention_days as i32)
        .bind(update.security_settings.oidc_login_enabled)
        .bind(update.security_settings.oidc_provider_label)
        .bind(update.security_settings.oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_password_login_enabled)
        .bind(update.security_settings.mailbox_oidc_login_enabled)
        .bind(update.security_settings.mailbox_oidc_provider_label)
        .bind(update.security_settings.mailbox_oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_app_passwords_enabled)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO admin_oidc_config (
                tenant_id, issuer_url, authorization_endpoint, token_endpoint, userinfo_endpoint,
                client_id, client_secret, scopes, claim_email, claim_display_name, claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.oidc_issuer_url)
        .bind(update.security_settings.oidc_authorization_endpoint)
        .bind(update.security_settings.oidc_token_endpoint)
        .bind(update.security_settings.oidc_userinfo_endpoint)
        .bind(update.security_settings.oidc_client_id)
        .bind(update.security_settings.oidc_client_secret)
        .bind(update.security_settings.oidc_scopes)
        .bind(update.security_settings.oidc_claim_email)
        .bind(update.security_settings.oidc_claim_display_name)
        .bind(update.security_settings.oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO account_oidc_config (
                tenant_id, issuer_url, authorization_endpoint, token_endpoint, userinfo_endpoint,
                client_id, client_secret, scopes, claim_email, claim_display_name, claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.mailbox_oidc_issuer_url)
        .bind(update.security_settings.mailbox_oidc_authorization_endpoint)
        .bind(update.security_settings.mailbox_oidc_token_endpoint)
        .bind(update.security_settings.mailbox_oidc_userinfo_endpoint)
        .bind(update.security_settings.mailbox_oidc_client_id)
        .bind(update.security_settings.mailbox_oidc_client_secret)
        .bind(update.security_settings.mailbox_oidc_scopes)
        .bind(update.security_settings.mailbox_oidc_claim_email)
        .bind(update.security_settings.mailbox_oidc_claim_display_name)
        .bind(update.security_settings.mailbox_oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO local_ai_settings (
                tenant_id, enabled, provider, model, offline_only, indexing_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                offline_only = EXCLUDED.offline_only,
                indexing_enabled = EXCLUDED.indexing_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.local_ai_settings.enabled)
        .bind(update.local_ai_settings.provider)
        .bind(update.local_ai_settings.model)
        .bind(update.local_ai_settings.offline_only)
        .bind(update.local_ai_settings.indexing_enabled)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO antispam_settings (
                tenant_id, content_filtering_enabled, spam_engine,
                quarantine_enabled, quarantine_retention_days
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id) DO UPDATE SET
                content_filtering_enabled = EXCLUDED.content_filtering_enabled,
                spam_engine = EXCLUDED.spam_engine,
                quarantine_enabled = EXCLUDED.quarantine_enabled,
                quarantine_retention_days = EXCLUDED.quarantine_retention_days,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.antispam_settings.content_filtering_enabled)
        .bind(update.antispam_settings.spam_engine)
        .bind(update.antispam_settings.quarantine_enabled)
        .bind(update.antispam_settings.quarantine_retention_days as i32)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_mail_flow_entries(&self) -> Result<Vec<MailFlowEntry>> {
        let rows = sqlx::query_as::<_, MailFlowRow>(
            r#"
            SELECT
                q.id AS queue_id,
                q.message_id,
                a.primary_email AS account_email,
                m.subject_normalized AS subject,
                m.internet_message_id,
                q.status,
                m.delivery_status,
                TRUE AS was_submitted,
                (mb.role = 'sent' AND m.sent_at IS NOT NULL) AS in_sent_mailbox,
                q.attempts,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS submitted_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                CASE
                    WHEN q.last_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_attempt_at,
                CASE
                    WHEN q.next_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS next_attempt_at,
                q.last_trace_id AS trace_id,
                q.remote_message_ref,
                q.last_error,
                q.retry_after_seconds,
                q.retry_policy,
                q.last_dsn_status,
                q.last_smtp_code,
                q.last_enhanced_status
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            JOIN accounts a ON a.id = m.account_id
            WHERE q.tenant_id = $1
            ORDER BY q.created_at DESC
            LIMIT 100
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_mail_flow_row).collect())
    }

    pub async fn search_email_trace(
        &self,
        input: EmailTraceSearchInput,
    ) -> Result<Vec<EmailTraceResult>> {
        let like_query = format!("%{}%", input.query.trim().to_lowercase());
        let rows = sqlx::query_as::<_, EmailTraceRow>(
            r#"
            SELECT
                m.id AS message_id,
                m.internet_message_id,
                m.subject_normalized AS subject,
                m.from_address AS sender,
                a.primary_email AS account_email,
                mb.display_name AS mailbox,
                m.delivery_status,
                (m.submitted_by_account_id IS NOT NULL OR q.queue_status IS NOT NULL) AS was_submitted,
                (mb.role = 'sent' AND m.sent_at IS NOT NULL) AS in_sent_mailbox,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                q.queue_status,
                q.latest_trace_id,
                q.remote_message_ref,
                q.last_attempt_at,
                q.next_attempt_at,
                q.last_error,
                q.last_dsn_status,
                q.last_smtp_code,
                q.last_enhanced_status,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
            FROM messages m
            JOIN accounts a ON a.id = m.account_id
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN LATERAL (
                SELECT
                    q.status AS queue_status,
                    q.last_trace_id AS latest_trace_id,
                    q.remote_message_ref,
                    CASE
                        WHEN q.last_attempt_at IS NULL THEN NULL
                        ELSE to_char(q.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS last_attempt_at,
                    CASE
                        WHEN q.next_attempt_at IS NULL THEN NULL
                        ELSE to_char(q.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                    END AS next_attempt_at,
                    q.last_error,
                    q.last_dsn_status,
                    q.last_smtp_code,
                    q.last_enhanced_status
                FROM outbound_message_queue q
                WHERE q.tenant_id = m.tenant_id
                  AND q.message_id = m.id
                ORDER BY q.created_at DESC
                LIMIT 1
            ) q ON TRUE
            WHERE m.tenant_id = $1
              AND (
                lower(m.from_address) LIKE $2
                OR lower(m.subject_normalized) LIKE $2
                OR lower(COALESCE(m.internet_message_id, '')) LIKE $2
                OR lower(a.primary_email) LIKE $2
                OR lower(COALESCE(q.latest_trace_id, '')) LIKE $2
                OR lower(COALESCE(q.remote_message_ref, '')) LIKE $2
                OR EXISTS (
                    SELECT 1
                    FROM attachments at
                    WHERE at.tenant_id = m.tenant_id
                      AND at.message_id = m.id
                      AND lower(COALESCE(at.extracted_text, '')) LIKE $2
                )
              )
            ORDER BY m.received_at DESC
            LIMIT 50
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(like_query)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_email_trace_row).collect())
    }

    async fn fetch_server_settings(&self) -> Result<ServerSettings> {
        let row = sqlx::query(
            r#"
            SELECT primary_hostname, admin_bind_address, smtp_bind_address, imap_bind_address,
                   jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            FROM server_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => ServerSettings {
                primary_hostname: row.try_get("primary_hostname")?,
                admin_bind_address: row.try_get("admin_bind_address")?,
                smtp_bind_address: row.try_get("smtp_bind_address")?,
                imap_bind_address: row.try_get("imap_bind_address")?,
                jmap_bind_address: row.try_get("jmap_bind_address")?,
                default_locale: row.try_get("default_locale")?,
                max_message_size_mb: row.try_get::<i32, _>("max_message_size_mb")? as u32,
                tls_mode: row.try_get("tls_mode")?,
            },
            None => ServerSettings {
                primary_hostname: env_hostname("LPE_PUBLIC_HOSTNAME")
                    .or_else(|| env_hostname("LPE_SERVER_NAME"))
                    .unwrap_or_else(|| "localhost".to_string()),
                admin_bind_address: env_bind_address("LPE_BIND_ADDRESS", "127.0.0.1:8080"),
                smtp_bind_address: env_bind_address("LPE_SMTP_BIND_ADDRESS", "0.0.0.0:25"),
                imap_bind_address: env_bind_address("LPE_IMAP_BIND_ADDRESS", "0.0.0.0:143"),
                jmap_bind_address: env_bind_address("LPE_JMAP_BIND_ADDRESS", "0.0.0.0:8081"),
                default_locale: "en".to_string(),
                max_message_size_mb: 64,
                tls_mode: "required".to_string(),
            },
        })
    }

    async fn fetch_security_settings(&self) -> Result<SecuritySettings> {
        let row = sqlx::query(
            r#"
            SELECT
                s.password_login_enabled,
                s.mfa_required_for_admins,
                s.session_timeout_minutes,
                s.audit_retention_days,
                s.oidc_login_enabled,
                s.oidc_provider_label,
                s.oidc_auto_link_by_email,
                s.mailbox_password_login_enabled,
                s.mailbox_oidc_login_enabled,
                s.mailbox_oidc_provider_label,
                s.mailbox_oidc_auto_link_by_email,
                s.mailbox_app_passwords_enabled,
                c.issuer_url,
                c.authorization_endpoint,
                c.token_endpoint,
                c.userinfo_endpoint,
                c.client_id,
                c.client_secret,
                c.scopes,
                c.claim_email,
                c.claim_display_name,
                c.claim_subject,
                mc.issuer_url AS mailbox_issuer_url,
                mc.authorization_endpoint AS mailbox_authorization_endpoint,
                mc.token_endpoint AS mailbox_token_endpoint,
                mc.userinfo_endpoint AS mailbox_userinfo_endpoint,
                mc.client_id AS mailbox_client_id,
                mc.client_secret AS mailbox_client_secret,
                mc.scopes AS mailbox_scopes,
                mc.claim_email AS mailbox_claim_email,
                mc.claim_display_name AS mailbox_claim_display_name,
                mc.claim_subject AS mailbox_claim_subject
            FROM security_settings s
            LEFT JOIN admin_oidc_config c ON c.tenant_id = s.tenant_id
            LEFT JOIN account_oidc_config mc ON mc.tenant_id = s.tenant_id
            WHERE s.tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => SecuritySettings {
                password_login_enabled: row.try_get("password_login_enabled")?,
                mfa_required_for_admins: row.try_get("mfa_required_for_admins")?,
                session_timeout_minutes: row.try_get::<i32, _>("session_timeout_minutes")? as u32,
                audit_retention_days: row.try_get::<i32, _>("audit_retention_days")? as u32,
                oidc_login_enabled: row.try_get("oidc_login_enabled")?,
                oidc_provider_label: row.try_get("oidc_provider_label")?,
                oidc_auto_link_by_email: row.try_get("oidc_auto_link_by_email")?,
                oidc_issuer_url: row
                    .try_get::<Option<String>, _>("issuer_url")?
                    .unwrap_or_default(),
                oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("authorization_endpoint")?
                    .unwrap_or_default(),
                oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("token_endpoint")?
                    .unwrap_or_default(),
                oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("userinfo_endpoint")?
                    .unwrap_or_default(),
                oidc_client_id: row
                    .try_get::<Option<String>, _>("client_id")?
                    .unwrap_or_default(),
                oidc_client_secret: row
                    .try_get::<Option<String>, _>("client_secret")?
                    .unwrap_or_default(),
                oidc_scopes: row
                    .try_get::<Option<String>, _>("scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                oidc_claim_email: row
                    .try_get::<Option<String>, _>("claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                oidc_claim_subject: row
                    .try_get::<Option<String>, _>("claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_password_login_enabled: row.try_get("mailbox_password_login_enabled")?,
                mailbox_oidc_login_enabled: row.try_get("mailbox_oidc_login_enabled")?,
                mailbox_oidc_provider_label: row.try_get("mailbox_oidc_provider_label")?,
                mailbox_oidc_auto_link_by_email: row.try_get("mailbox_oidc_auto_link_by_email")?,
                mailbox_oidc_issuer_url: row
                    .try_get::<Option<String>, _>("mailbox_issuer_url")?
                    .unwrap_or_default(),
                mailbox_oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_authorization_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_token_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_userinfo_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_client_id: row
                    .try_get::<Option<String>, _>("mailbox_client_id")?
                    .unwrap_or_default(),
                mailbox_oidc_client_secret: row
                    .try_get::<Option<String>, _>("mailbox_client_secret")?
                    .unwrap_or_default(),
                mailbox_oidc_scopes: row
                    .try_get::<Option<String>, _>("mailbox_scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                mailbox_oidc_claim_email: row
                    .try_get::<Option<String>, _>("mailbox_claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                mailbox_oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("mailbox_claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                mailbox_oidc_claim_subject: row
                    .try_get::<Option<String>, _>("mailbox_claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_app_passwords_enabled: row.try_get("mailbox_app_passwords_enabled")?,
            },
            None => SecuritySettings {
                password_login_enabled: true,
                mfa_required_for_admins: true,
                session_timeout_minutes: 45,
                audit_retention_days: 365,
                oidc_login_enabled: false,
                oidc_provider_label: "Corporate SSO".to_string(),
                oidc_auto_link_by_email: true,
                oidc_issuer_url: String::new(),
                oidc_authorization_endpoint: String::new(),
                oidc_token_endpoint: String::new(),
                oidc_userinfo_endpoint: String::new(),
                oidc_client_id: String::new(),
                oidc_client_secret: String::new(),
                oidc_scopes: "openid profile email".to_string(),
                oidc_claim_email: "email".to_string(),
                oidc_claim_display_name: "name".to_string(),
                oidc_claim_subject: "sub".to_string(),
                mailbox_password_login_enabled: true,
                mailbox_oidc_login_enabled: false,
                mailbox_oidc_provider_label: "Mailbox SSO".to_string(),
                mailbox_oidc_auto_link_by_email: true,
                mailbox_oidc_issuer_url: String::new(),
                mailbox_oidc_authorization_endpoint: String::new(),
                mailbox_oidc_token_endpoint: String::new(),
                mailbox_oidc_userinfo_endpoint: String::new(),
                mailbox_oidc_client_id: String::new(),
                mailbox_oidc_client_secret: String::new(),
                mailbox_oidc_scopes: "openid profile email".to_string(),
                mailbox_oidc_claim_email: "email".to_string(),
                mailbox_oidc_claim_display_name: "name".to_string(),
                mailbox_oidc_claim_subject: "sub".to_string(),
                mailbox_app_passwords_enabled: true,
            },
        })
    }

    async fn fetch_local_ai_settings(&self) -> Result<LocalAiSettings> {
        let row = sqlx::query(
            r#"
            SELECT enabled, provider, model, offline_only, indexing_enabled
            FROM local_ai_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => LocalAiSettings {
                enabled: row.try_get("enabled")?,
                provider: row.try_get("provider")?,
                model: row.try_get("model")?,
                offline_only: row.try_get("offline_only")?,
                indexing_enabled: row.try_get("indexing_enabled")?,
            },
            None => LocalAiSettings {
                enabled: true,
                provider: "stub-local".to_string(),
                model: "gemma3-local".to_string(),
                offline_only: true,
                indexing_enabled: true,
            },
        })
    }

    async fn fetch_antispam_settings(&self) -> Result<AntispamSettings> {
        let row = sqlx::query(
            r#"
            SELECT content_filtering_enabled, spam_engine, quarantine_enabled, quarantine_retention_days
            FROM antispam_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => AntispamSettings {
                content_filtering_enabled: row.try_get("content_filtering_enabled")?,
                spam_engine: row.try_get("spam_engine")?,
                quarantine_enabled: row.try_get("quarantine_enabled")?,
                quarantine_retention_days: row.try_get::<i32, _>("quarantine_retention_days")?
                    as u32,
            },
            None => AntispamSettings {
                content_filtering_enabled: true,
                spam_engine: "rspamd-ready".to_string(),
                quarantine_enabled: true,
                quarantine_retention_days: 30,
            },
        })
    }

    async fn fetch_server_administrators(&self) -> Result<Vec<ServerAdministrator>> {
        let rows = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
            ORDER BY sa.created_at ASC
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let permissions = permissions_from_storage(
                    &row.role,
                    Some(&row.rights_summary),
                    Some(&row.permissions_json),
                );
                ServerAdministrator {
                    id: row.id,
                    domain_id: row.domain_id,
                    domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                    email: row.email,
                    display_name: row.display_name,
                    role: row.role,
                    rights_summary: permission_summary(&permissions),
                    permissions,
                }
            })
            .collect())
    }

    async fn fetch_antispam_rules(&self) -> Result<Vec<FilterRule>> {
        let rows = sqlx::query_as::<_, FilterRuleRow>(
            r#"
            SELECT id, name, scope, action, status
            FROM antispam_filter_rules
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| FilterRule {
                id: row.id,
                name: row.name,
                scope: row.scope,
                action: row.action,
                status: row.status,
            })
            .collect())
    }

    async fn fetch_quarantine_items(&self) -> Result<Vec<QuarantineItem>> {
        let rows = sqlx::query_as::<_, QuarantineRow>(
            r#"
            SELECT
                id,
                message_ref,
                sender,
                recipient,
                reason,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
            FROM antispam_quarantine
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT 50
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| QuarantineItem {
                id: row.id,
                message_ref: row.message_ref,
                sender: row.sender,
                recipient: row.recipient,
                reason: row.reason,
                status: row.status,
                created_at: row.created_at,
            })
            .collect())
    }
}

fn map_mail_flow_row(row: MailFlowRow) -> MailFlowEntry {
    MailFlowEntry {
        queue_id: row.queue_id,
        message_id: row.message_id,
        account_email: row.account_email,
        subject: row.subject,
        internet_message_id: row.internet_message_id,
        status: row.status,
        delivery_status: row.delivery_status,
        was_submitted: row.was_submitted,
        in_sent_mailbox: row.in_sent_mailbox,
        attempts: row.attempts.max(0) as u32,
        submitted_at: row.submitted_at,
        sent_at: row.sent_at,
        last_attempt_at: row.last_attempt_at,
        next_attempt_at: row.next_attempt_at,
        trace_id: row.trace_id,
        remote_message_ref: row.remote_message_ref,
        last_error: row.last_error,
        retry_after_seconds: row.retry_after_seconds,
        retry_policy: row.retry_policy,
        last_dsn_status: row.last_dsn_status,
        last_smtp_code: row.last_smtp_code,
        last_enhanced_status: row.last_enhanced_status,
    }
}

fn map_email_trace_row(row: EmailTraceRow) -> EmailTraceResult {
    EmailTraceResult {
        message_id: row.message_id,
        internet_message_id: row.internet_message_id,
        subject: row.subject,
        sender: row.sender,
        account_email: row.account_email,
        mailbox: row.mailbox,
        delivery_status: row.delivery_status,
        was_submitted: row.was_submitted,
        in_sent_mailbox: row.in_sent_mailbox,
        sent_at: row.sent_at,
        queue_status: row.queue_status,
        latest_trace_id: row.latest_trace_id,
        remote_message_ref: row.remote_message_ref,
        last_attempt_at: row.last_attempt_at,
        next_attempt_at: row.next_attempt_at,
        last_error: row.last_error,
        last_dsn_status: row.last_dsn_status,
        last_smtp_code: row.last_smtp_code,
        last_enhanced_status: row.last_enhanced_status,
        received_at: row.received_at,
    }
}

#[cfg(test)]
mod tests {
    use super::{map_email_trace_row, map_mail_flow_row};
    use crate::{EmailTraceRow, MailFlowRow};
    use uuid::Uuid;

    #[test]
    fn mail_flow_mapping_keeps_explicit_submission_and_sent_signals() {
        let entry = map_mail_flow_row(MailFlowRow {
            queue_id: Uuid::nil(),
            message_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            subject: "Queued message".to_string(),
            internet_message_id: Some("<msg@example.test>".to_string()),
            status: "deferred".to_string(),
            delivery_status: "deferred".to_string(),
            was_submitted: true,
            in_sent_mailbox: true,
            attempts: -2,
            submitted_at: "2026-04-23T08:00:00Z".to_string(),
            sent_at: Some("2026-04-23T08:00:00Z".to_string()),
            last_attempt_at: Some("2026-04-23T08:05:00Z".to_string()),
            next_attempt_at: Some("2026-04-23T08:10:00Z".to_string()),
            trace_id: Some("trace-1".to_string()),
            remote_message_ref: Some("remote-1".to_string()),
            last_error: Some("temporary failure".to_string()),
            retry_after_seconds: Some(300),
            retry_policy: Some("deferred-backoff".to_string()),
            last_dsn_status: Some("4.4.1".to_string()),
            last_smtp_code: Some(451),
            last_enhanced_status: Some("4.4.1".to_string()),
        });

        assert!(entry.was_submitted);
        assert!(entry.in_sent_mailbox);
        assert_eq!(entry.attempts, 0);
        assert_eq!(entry.trace_id.as_deref(), Some("trace-1"));
    }

    #[test]
    fn email_trace_mapping_surfaces_latest_queue_state() {
        let entry = map_email_trace_row(EmailTraceRow {
            message_id: Uuid::nil(),
            internet_message_id: Some("<msg@example.test>".to_string()),
            subject: "Relay result".to_string(),
            sender: "alice@example.test".to_string(),
            account_email: "alice@example.test".to_string(),
            mailbox: "Sent".to_string(),
            delivery_status: "relayed".to_string(),
            was_submitted: true,
            in_sent_mailbox: true,
            sent_at: Some("2026-04-23T08:00:00Z".to_string()),
            queue_status: Some("relayed".to_string()),
            latest_trace_id: Some("trace-2".to_string()),
            remote_message_ref: Some("remote-2".to_string()),
            last_attempt_at: Some("2026-04-23T08:01:00Z".to_string()),
            next_attempt_at: None,
            last_error: None,
            last_dsn_status: None,
            last_smtp_code: Some(250),
            last_enhanced_status: Some("2.0.0".to_string()),
            received_at: "2026-04-23T08:00:00Z".to_string(),
        });

        assert_eq!(entry.queue_status.as_deref(), Some("relayed"));
        assert_eq!(entry.latest_trace_id.as_deref(), Some("trace-2"));
        assert!(entry.in_sent_mailbox);
    }
}
