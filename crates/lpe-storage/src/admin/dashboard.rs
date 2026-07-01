use anyhow::Result;

use crate::{
    AccountRow, AdminDashboard, AliasRecord, AliasRow, AuditEvent, AuditRow, DomainRecord,
    DomainRow, HealthResponse, MailboxRecord, MailboxRow, ProtocolStatus, PstTransferJobRecord,
    PstTransferJobRow, Storage, StorageOverview, PLATFORM_TENANT_ID,
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
                COALESCE((
                    SELECT SUM(logical_messages.size_octets)::BIGINT
                    FROM (
                        SELECT DISTINCT m.id, m.size_octets
                        FROM mailbox_messages mm
                        JOIN messages m
                          ON m.tenant_id = mm.tenant_id
                         AND m.id = mm.message_id
                        WHERE mm.tenant_id = accounts.tenant_id
                          AND mm.account_id = accounts.id
                          AND mm.visibility <> 'expunged'
                    ) logical_messages
                ), 0)::BIGINT AS quota_used_octets,
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
                COALESCE(COUNT(mm.id), 0)::BIGINT AS message_count,
                mb.retention_days
            FROM mailboxes mb
            LEFT JOIN mailbox_messages mm
              ON mm.tenant_id = mb.tenant_id
             AND mm.account_id = mb.account_id
             AND mm.mailbox_id = mb.id
             AND mm.visibility <> 'expunged'
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
                used_mb: (row.quota_used_octets.max(0) / 1_048_576) as u32,
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
                default_sieve_script,
                jmap_push_journal_retention_days
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
            jmap_push_journal_retention_days: row.jmap_push_journal_retention_days.max(1) as u32,
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
            FROM submission_queue
            WHERE tenant_id = $1
              AND status IN ('queued', 'ready', 'handed_off', 'deferred')
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
            overview: crate::OverviewStats {
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
            quarantine_items: Vec::new(),
            storage: StorageOverview {
                primary_store: "PostgreSQL".to_string(),
                search_engine: "PostgreSQL FTS + pg_trgm".to_string(),
                attachment_formats: vec!["PDF".to_string(), "DOCX".to_string(), "ODT".to_string()],
                replication_mode: "single node bootstrap".to_string(),
            },
            audit_log,
        })
    }
}
