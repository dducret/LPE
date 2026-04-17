use anyhow::{bail, Result};
use serde::Serialize;
use sqlx::{FromRow, Pool, Postgres, Row};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use uuid::Uuid;

const DEFAULT_TENANT_ID: &str = "default";

#[derive(Clone)]
pub struct Storage {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminDashboard {
    pub health: HealthResponse,
    pub overview: OverviewStats,
    pub protocols: Vec<ProtocolStatus>,
    pub accounts: Vec<AccountRecord>,
    pub domains: Vec<DomainRecord>,
    pub aliases: Vec<AliasRecord>,
    pub server_admins: Vec<ServerAdministrator>,
    pub server_settings: ServerSettings,
    pub security_settings: SecuritySettings,
    pub local_ai_settings: LocalAiSettings,
    pub antispam_settings: AntispamSettings,
    pub antispam_rules: Vec<FilterRule>,
    pub quarantine_items: Vec<QuarantineItem>,
    pub storage: StorageOverview,
    pub audit_log: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub service: &'static str,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverviewStats {
    pub total_accounts: usize,
    pub total_mailboxes: usize,
    pub total_domains: usize,
    pub total_aliases: usize,
    pub pending_queue_items: u32,
    pub attachment_formats: usize,
    pub local_ai_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolStatus {
    pub name: String,
    pub enabled: bool,
    pub bind_address: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MailboxRecord {
    pub id: Uuid,
    pub display_name: String,
    pub role: String,
    pub message_count: u32,
    pub retention_days: u16,
    pub pst_jobs: Vec<PstTransferJobRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PstTransferJobRecord {
    pub id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub status: String,
    pub requested_by: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub processed_messages: u32,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRecord {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub quota_mb: u32,
    pub used_mb: u32,
    pub status: String,
    pub mailboxes: Vec<MailboxRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DomainRecord {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    pub default_quota_mb: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct AliasRecord {
    pub id: Uuid,
    pub source: String,
    pub target: String,
    pub kind: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerAdministrator {
    pub id: Uuid,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub rights_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAdmin {
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerSettings {
    pub primary_hostname: String,
    pub admin_bind_address: String,
    pub smtp_bind_address: String,
    pub imap_bind_address: String,
    pub jmap_bind_address: String,
    pub default_locale: String,
    pub max_message_size_mb: u32,
    pub tls_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecuritySettings {
    pub password_login_enabled: bool,
    pub mfa_required_for_admins: bool,
    pub session_timeout_minutes: u32,
    pub audit_retention_days: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalAiSettings {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub offline_only: bool,
    pub indexing_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AntispamSettings {
    pub content_filtering_enabled: bool,
    pub spam_engine: String,
    pub quarantine_enabled: bool,
    pub quarantine_retention_days: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilterRule {
    pub id: Uuid,
    pub name: String,
    pub scope: String,
    pub action: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuarantineItem {
    pub id: Uuid,
    pub message_ref: String,
    pub sender: String,
    pub recipient: String,
    pub reason: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageOverview {
    pub primary_store: String,
    pub search_engine: String,
    pub attachment_formats: Vec<String>,
    pub replication_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub subject: String,
}

#[derive(Debug, Clone)]
pub struct NewAccount {
    pub email: String,
    pub display_name: String,
    pub quota_mb: u32,
}

#[derive(Debug, Clone)]
pub struct NewMailbox {
    pub account_id: Uuid,
    pub display_name: String,
    pub role: String,
    pub retention_days: u16,
}

#[derive(Debug, Clone)]
pub struct NewPstTransferJob {
    pub mailbox_id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub requested_by: String,
}

#[derive(Debug, Clone)]
pub struct NewDomain {
    pub name: String,
    pub default_quota_mb: u32,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewAlias {
    pub source: String,
    pub target: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct AuditEntryInput {
    pub actor: String,
    pub action: String,
    pub subject: String,
}

#[derive(Debug, Clone)]
pub struct DashboardUpdate {
    pub server_settings: ServerSettings,
    pub security_settings: SecuritySettings,
    pub local_ai_settings: LocalAiSettings,
    pub antispam_settings: AntispamSettings,
}

#[derive(Debug, Clone)]
pub struct NewServerAdministrator {
    pub domain_id: Option<Uuid>,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub rights_summary: String,
}

#[derive(Debug, Clone)]
pub struct AdminCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AccountCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AdminLogin {
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
}

#[derive(Debug, Clone)]
pub struct AccountLogin {
    pub account_id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAccount {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PstJobExecutionSummary {
    pub processed_jobs: u32,
    pub completed_jobs: u32,
    pub failed_jobs: u32,
}

#[derive(Debug, Clone)]
pub struct NewFilterRule {
    pub name: String,
    pub scope: String,
    pub action: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct EmailTraceSearchInput {
    pub query: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmailTraceResult {
    pub message_id: Uuid,
    pub internet_message_id: Option<String>,
    pub subject: String,
    pub sender: String,
    pub account_email: String,
    pub mailbox: String,
    pub received_at: String,
}

#[derive(Debug, Clone)]
pub struct SubmitMessageInput {
    pub account_id: Uuid,
    pub source: String,
    pub from_display: Option<String>,
    pub from_address: String,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub size_octets: i64,
}

#[derive(Debug, Clone)]
pub struct SubmittedRecipientInput {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmittedMessage {
    pub message_id: Uuid,
    pub account_id: Uuid,
    pub sent_mailbox_id: Uuid,
    pub outbound_queue_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, FromRow)]
struct AccountRow {
    id: Uuid,
    primary_email: String,
    display_name: String,
    quota_mb: i32,
    used_mb: i32,
    status: String,
}

#[derive(Debug, FromRow)]
struct MailboxRow {
    id: Uuid,
    account_id: Uuid,
    display_name: String,
    role: String,
    message_count: i64,
    retention_days: i32,
}

#[derive(Debug, FromRow)]
struct PstTransferJobRow {
    id: Uuid,
    mailbox_id: Uuid,
    direction: String,
    server_path: String,
    status: String,
    requested_by: String,
    created_at: String,
    completed_at: Option<String>,
    processed_messages: i32,
    error_message: Option<String>,
}

#[derive(Debug, FromRow)]
struct DomainRow {
    id: Uuid,
    name: String,
    status: String,
    inbound_enabled: bool,
    outbound_enabled: bool,
    default_quota_mb: i32,
}

#[derive(Debug, FromRow)]
struct AliasRow {
    id: Uuid,
    source: String,
    target: String,
    kind: String,
    status: String,
}

#[derive(Debug, FromRow)]
struct AuditRow {
    id: Uuid,
    timestamp: String,
    actor: String,
    action: String,
    subject: String,
}

#[derive(Debug, FromRow)]
struct ServerAdministratorRow {
    id: Uuid,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    email: String,
    display_name: String,
    role: String,
    rights_summary: String,
}

#[derive(Debug, FromRow)]
struct AdminLoginRow {
    email: String,
    password_hash: String,
    status: String,
    display_name: Option<String>,
    role: Option<String>,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    rights_summary: Option<String>,
}

#[derive(Debug, FromRow)]
struct AccountLoginRow {
    account_id: Uuid,
    email: String,
    password_hash: String,
    status: String,
    display_name: String,
}

#[derive(Debug, FromRow)]
struct AuthenticatedAdminRow {
    email: String,
    display_name: Option<String>,
    role: Option<String>,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    rights_summary: Option<String>,
    expires_at: String,
}

#[derive(Debug, FromRow)]
struct AuthenticatedAccountRow {
    account_id: Uuid,
    email: String,
    display_name: String,
    expires_at: String,
}

#[derive(Debug, FromRow)]
struct PendingPstJobRow {
    id: Uuid,
    mailbox_id: Uuid,
    account_id: Uuid,
    direction: String,
    server_path: String,
    requested_by: String,
}

#[derive(Debug, FromRow)]
struct FilterRuleRow {
    id: Uuid,
    name: String,
    scope: String,
    action: String,
    status: String,
}

#[derive(Debug, FromRow)]
struct QuarantineRow {
    id: Uuid,
    message_ref: String,
    sender: String,
    recipient: String,
    reason: String,
    status: String,
    created_at: String,
}

#[derive(Debug, FromRow)]
struct EmailTraceRow {
    message_id: Uuid,
    internet_message_id: Option<String>,
    subject: String,
    sender: String,
    account_email: String,
    mailbox: String,
    received_at: String,
}

impl Storage {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = Pool::<Postgres>::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    pub async fn fetch_admin_dashboard(&self) -> Result<AdminDashboard> {
        let tenant_id = DEFAULT_TENANT_ID;
        let account_rows = sqlx::query_as::<_, AccountRow>(
            r#"
            SELECT id, primary_email, display_name, quota_mb, used_mb, status
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

            accounts.push(AccountRecord {
                id: row.id,
                email: row.primary_email,
                display_name: row.display_name,
                quota_mb: row.quota_mb as u32,
                used_mb: row.used_mb as u32,
                status: row.status,
                mailboxes,
            });
        }

        let domains = sqlx::query_as::<_, DomainRow>(
            r#"
            SELECT id, name, status, inbound_enabled, outbound_enabled, default_quota_mb
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
        let pending_queue_items = accounts
            .iter()
            .map(|account| {
                account
                    .mailboxes
                    .iter()
                    .map(|mailbox| mailbox.message_count)
                    .sum::<u32>()
            })
            .sum();

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

        let insert_result = sqlx::query(
            r#"
            INSERT INTO accounts (id, tenant_id, primary_email, display_name, quota_mb, used_mb, status)
            VALUES ($1, $2, $3, $4, $5, 0, 'active')
            ON CONFLICT (tenant_id, primary_email) DO NOTHING
            "#,
        )
        .bind(account_id)
        .bind(DEFAULT_TENANT_ID)
        .bind(&email)
        .bind(display_name)
        .bind(input.quota_mb as i32)
        .execute(&mut *tx)
        .await?;

        if insert_result.rows_affected() > 0 {
            sqlx::query(
                r#"
                INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
                VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 365)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(DEFAULT_TENANT_ID)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_mailbox(&self, input: NewMailbox, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
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
            .bind(DEFAULT_TENANT_ID)
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
            .bind(DEFAULT_TENANT_ID)
            .bind(input.account_id)
            .bind(input.role.trim())
            .bind(input.display_name.trim())
            .bind(sort_order)
            .bind(input.retention_days as i32)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, audit).await?;
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

        let mailbox_exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
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
            .bind(DEFAULT_TENANT_ID)
            .bind(input.mailbox_id)
            .bind(direction)
            .bind(server_path)
            .bind(requested_by)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_domain(&self, input: NewDomain, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            INSERT INTO domains (id, tenant_id, name, status, inbound_enabled, outbound_enabled, default_quota_mb)
            VALUES ($1, $2, $3, 'active', $4, $5, $6)
            ON CONFLICT (tenant_id, name) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(input.name.trim().to_lowercase())
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_quota_mb as i32)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_alias(&self, input: NewAlias, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            INSERT INTO aliases (id, tenant_id, source, target, kind, status)
            VALUES ($1, $2, $3, $4, $5, 'active')
            ON CONFLICT (tenant_id, source) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(input.source.trim().to_lowercase())
        .bind(input.target.trim().to_lowercase())
        .bind(input.kind.trim())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, audit).await?;
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
        sqlx::query(
            r#"
            INSERT INTO server_administrators (
                id, tenant_id, domain_id, email, display_name, role, rights_summary
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(input.domain_id)
        .bind(input.email.trim().to_lowercase())
        .bind(input.display_name.trim())
        .bind(input.role.trim())
        .bind(input.rights_summary.trim())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_admin_credential(
        &self,
        input: AdminCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("admin credential email and password hash are required");
        }

        sqlx::query(
            r#"
            INSERT INTO admin_credentials (email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(DEFAULT_TENANT_ID)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_account_credential(
        &self,
        input: AccountCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("account credential email and password hash are required");
        }

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        sqlx::query(
            r#"
            INSERT INTO account_credentials (account_email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (account_email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(DEFAULT_TENANT_ID)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_admin_login(&self, email: &str) -> Result<Option<AdminLogin>> {
        let email = normalize_email(email);
        let row = sqlx::query_as::<_, AdminLoginRow>(
            r#"
            SELECT
                ac.email,
                ac.password_hash,
                ac.status,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary
            FROM admin_credentials ac
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = ac.tenant_id AND lower(sa.email) = lower(ac.email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE ac.tenant_id = $1 AND lower(ac.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AdminLogin {
            email: row.email,
            password_hash: row.password_hash,
            status: row.status,
            display_name: row
                .display_name
                .unwrap_or_else(|| "LPE Administrator".to_string()),
            role: row.role.unwrap_or_else(|| "server-admin".to_string()),
            domain_id: row.domain_id,
            domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
            rights_summary: row.rights_summary.unwrap_or_else(|| {
                "server, domains, accounts, aliases, policies, antispam, pst, audit".to_string()
            }),
        }))
    }

    pub async fn create_admin_session(
        &self,
        token: &str,
        email: &str,
        session_timeout_minutes: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO admin_sessions (id, tenant_id, token, admin_email, expires_at)
            VALUES ($1, $2, $3, $4, NOW() + ($5::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .bind(normalize_email(email))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_login(&self, email: &str) -> Result<Option<AccountLogin>> {
        let email = normalize_email(email);
        let row = sqlx::query_as::<_, AccountLoginRow>(
            r#"
            SELECT
                a.id AS account_id,
                ac.account_email AS email,
                ac.password_hash,
                ac.status,
                a.display_name
            FROM account_credentials ac
            JOIN accounts a
              ON a.tenant_id = ac.tenant_id
             AND lower(a.primary_email) = lower(ac.account_email)
            WHERE ac.tenant_id = $1 AND lower(ac.account_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AccountLogin {
            account_id: row.account_id,
            email: row.email,
            password_hash: row.password_hash,
            status: row.status,
            display_name: row.display_name,
        }))
    }

    pub async fn create_account_session(
        &self,
        token: &str,
        account_email: &str,
        session_timeout_minutes: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO account_sessions (id, tenant_id, token, account_email, expires_at)
            VALUES ($1, $2, $3, $4, NOW() + ($5::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .bind(normalize_email(account_email))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_admin_session(&self, token: &str) -> Result<Option<AuthenticatedAdmin>> {
        let row = sqlx::query_as::<_, AuthenticatedAdminRow>(
            r#"
            SELECT
                ac.email,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM admin_sessions s
            JOIN admin_credentials ac ON ac.email = s.admin_email
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = s.tenant_id AND lower(sa.email) = lower(s.admin_email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AuthenticatedAdmin {
            email: row.email,
            display_name: row
                .display_name
                .unwrap_or_else(|| "LPE Administrator".to_string()),
            role: row.role.unwrap_or_else(|| "server-admin".to_string()),
            domain_id: row.domain_id,
            domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
            rights_summary: row.rights_summary.unwrap_or_else(|| {
                "server, domains, accounts, aliases, policies, antispam, pst, audit".to_string()
            }),
            expires_at: row.expires_at,
        }))
    }

    pub async fn delete_admin_session(&self, token: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM admin_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
        let row = sqlx::query_as::<_, AuthenticatedAccountRow>(
            r#"
            SELECT
                a.id AS account_id,
                ac.account_email AS email,
                a.display_name,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM account_sessions s
            JOIN account_credentials ac ON ac.account_email = s.account_email
            JOIN accounts a
              ON a.tenant_id = s.tenant_id
             AND lower(a.primary_email) = lower(s.account_email)
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AuthenticatedAccount {
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            expires_at: row.expires_at,
        }))
    }

    pub async fn delete_account_session(&self, token: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM account_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_filter_rule(
        &self,
        input: NewFilterRule,
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
        .bind(DEFAULT_TENANT_ID)
        .bind(input.name.trim())
        .bind(input.scope.trim())
        .bind(input.action.trim())
        .bind(input.status.trim())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
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
        .bind(DEFAULT_TENANT_ID)
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

        sqlx::query(
            r#"
            INSERT INTO security_settings (
                tenant_id, password_login_enabled, mfa_required_for_admins,
                session_timeout_minutes, audit_retention_days
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id) DO UPDATE SET
                password_login_enabled = EXCLUDED.password_login_enabled,
                mfa_required_for_admins = EXCLUDED.mfa_required_for_admins,
                session_timeout_minutes = EXCLUDED.session_timeout_minutes,
                audit_retention_days = EXCLUDED.audit_retention_days,
                updated_at = NOW()
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(update.security_settings.password_login_enabled)
        .bind(update.security_settings.mfa_required_for_admins)
        .bind(update.security_settings.session_timeout_minutes as i32)
        .bind(update.security_settings.audit_retention_days as i32)
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
        .bind(DEFAULT_TENANT_ID)
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
        .bind(DEFAULT_TENANT_ID)
        .bind(update.antispam_settings.content_filtering_enabled)
        .bind(update.antispam_settings.spam_engine)
        .bind(update.antispam_settings.quarantine_enabled)
        .bind(update.antispam_settings.quarantine_retention_days as i32)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let from_address = normalize_email(&input.from_address);
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let recipients = normalize_submitted_recipients(&input);

        if from_address.is_empty() {
            bail!("from_address is required");
        }
        if recipients.is_empty() {
            bail!("at least one recipient is required");
        }
        if subject.is_empty() && body_text.is_empty() {
            bail!("subject or body_text is required");
        }

        let mut tx = self.pool.begin().await?;

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        let sent_mailbox_id = self
            .ensure_mailbox(&mut tx, input.account_id, "sent", "Sent", 20, 365)
            .await?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let outbound_queue_id = Uuid::new_v4();
        let preview_text = preview_text(&body_text);
        let participants_normalized = participants_normalized(&from_address, &recipients);
        let mime_blob_ref = input
            .mime_blob_ref
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("canonical-message:{message_id}"));
        let content_hash = format!("message:{message_id}");

        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                NOW(), NOW(), $7, $8, $9,
                $10, FALSE, FALSE, FALSE, $11, $12,
                $13, 'queued'
            )
            "#,
        )
        .bind(message_id)
        .bind(DEFAULT_TENANT_ID)
        .bind(input.account_id)
        .bind(sent_mailbox_id)
        .bind(thread_id)
        .bind(input.internet_message_id)
        .bind(input.from_display.map(|value| value.trim().to_string()))
        .bind(&from_address)
        .bind(&subject)
        .bind(&preview_text)
        .bind(input.size_octets.max(0))
        .bind(&mime_blob_ref)
        .bind(input.source.trim().to_lowercase())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            "#,
        )
        .bind(message_id)
        .bind(&body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants_normalized)
        .bind(content_hash)
        .bind(format!("{subject} {body_text} {participants_normalized}"))
        .execute(&mut *tx)
        .await?;

        for (ordinal, (kind, recipient)) in recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(DEFAULT_TENANT_ID)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"
            INSERT INTO outbound_message_queue (
                id, tenant_id, message_id, account_id, transport, status
            )
            VALUES ($1, $2, $3, $4, 'lpe-ct-smtp', 'queued')
            "#,
        )
        .bind(outbound_queue_id)
        .bind(DEFAULT_TENANT_ID)
        .bind(message_id)
        .bind(input.account_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;

        Ok(SubmittedMessage {
            message_id,
            account_id: input.account_id,
            sent_mailbox_id,
            outbound_queue_id,
            delivery_status: "queued".to_string(),
        })
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
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
            FROM messages m
            JOIN accounts a ON a.id = m.account_id
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            WHERE m.tenant_id = $1
              AND (
                lower(m.from_address) LIKE $2
                OR lower(m.subject_normalized) LIKE $2
                OR lower(COALESCE(m.internet_message_id, '')) LIKE $2
                OR lower(a.primary_email) LIKE $2
              )
            ORDER BY m.received_at DESC
            LIMIT 50
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(like_query)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| EmailTraceResult {
                message_id: row.message_id,
                internet_message_id: row.internet_message_id,
                subject: row.subject,
                sender: row.sender,
                account_email: row.account_email,
                mailbox: row.mailbox,
                received_at: row.received_at,
            })
            .collect())
    }

    pub async fn process_pending_pst_jobs(&self) -> Result<PstJobExecutionSummary> {
        let jobs = sqlx::query_as::<_, PendingPstJobRow>(
            r#"
            SELECT
                j.id,
                j.mailbox_id,
                mb.account_id,
                j.direction,
                j.server_path,
                j.requested_by
            FROM mailbox_pst_jobs j
            JOIN mailboxes mb ON mb.id = j.mailbox_id
            WHERE j.tenant_id = $1 AND j.status IN ('requested', 'failed')
            ORDER BY j.created_at ASC
            LIMIT 10
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        let mut summary = PstJobExecutionSummary {
            processed_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
        };

        for job in jobs {
            summary.processed_jobs += 1;
            if let Err(error) = self.mark_pst_job_running(job.id).await {
                summary.failed_jobs += 1;
                let _ = self
                    .mark_pst_job_failed(job.id, &format!("cannot start job: {error}"))
                    .await;
                continue;
            }

            let result = if job.direction == "export" {
                self.export_mailbox_to_pst(&job).await
            } else {
                self.import_mailbox_from_pst(&job).await
            };

            match result {
                Ok(processed_messages) => {
                    self.mark_pst_job_completed(job.id, processed_messages)
                        .await?;
                    summary.completed_jobs += 1;
                }
                Err(error) => {
                    self.mark_pst_job_failed(job.id, &error.to_string()).await?;
                    summary.failed_jobs += 1;
                }
            }
        }

        Ok(summary)
    }

    async fn mark_pst_job_running(&self, job_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'running', error_message = NULL
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_completed(&self, job_id: Uuid, processed_messages: u32) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'completed',
                processed_messages = $3,
                error_message = NULL,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(job_id)
        .bind(processed_messages as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_failed(&self, job_id: Uuid, error_message: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'failed',
                error_message = $3,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(job_id)
        .bind(error_message.chars().take(1000).collect::<String>())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn export_mailbox_to_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        ensure_parent_directory(&job.server_path)?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.internet_message_id,
                m.from_address,
                m.subject_normalized,
                COALESCE(mb.body_text, '') AS body_text
            FROM messages m
            LEFT JOIN message_bodies mb ON mb.message_id = m.id
            WHERE m.tenant_id = $1 AND m.mailbox_id = $2
            ORDER BY m.received_at ASC
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(job.mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let mut file = File::create(&job.server_path)?;
        writeln!(file, "LPE-PST-V1")?;
        writeln!(file, "mailbox_id={}", job.mailbox_id)?;
        writeln!(file, "requested_by={}", job.requested_by)?;

        for row in &rows {
            let internet_message_id = row
                .try_get::<Option<String>, _>("internet_message_id")?
                .unwrap_or_default();
            let from_address: String = row.try_get("from_address")?;
            let subject: String = row.try_get("subject_normalized")?;
            let body_text: String = row.try_get("body_text")?;
            writeln!(
                file,
                "MESSAGE\t{}\t{}\t{}\t{}",
                encode_pst_field(&internet_message_id),
                encode_pst_field(&from_address),
                encode_pst_field(&subject),
                encode_pst_field(&body_text)
            )?;
        }

        Ok(rows.len() as u32)
    }

    async fn import_mailbox_from_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        let file = File::open(&job.server_path)?;
        let mut reader = BufReader::new(file);
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header.trim() != "LPE-PST-V1" {
            bail!("unsupported PST file for this bootstrap engine");
        }

        let mut processed_messages = 0;
        let mut tx = self.pool.begin().await?;
        for line in reader.lines() {
            let line = line?;
            if !line.starts_with("MESSAGE\t") {
                continue;
            }
            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 5 {
                continue;
            }

            let message_id = Uuid::new_v4();
            let body_text = decode_pst_field(parts[4]);
            let subject = decode_pst_field(parts[3]);
            let from_address = decode_pst_field(parts[2]);
            let internet_message_id = decode_pst_field(parts[1]);
            let preview_text = preview_text(&body_text);

            sqlx::query(
                r#"
                INSERT INTO messages (
                    id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                    received_at, sent_at, from_display, from_address, subject_normalized,
                    preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                    submission_source, delivery_status
                )
                VALUES (
                    $1, $2, $3, $4, $5, NULLIF($6, ''),
                    NOW(), NULL, NULL, $7, $8,
                    $9, TRUE, FALSE, FALSE, $10, $11,
                    'pst-import', 'stored'
                )
                "#,
            )
            .bind(message_id)
            .bind(DEFAULT_TENANT_ID)
            .bind(job.account_id)
            .bind(job.mailbox_id)
            .bind(Uuid::new_v4())
            .bind(internet_message_id)
            .bind(from_address)
            .bind(subject.clone())
            .bind(preview_text)
            .bind(body_text.len() as i64)
            .bind(format!("pst-import:{message_id}"))
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO message_bodies (
                    message_id, body_text, body_html_sanitized, participants_normalized,
                    language_code, content_hash, search_vector
                )
                VALUES ($1, $2, NULL, '', NULL, $3, to_tsvector('simple', $4))
                "#,
            )
            .bind(message_id)
            .bind(body_text.clone())
            .bind(format!("pst-import:{message_id}"))
            .bind(format!("{subject} {body_text}"))
            .execute(&mut *tx)
            .await?;

            processed_messages += 1;
        }

        tx.commit().await?;
        Ok(processed_messages)
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
        .bind(DEFAULT_TENANT_ID)
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
                primary_hostname: "mail.example.test".to_string(),
                admin_bind_address: "127.0.0.1:8080".to_string(),
                smtp_bind_address: "0.0.0.0:25".to_string(),
                imap_bind_address: "0.0.0.0:143".to_string(),
                jmap_bind_address: "0.0.0.0:8081".to_string(),
                default_locale: "en".to_string(),
                max_message_size_mb: 64,
                tls_mode: "required".to_string(),
            },
        })
    }

    async fn fetch_security_settings(&self) -> Result<SecuritySettings> {
        let row = sqlx::query(
            r#"
            SELECT password_login_enabled, mfa_required_for_admins, session_timeout_minutes, audit_retention_days
            FROM security_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => SecuritySettings {
                password_login_enabled: row.try_get("password_login_enabled")?,
                mfa_required_for_admins: row.try_get("mfa_required_for_admins")?,
                session_timeout_minutes: row.try_get::<i32, _>("session_timeout_minutes")? as u32,
                audit_retention_days: row.try_get::<i32, _>("audit_retention_days")? as u32,
            },
            None => SecuritySettings {
                password_login_enabled: true,
                mfa_required_for_admins: true,
                session_timeout_minutes: 45,
                audit_retention_days: 365,
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
        .bind(DEFAULT_TENANT_ID)
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
        .bind(DEFAULT_TENANT_ID)
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
                sa.rights_summary
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
            ORDER BY sa.created_at ASC
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ServerAdministrator {
                id: row.id,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                email: row.email,
                display_name: row.display_name,
                role: row.role,
                rights_summary: row.rights_summary,
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
        .bind(DEFAULT_TENANT_ID)
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
        .bind(DEFAULT_TENANT_ID)
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

    async fn ensure_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        account_id: Uuid,
        role: &str,
        display_name: &str,
        sort_order: i32,
        retention_days: i32,
    ) -> Result<Uuid> {
        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND role = $3
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(DEFAULT_TENANT_ID)
        .bind(account_id)
        .bind(role)
        .fetch_optional(&mut **tx)
        .await?
        {
            return row.try_get("id").map_err(Into::into);
        }

        let mailbox_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, role, display_name, sort_order, retention_days
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(mailbox_id)
        .bind(DEFAULT_TENANT_ID)
        .bind(account_id)
        .bind(role)
        .bind(display_name)
        .bind(sort_order)
        .bind(retention_days)
        .execute(&mut **tx)
        .await?;

        Ok(mailbox_id)
    }

    async fn insert_audit(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        audit: AuditEntryInput,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO audit_events (id, tenant_id, actor, action, subject)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(DEFAULT_TENANT_ID)
        .bind(audit.actor)
        .bind(audit.action)
        .bind(audit.subject)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn normalize_subject(value: &str) -> String {
    value.trim().to_string()
}

fn preview_text(body_text: &str) -> String {
    let preview = body_text
        .split_whitespace()
        .take(28)
        .collect::<Vec<_>>()
        .join(" ");

    if preview.is_empty() {
        "(no preview)".to_string()
    } else {
        preview
    }
}

fn normalize_submitted_recipients(
    input: &SubmitMessageInput,
) -> Vec<(&'static str, SubmittedRecipientInput)> {
    let mut recipients = Vec::new();
    push_recipients(&mut recipients, "to", &input.to);
    push_recipients(&mut recipients, "cc", &input.cc);
    push_recipients(&mut recipients, "bcc", &input.bcc);
    recipients
}

fn push_recipients(
    output: &mut Vec<(&'static str, SubmittedRecipientInput)>,
    kind: &'static str,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push((
            kind,
            SubmittedRecipientInput {
                address,
                display_name: recipient
                    .display_name
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
        ));
    }
}

fn participants_normalized(
    from_address: &str,
    recipients: &[(&'static str, SubmittedRecipientInput)],
) -> String {
    let mut participants = Vec::with_capacity(recipients.len() + 1);
    participants.push(from_address.to_string());
    participants.extend(
        recipients
            .iter()
            .map(|(_, recipient)| recipient.address.clone()),
    );
    participants.join(" ")
}

fn ensure_parent_directory(path: &str) -> Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

fn encode_pst_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

fn decode_pst_field(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.chars();
    while let Some(char) = chars.next() {
        if char != '\\' {
            output.push(char);
            continue;
        }

        match chars.next() {
            Some('t') => output.push('\t'),
            Some('r') => output.push('\r'),
            Some('n') => output.push('\n'),
            Some('\\') => output.push('\\'),
            Some(other) => {
                output.push('\\');
                output.push(other);
            }
            None => output.push('\\'),
        }
    }
    output
}
