use anyhow::Result;
use serde::Serialize;
use sqlx::{FromRow, Pool, Postgres, Row};
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
    pub server_settings: ServerSettings,
    pub security_settings: SecuritySettings,
    pub local_ai_settings: LocalAiSettings,
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
            server_settings,
            security_settings,
            local_ai_settings,
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

        self.insert_audit(&mut tx, audit).await?;
        tx.commit().await?;
        Ok(())
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
