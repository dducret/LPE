use anyhow::{anyhow, Context, Result};
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use std::sync::OnceLock;
use tokio::sync::OnceCell;

#[derive(Debug, Clone, Default)]
pub(crate) struct LocalDbConfig {
    pub enabled: bool,
    pub database_url: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecipientVerificationCacheEntry {
    pub cache_key: String,
    pub sender: Option<String>,
    pub recipient: String,
    pub account_id: Option<String>,
    pub verdict: String,
    pub detail: Option<String>,
    pub expires_at_unix: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RecipientVerificationCacheRecord {
    pub verdict: String,
    pub detail: Option<String>,
    pub expires_at_unix: u64,
}

static LOCAL_DB_POOL: OnceCell<PgPool> = OnceCell::const_new();
static LOCAL_DB_POOL_URL: OnceLock<String> = OnceLock::new();
static LOCAL_DB_SCHEMA_READY: OnceCell<()> = OnceCell::const_new();

pub(crate) async fn ensure_local_db_schema(
    config: &LocalDbConfig,
) -> Result<Option<&'static PgPool>> {
    let Some(pool) = local_db_pool(config).await? else {
        return Ok(None);
    };

    LOCAL_DB_SCHEMA_READY
        .get_or_try_init(|| async {
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS greylist_entries (
                    entry_key TEXT PRIMARY KEY,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS reputation_entries (
                    entry_key TEXT PRIMARY KEY,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS bayespam_corpora (
                    corpus_key TEXT PRIMARY KEY,
                    corpus JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS throttle_windows (
                    rule_id TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (rule_id, bucket_key)
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS quarantine_messages (
                    trace_id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    peer TEXT NOT NULL,
                    helo TEXT NOT NULL,
                    mail_from TEXT NOT NULL,
                    rcpt_to JSONB NOT NULL,
                    subject TEXT NOT NULL,
                    internet_message_id TEXT,
                    spool_path TEXT NOT NULL,
                    reason TEXT,
                    spam_score REAL NOT NULL,
                    security_score REAL NOT NULL,
                    reputation_score INTEGER NOT NULL,
                    dnsbl_hits JSONB NOT NULL,
                    auth_summary JSONB NOT NULL,
                    decision_trace JSONB NOT NULL,
                    magika_summary TEXT,
                    magika_decision TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS mail_flow_history (
                    event_key TEXT PRIMARY KEY,
                    event_unix BIGINT NOT NULL,
                    timestamp TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    queue TEXT NOT NULL,
                    status TEXT NOT NULL,
                    peer TEXT NOT NULL,
                    mail_from TEXT NOT NULL,
                    rcpt_to JSONB NOT NULL,
                    subject TEXT NOT NULL,
                    internet_message_id TEXT,
                    reason TEXT,
                    route_target TEXT,
                    remote_message_ref TEXT,
                    spam_score REAL NOT NULL,
                    security_score REAL NOT NULL,
                    reputation_score INTEGER NOT NULL,
                    dnsbl_hits JSONB NOT NULL,
                    auth_summary JSONB NOT NULL,
                    magika_summary TEXT,
                    magika_decision TEXT,
                    technical_status JSONB,
                    dsn JSONB,
                    throttle JSONB,
                    decision_trace JSONB NOT NULL,
                    search_text TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS policy_address_rules (
                    address_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    match_value TEXT NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (address_role, action, match_value)
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS attachment_policy_rules (
                    rule_scope TEXT NOT NULL,
                    action TEXT NOT NULL,
                    match_value TEXT NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (rule_scope, action, match_value)
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS digest_settings (
                    settings_key TEXT PRIMARY KEY,
                    digest_enabled BOOLEAN NOT NULL,
                    digest_interval_minutes INTEGER NOT NULL,
                    digest_max_items INTEGER NOT NULL,
                    history_retention_days INTEGER NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS digest_recipients (
                    scope_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    source TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (scope_type, scope_key, recipient)
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS recipient_verification_cache (
                    cache_key TEXT PRIMARY KEY,
                    sender TEXT,
                    recipient TEXT NOT NULL,
                    account_id TEXT,
                    verdict TEXT NOT NULL,
                    detail TEXT,
                    expires_at TIMESTAMPTZ NOT NULL,
                    cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS recipient_verification_settings (
                    settings_key TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL,
                    fail_closed BOOLEAN NOT NULL,
                    cache_ttl_seconds BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS dkim_domain_configs (
                    domain TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    selector TEXT NOT NULL,
                    private_key_path TEXT NOT NULL,
                    over_sign BOOLEAN NOT NULL DEFAULT TRUE,
                    expiration_seconds BIGINT,
                    signed_headers JSONB NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS mail_flow_history_trace_id_idx ON mail_flow_history (trace_id, event_unix DESC)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS mail_flow_history_event_unix_idx ON mail_flow_history (event_unix DESC)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS mail_flow_history_search_tsv_idx ON mail_flow_history USING GIN (to_tsvector('simple', search_text))"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS quarantine_messages_status_received_idx ON quarantine_messages (status, received_at DESC)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS policy_address_rules_role_action_idx ON policy_address_rules (address_role, action)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS attachment_policy_rules_scope_action_idx ON attachment_policy_rules (rule_scope, action)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS digest_recipients_scope_idx ON digest_recipients (scope_type, scope_key)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS recipient_verification_cache_expires_idx ON recipient_verification_cache (expires_at)"
            )
            .execute(pool)
            .await?;
            if ensure_pg_trgm_extension(pool).await {
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS mail_flow_history_search_trgm_idx ON mail_flow_history USING GIN (search_text gin_trgm_ops)"
                )
                .execute(pool)
                .await?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(Some(pool))
}

pub(crate) async fn sync_dashboard_configuration(
    config: &LocalDbConfig,
    dashboard: &crate::DashboardState,
) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };

    let mut tx = pool.begin().await?;

    sqlx::query("DELETE FROM policy_address_rules")
        .execute(&mut *tx)
        .await?;
    for value in &dashboard.policies.address_policy.allow_senders {
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("sender")
        .bind("allow")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.address_policy.block_senders {
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("sender")
        .bind("block")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.address_policy.allow_recipients {
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("recipient")
        .bind("allow")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.address_policy.block_recipients {
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("recipient")
        .bind("block")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query("DELETE FROM attachment_policy_rules")
        .execute(&mut *tx)
        .await?;
    for value in &dashboard.policies.attachment_policy.allow_extensions {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("extension")
        .bind("allow")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.attachment_policy.block_extensions {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("extension")
        .bind("block")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.attachment_policy.allow_mime_types {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("mime-type")
        .bind("allow")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.attachment_policy.block_mime_types {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("mime-type")
        .bind("block")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.attachment_policy.allow_detected_types {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("detected-type")
        .bind("allow")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    for value in &dashboard.policies.attachment_policy.block_detected_types {
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("detected-type")
        .bind("block")
        .bind(value.trim().to_ascii_lowercase())
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO recipient_verification_settings (
            settings_key, enabled, fail_closed, cache_ttl_seconds, updated_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (settings_key) DO UPDATE SET
            enabled = EXCLUDED.enabled,
            fail_closed = EXCLUDED.fail_closed,
            cache_ttl_seconds = EXCLUDED.cache_ttl_seconds,
            updated_at = NOW()
        "#,
    )
    .bind("active")
    .bind(dashboard.policies.recipient_verification.enabled)
    .bind(dashboard.policies.recipient_verification.fail_closed)
    .bind(i64::from(
        dashboard.policies.recipient_verification.cache_ttl_seconds,
    ))
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM dkim_domain_configs")
        .execute(&mut *tx)
        .await?;
    for domain in dashboard
        .policies
        .dkim
        .domains
        .iter()
        .filter(|domain| !domain.domain.trim().is_empty())
    {
        sqlx::query(
            r#"
            INSERT INTO dkim_domain_configs (
                domain, enabled, selector, private_key_path, over_sign, expiration_seconds,
                signed_headers, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            "#,
        )
        .bind(domain.domain.trim().to_ascii_lowercase())
        .bind(domain.enabled)
        .bind(domain.selector.trim())
        .bind(domain.private_key_path.trim())
        .bind(dashboard.policies.dkim.over_sign)
        .bind(dashboard.policies.dkim.expiration_seconds.map(i64::from))
        .bind(Json(dashboard.policies.dkim.headers.clone()))
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO digest_settings (
            settings_key, digest_enabled, digest_interval_minutes, digest_max_items,
            history_retention_days, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (settings_key) DO UPDATE SET
            digest_enabled = EXCLUDED.digest_enabled,
            digest_interval_minutes = EXCLUDED.digest_interval_minutes,
            digest_max_items = EXCLUDED.digest_max_items,
            history_retention_days = EXCLUDED.history_retention_days,
            updated_at = NOW()
        "#,
    )
    .bind("active")
    .bind(dashboard.reporting.digest_enabled)
    .bind(i32::try_from(dashboard.reporting.digest_interval_minutes).unwrap_or(i32::MAX))
    .bind(i32::try_from(dashboard.reporting.digest_max_items).unwrap_or(i32::MAX))
    .bind(i32::try_from(dashboard.reporting.history_retention_days).unwrap_or(i32::MAX))
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM digest_recipients")
        .execute(&mut *tx)
        .await?;
    for domain in &dashboard.reporting.domain_defaults {
        for recipient in &domain.recipients {
            sqlx::query(
                r#"
                INSERT INTO digest_recipients (
                    scope_type, scope_key, recipient, enabled, source, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                "#,
            )
            .bind("domain-default")
            .bind(domain.domain.trim().to_ascii_lowercase())
            .bind(recipient.trim().to_ascii_lowercase())
            .bind(true)
            .bind("state.json")
            .execute(&mut *tx)
            .await?;
        }
    }
    for override_entry in &dashboard.reporting.user_overrides {
        sqlx::query(
            r#"
            INSERT INTO digest_recipients (
                scope_type, scope_key, recipient, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind("mailbox-override")
        .bind(override_entry.mailbox.trim().to_ascii_lowercase())
        .bind(override_entry.recipient.trim().to_ascii_lowercase())
        .bind(override_entry.enabled)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub(crate) async fn load_recipient_verification_cache_entry(
    config: &LocalDbConfig,
    cache_key: &str,
    now_unix: u64,
) -> Result<Option<RecipientVerificationCacheRecord>> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(None);
    };

    sqlx::query("DELETE FROM recipient_verification_cache WHERE expires_at <= to_timestamp($1)")
        .bind(now_unix as i64)
        .execute(pool)
        .await?;

    let row = sqlx::query(
        r#"
        SELECT
            verdict,
            detail,
            EXTRACT(EPOCH FROM expires_at)::BIGINT AS expires_at_unix
        FROM recipient_verification_cache
        WHERE cache_key = $1
        "#,
    )
    .bind(cache_key)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| RecipientVerificationCacheRecord {
        verdict: row.get::<String, _>("verdict"),
        detail: row.get::<Option<String>, _>("detail"),
        expires_at_unix: row
            .get::<Option<i64>, _>("expires_at_unix")
            .unwrap_or_default()
            .max(0) as u64,
    }))
}

pub(crate) async fn persist_recipient_verification_cache_entry(
    config: &LocalDbConfig,
    entry: &RecipientVerificationCacheEntry,
) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };

    sqlx::query(
        r#"
        INSERT INTO recipient_verification_cache (
            cache_key, sender, recipient, account_id, verdict, detail, expires_at, cached_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7), NOW(), NOW())
        ON CONFLICT (cache_key) DO UPDATE SET
            sender = EXCLUDED.sender,
            recipient = EXCLUDED.recipient,
            account_id = EXCLUDED.account_id,
            verdict = EXCLUDED.verdict,
            detail = EXCLUDED.detail,
            expires_at = EXCLUDED.expires_at,
            updated_at = NOW()
        "#,
    )
    .bind(&entry.cache_key)
    .bind(&entry.sender)
    .bind(&entry.recipient)
    .bind(&entry.account_id)
    .bind(&entry.verdict)
    .bind(&entry.detail)
    .bind(entry.expires_at_unix as i64)
    .execute(pool)
    .await?;

    Ok(())
}

async fn local_db_pool(config: &LocalDbConfig) -> Result<Option<&'static PgPool>> {
    if !config.enabled {
        return Ok(None);
    }

    let database_url = config.database_url.as_deref().ok_or_else(|| {
        anyhow!("LPE_CT_LOCAL_DB_URL must be set when LPE_CT_LOCAL_DB_ENABLED=true")
    })?;

    if let Some(initialized_url) = LOCAL_DB_POOL_URL.get() {
        if initialized_url != database_url {
            return Err(anyhow!(
                "LPE_CT_LOCAL_DB_URL changed after pool initialization; restart LPE-CT to switch databases"
            ));
        }
    }

    let pool = LOCAL_DB_POOL
        .get_or_try_init(|| async move {
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .connect(database_url)
                .await
                .with_context(|| "unable to connect to the dedicated LPE-CT PostgreSQL store")?;
            let _ = LOCAL_DB_POOL_URL.set(database_url.to_string());
            Ok::<PgPool, anyhow::Error>(pool)
        })
        .await?;

    Ok(Some(pool))
}

async fn ensure_pg_trgm_extension(pool: &PgPool) -> bool {
    match sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        .execute(pool)
        .await
    {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "unable to enable pg_trgm on the dedicated LPE-CT PostgreSQL store"
            );
            false
        }
    }
}
