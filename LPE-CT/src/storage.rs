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
                    received_unix BIGINT NOT NULL DEFAULT 0,
                    peer TEXT NOT NULL,
                    helo TEXT NOT NULL,
                    mail_from TEXT NOT NULL,
                    sender_domain TEXT,
                    rcpt_to JSONB NOT NULL,
                    recipient_domains JSONB NOT NULL DEFAULT '[]'::JSONB,
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
                    remote_message_ref TEXT,
                    route_target TEXT,
                    search_text TEXT NOT NULL DEFAULT '',
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
                    digest_report_retention_days INTEGER NOT NULL DEFAULT 14,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                "#,
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS received_unix BIGINT NOT NULL DEFAULT 0"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS sender_domain TEXT"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS recipient_domains JSONB NOT NULL DEFAULT '[]'::JSONB"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS remote_message_ref TEXT"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS route_target TEXT"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE quarantine_messages ADD COLUMN IF NOT EXISTS search_text TEXT NOT NULL DEFAULT ''"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "ALTER TABLE digest_settings ADD COLUMN IF NOT EXISTS digest_report_retention_days INTEGER NOT NULL DEFAULT 14"
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
                r#"
                CREATE TABLE IF NOT EXISTS accepted_domains (
                    domain TEXT PRIMARY KEY,
                    domain_id TEXT NOT NULL,
                    destination_server TEXT NOT NULL,
                    verification_type TEXT NOT NULL,
                    rbl_checks BOOLEAN NOT NULL DEFAULT TRUE,
                    spf_checks BOOLEAN NOT NULL DEFAULT TRUE,
                    greylisting BOOLEAN NOT NULL DEFAULT TRUE,
                    verified BOOLEAN NOT NULL DEFAULT FALSE,
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
                "CREATE INDEX IF NOT EXISTS quarantine_messages_received_unix_idx ON quarantine_messages (received_unix DESC)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS quarantine_messages_sender_domain_idx ON quarantine_messages (sender_domain)"
            )
            .execute(pool)
            .await?;
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS quarantine_messages_search_tsv_idx ON quarantine_messages USING GIN (to_tsvector('simple', search_text))"
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
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS accepted_domains_verified_idx ON accepted_domains (verified, domain)"
            )
            .execute(pool)
            .await?;
            if ensure_pg_trgm_extension(pool).await {
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS mail_flow_history_search_trgm_idx ON mail_flow_history USING GIN (search_text gin_trgm_ops)"
                )
                .execute(pool)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS quarantine_messages_search_trgm_idx ON quarantine_messages USING GIN (search_text gin_trgm_ops)"
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

    let mut accepted_domain_keys = Vec::new();
    for domain in &dashboard.accepted_domains {
        let domain_key = domain.domain.trim().to_ascii_lowercase();
        if domain_key.is_empty() {
            continue;
        }
        accepted_domain_keys.push(domain_key.clone());
        sqlx::query(
            r#"
            INSERT INTO accepted_domains (
                domain, domain_id, destination_server, verification_type, rbl_checks,
                spf_checks, greylisting, verified, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (domain) DO UPDATE SET
                domain_id = EXCLUDED.domain_id,
                destination_server = EXCLUDED.destination_server,
                verification_type = EXCLUDED.verification_type,
                rbl_checks = EXCLUDED.rbl_checks,
                spf_checks = EXCLUDED.spf_checks,
                greylisting = EXCLUDED.greylisting,
                verified = EXCLUDED.verified,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                accepted_domains.domain_id IS DISTINCT FROM EXCLUDED.domain_id OR
                accepted_domains.destination_server IS DISTINCT FROM EXCLUDED.destination_server OR
                accepted_domains.verification_type IS DISTINCT FROM EXCLUDED.verification_type OR
                accepted_domains.rbl_checks IS DISTINCT FROM EXCLUDED.rbl_checks OR
                accepted_domains.spf_checks IS DISTINCT FROM EXCLUDED.spf_checks OR
                accepted_domains.greylisting IS DISTINCT FROM EXCLUDED.greylisting OR
                accepted_domains.verified IS DISTINCT FROM EXCLUDED.verified OR
                accepted_domains.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind(domain_key)
        .bind(domain.id.trim())
        .bind(domain.destination_server.trim())
        .bind(domain.verification_type.trim().to_ascii_lowercase())
        .bind(domain.rbl_checks)
        .bind(domain.spf_checks)
        .bind(domain.greylisting)
        .bind(domain.verified)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "DELETE FROM accepted_domains WHERE source = 'state.json' AND NOT (domain = ANY($1))",
    )
    .bind(&accepted_domain_keys)
    .execute(&mut *tx)
    .await?;

    let mut allow_sender_keys = Vec::new();
    for value in &dashboard.policies.address_policy.allow_senders {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        allow_sender_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (address_role, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                policy_address_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                policy_address_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("sender")
        .bind("allow")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_policy_address_rules(&mut tx, "sender", "allow", &allow_sender_keys).await?;

    let mut block_sender_keys = Vec::new();
    for value in &dashboard.policies.address_policy.block_senders {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        block_sender_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (address_role, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                policy_address_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                policy_address_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("sender")
        .bind("block")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_policy_address_rules(&mut tx, "sender", "block", &block_sender_keys).await?;

    let mut allow_recipient_keys = Vec::new();
    for value in &dashboard.policies.address_policy.allow_recipients {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        allow_recipient_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (address_role, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                policy_address_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                policy_address_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("recipient")
        .bind("allow")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_policy_address_rules(&mut tx, "recipient", "allow", &allow_recipient_keys).await?;

    let mut block_recipient_keys = Vec::new();
    for value in &dashboard.policies.address_policy.block_recipients {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        block_recipient_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO policy_address_rules (
                address_role, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (address_role, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                policy_address_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                policy_address_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("recipient")
        .bind("block")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_policy_address_rules(&mut tx, "recipient", "block", &block_recipient_keys).await?;

    let mut allow_extension_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.allow_extensions {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        allow_extension_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("extension")
        .bind("allow")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(&mut tx, "extension", "allow", &allow_extension_keys)
        .await?;

    let mut block_extension_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.block_extensions {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        block_extension_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("extension")
        .bind("block")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(&mut tx, "extension", "block", &block_extension_keys)
        .await?;

    let mut allow_mime_type_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.allow_mime_types {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        allow_mime_type_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("mime-type")
        .bind("allow")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(&mut tx, "mime-type", "allow", &allow_mime_type_keys)
        .await?;

    let mut block_mime_type_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.block_mime_types {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        block_mime_type_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("mime-type")
        .bind("block")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(&mut tx, "mime-type", "block", &block_mime_type_keys)
        .await?;

    let mut allow_detected_type_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.allow_detected_types {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        allow_detected_type_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("detected-type")
        .bind("allow")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(
        &mut tx,
        "detected-type",
        "allow",
        &allow_detected_type_keys,
    )
    .await?;

    let mut block_detected_type_keys = Vec::new();
    for value in &dashboard.policies.attachment_policy.block_detected_types {
        let value = value.trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        block_detected_type_keys.push(value.clone());
        sqlx::query(
            r#"
            INSERT INTO attachment_policy_rules (
                rule_scope, action, match_value, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (rule_scope, action, match_value) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                attachment_policy_rules.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                attachment_policy_rules.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("detected-type")
        .bind("block")
        .bind(value)
        .bind(true)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    delete_stale_attachment_policy_rules(
        &mut tx,
        "detected-type",
        "block",
        &block_detected_type_keys,
    )
    .await?;

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

    let mut dkim_domain_keys = Vec::new();
    for domain in dashboard
        .policies
        .dkim
        .domains
        .iter()
        .filter(|domain| !domain.domain.trim().is_empty())
    {
        let domain_key = domain.domain.trim().to_ascii_lowercase();
        dkim_domain_keys.push(domain_key.clone());
        sqlx::query(
            r#"
            INSERT INTO dkim_domain_configs (
                domain, enabled, selector, private_key_path, over_sign, expiration_seconds,
                signed_headers, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (domain) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                selector = EXCLUDED.selector,
                private_key_path = EXCLUDED.private_key_path,
                over_sign = EXCLUDED.over_sign,
                expiration_seconds = EXCLUDED.expiration_seconds,
                signed_headers = EXCLUDED.signed_headers,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                dkim_domain_configs.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                dkim_domain_configs.selector IS DISTINCT FROM EXCLUDED.selector OR
                dkim_domain_configs.private_key_path IS DISTINCT FROM EXCLUDED.private_key_path OR
                dkim_domain_configs.over_sign IS DISTINCT FROM EXCLUDED.over_sign OR
                dkim_domain_configs.expiration_seconds IS DISTINCT FROM EXCLUDED.expiration_seconds OR
                dkim_domain_configs.signed_headers IS DISTINCT FROM EXCLUDED.signed_headers OR
                dkim_domain_configs.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind(domain_key)
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
        "DELETE FROM dkim_domain_configs WHERE source = 'state.json' AND NOT (domain = ANY($1))",
    )
    .bind(&dkim_domain_keys)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO digest_settings (
            settings_key, digest_enabled, digest_interval_minutes, digest_max_items,
            history_retention_days, digest_report_retention_days, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (settings_key) DO UPDATE SET
            digest_enabled = EXCLUDED.digest_enabled,
            digest_interval_minutes = EXCLUDED.digest_interval_minutes,
            digest_max_items = EXCLUDED.digest_max_items,
            history_retention_days = EXCLUDED.history_retention_days,
            digest_report_retention_days = EXCLUDED.digest_report_retention_days,
            updated_at = NOW()
        "#,
    )
    .bind("active")
    .bind(dashboard.reporting.digest_enabled)
    .bind(i32::try_from(dashboard.reporting.digest_interval_minutes).unwrap_or(i32::MAX))
    .bind(i32::try_from(dashboard.reporting.digest_max_items).unwrap_or(i32::MAX))
    .bind(i32::try_from(dashboard.reporting.history_retention_days).unwrap_or(i32::MAX))
    .bind(i32::try_from(dashboard.reporting.digest_report_retention_days).unwrap_or(i32::MAX))
    .execute(&mut *tx)
    .await?;

    let mut digest_recipient_keys = Vec::new();
    for domain in &dashboard.reporting.domain_defaults {
        for recipient in &domain.recipients {
            let scope_key = domain.domain.trim().to_ascii_lowercase();
            let recipient = recipient.trim().to_ascii_lowercase();
            if scope_key.is_empty() || recipient.is_empty() {
                continue;
            }
            digest_recipient_keys.push(format!("domain-default\t{scope_key}\t{recipient}"));
            sqlx::query(
                r#"
                INSERT INTO digest_recipients (
                    scope_type, scope_key, recipient, enabled, source, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (scope_type, scope_key, recipient) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    source = EXCLUDED.source,
                    updated_at = NOW()
                WHERE
                    digest_recipients.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                    digest_recipients.source IS DISTINCT FROM EXCLUDED.source
                "#,
            )
            .bind("domain-default")
            .bind(scope_key)
            .bind(recipient)
            .bind(true)
            .bind("state.json")
            .execute(&mut *tx)
            .await?;
        }
    }
    for override_entry in &dashboard.reporting.user_overrides {
        let scope_key = override_entry.mailbox.trim().to_ascii_lowercase();
        let recipient = override_entry.recipient.trim().to_ascii_lowercase();
        if scope_key.is_empty() || recipient.is_empty() {
            continue;
        }
        digest_recipient_keys.push(format!("mailbox-override\t{scope_key}\t{recipient}"));
        sqlx::query(
            r#"
            INSERT INTO digest_recipients (
                scope_type, scope_key, recipient, enabled, source, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (scope_type, scope_key, recipient) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                updated_at = NOW()
            WHERE
                digest_recipients.enabled IS DISTINCT FROM EXCLUDED.enabled OR
                digest_recipients.source IS DISTINCT FROM EXCLUDED.source
            "#,
        )
        .bind("mailbox-override")
        .bind(scope_key)
        .bind(recipient)
        .bind(override_entry.enabled)
        .bind("state.json")
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "DELETE FROM digest_recipients WHERE source = 'state.json' AND NOT (concat_ws(chr(9), scope_type, scope_key, recipient) = ANY($1))",
    )
    .bind(&digest_recipient_keys)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

async fn delete_stale_policy_address_rules(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    address_role: &str,
    action: &str,
    active_values: &[String],
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM policy_address_rules
        WHERE source = 'state.json'
          AND address_role = $1
          AND action = $2
          AND NOT (match_value = ANY($3))
        "#,
    )
    .bind(address_role)
    .bind(action)
    .bind(active_values)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn delete_stale_attachment_policy_rules(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rule_scope: &str,
    action: &str,
    active_values: &[String],
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM attachment_policy_rules
        WHERE source = 'state.json'
          AND rule_scope = $1
          AND action = $2
          AND NOT (match_value = ANY($3))
        "#,
    )
    .bind(rule_scope)
    .bind(action)
    .bind(active_values)
    .execute(&mut **tx)
    .await?;

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
