use std::{env, str::FromStr};

use anyhow::{Context, Result};
use lpe_storage::{
    AuditEntryInput, NewDomain, NewMailbox, Storage, SubmitMessageInput, SubmittedMessage,
    SubmittedRecipientInput,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const PLATFORM_TENANT_ID: &str = "__platform__";

struct RuntimeFixture {
    account_id: Uuid,
    inbox_id: Uuid,
    account_email: String,
}

#[tokio::test]
async fn schema_sql_matches_representative_runtime_paths_when_database_is_enabled() -> Result<()> {
    let Some(database_url) = env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping runtime schema drift validation; TEST_DATABASE_URL is not set");
        return Ok(());
    };

    let schema_name = format!("lpe_runtime_drift_{}", Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&database_url)?)
        .await
        .context("connect to TEST_DATABASE_URL for runtime schema drift validation")?;

    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public")
        .execute(&admin_pool)
        .await
        .context("ensure pg_trgm is available before applying schema.sql")?;
    sqlx::query(&format!("CREATE SCHEMA {schema_name}"))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("create isolated test schema {schema_name}"))?;

    let search_path = format!("{schema_name},public");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            PgConnectOptions::from_str(&database_url)?.options([("search_path", &search_path)]),
        )
        .await
        .with_context(|| format!("connect with search_path={search_path}"))?;

    let result = run_runtime_drift_validation(&pool).await;

    pool.close().await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("drop isolated test schema {schema_name}"));
    admin_pool.close().await;

    cleanup?;
    result
}

async fn run_runtime_drift_validation(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .context("apply crates/lpe-storage/sql/schema.sql")?;
    assert_schema_metadata(pool).await?;

    let storage = Storage::new(pool.clone());
    let mut failures = Vec::new();

    collect(
        &mut failures,
        "platform tenant test fixture",
        seed_platform_tenant(pool).await,
    );

    collect(
        &mut failures,
        "admin SQL path",
        exercise_admin_path(&storage).await,
    );

    let fixture = collect(
        &mut failures,
        "mailbox fixture",
        seed_mailbox_fixture(pool).await,
    );

    if let Some(fixture) = fixture {
        collect(
            &mut failures,
            "mailbox SQL path",
            exercise_mailbox_path(&storage, &fixture).await,
        );

        let submitted = collect(
            &mut failures,
            "submission SQL path",
            exercise_submission_path(&storage, &fixture).await,
        );

        collect(
            &mut failures,
            "JMAP query SQL path",
            exercise_jmap_path(&storage, &fixture, submitted.as_ref()).await,
        );

        collect(
            &mut failures,
            "ActiveSync state SQL path",
            exercise_activesync_path(&storage, &fixture).await,
        );
    }

    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "schema/runtime drift validation failed:\n- {}",
            failures.join("\n- ")
        );
    }
}

fn collect<T>(failures: &mut Vec<String>, label: &str, result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            failures.push(format!("{label}: {error:#}"));
            None
        }
    }
}

async fn assert_schema_metadata(pool: &PgPool) -> Result<()> {
    let version = sqlx::query_scalar::<_, String>(
        "SELECT schema_version FROM schema_metadata WHERE singleton = TRUE",
    )
    .fetch_one(pool)
    .await
    .context("read schema_metadata after applying schema.sql")?;
    anyhow::ensure!(
        version == "0.3.0-sql-v2",
        "unexpected schema version {version}"
    );
    Ok(())
}

async fn seed_platform_tenant(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, 'platform', 'Platform')
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(PLATFORM_TENANT_ID)
    .execute(pool)
    .await
    .context("seed platform tenant expected by admin runtime SQL")?;
    Ok(())
}

async fn exercise_admin_path(storage: &Storage) -> Result<()> {
    storage
        .create_domain(
            NewDomain {
                name: format!("admin-{}.example.test", Uuid::new_v4().simple()),
                default_quota_mb: 4096,
                inbound_enabled: true,
                outbound_enabled: true,
                default_sieve_script: String::new(),
                jmap_push_journal_retention_days: 30,
            },
            audit("test-admin", "domain.create", "admin drift probe"),
        )
        .await
        .context("create_domain")?;

    storage
        .fetch_admin_dashboard()
        .await
        .context("fetch_admin_dashboard")?;
    Ok(())
}

async fn seed_mailbox_fixture(pool: &PgPool) -> Result<RuntimeFixture> {
    let unique = Uuid::new_v4().simple().to_string();
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let inbox_id = Uuid::new_v4();
    let domain_name = format!("runtime-{unique}.example.test");
    let account_email = format!("alice@{domain_name}");

    sqlx::query(
        r#"
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(tenant_id)
    .bind(format!("runtime-{unique}"))
    .bind("Runtime Drift Tenant")
    .execute(pool)
    .await
    .context("seed runtime tenant")?;

    sqlx::query(
        r#"
        INSERT INTO domains (id, tenant_id, name, default_quota_mb)
        VALUES ($1, $2, $3, 4096)
        "#,
    )
    .bind(domain_id)
    .bind(tenant_id)
    .bind(&domain_name)
    .execute(pool)
    .await
    .context("seed runtime domain")?;

    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Alice Drift')
        "#,
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(&account_email)
    .execute(pool)
    .await
    .context("seed runtime account")?;

    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 1)
        "#,
    )
    .bind(inbox_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(pool)
    .await
    .context("seed runtime inbox mailbox")?;

    Ok(RuntimeFixture {
        account_id,
        inbox_id,
        account_email,
    })
}

async fn exercise_mailbox_path(storage: &Storage, fixture: &RuntimeFixture) -> Result<()> {
    storage
        .create_mailbox(
            NewMailbox {
                account_id: fixture.account_id,
                display_name: "Runtime Drift Folder".to_string(),
                role: "custom".to_string(),
                retention_days: 365,
            },
            audit("test-admin", "mailbox.create", "runtime drift mailbox"),
        )
        .await
        .context("create_mailbox")?;
    storage
        .ensure_imap_mailboxes(fixture.account_id)
        .await
        .context("ensure_imap_mailboxes")?;
    storage
        .fetch_jmap_mailboxes(fixture.account_id)
        .await
        .context("fetch_jmap_mailboxes")?;
    storage
        .fetch_imap_mailbox_state(fixture.account_id, fixture.inbox_id)
        .await
        .context("fetch_imap_mailbox_state")?;
    Ok(())
}

async fn exercise_submission_path(
    storage: &Storage,
    fixture: &RuntimeFixture,
) -> Result<SubmittedMessage> {
    storage
        .submit_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "jmap".to_string(),
                from_display: Some("Alice Drift".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob Example".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![SubmittedRecipientInput {
                    address: "audit-hidden@example.test".to_string(),
                    display_name: None,
                }],
                subject: "Runtime schema drift probe".to_string(),
                body_text: "Body text used by drift validation.".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!("<{}@example.test>", Uuid::new_v4())),
                mime_blob_ref: None,
                size_octets: 128,
                unread: Some(false),
                flagged: Some(false),
                attachments: Vec::new(),
            },
            audit("alice@example.test", "message.submit", "runtime drift message"),
        )
        .await
        .context("submit_message")
}

async fn exercise_jmap_path(
    storage: &Storage,
    fixture: &RuntimeFixture,
    submitted: Option<&SubmittedMessage>,
) -> Result<()> {
    let query = storage
        .query_jmap_email_ids(
            fixture.account_id,
            None,
            Some("runtime schema drift"),
            0,
            10,
        )
        .await
        .context("query_jmap_email_ids")?;

    if let Some(submitted) = submitted {
        storage
            .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
            .await
            .context("fetch_jmap_emails")?;
        storage
            .fetch_jmap_email_submissions(fixture.account_id, &[submitted.outbound_queue_id])
            .await
            .context("fetch_jmap_email_submissions")?;
    } else if !query.ids.is_empty() {
        storage
            .fetch_jmap_emails(fixture.account_id, &query.ids)
            .await
            .context("fetch_jmap_emails")?;
    }

    let state_id = storage
        .save_jmap_query_state(
            fixture.account_id,
            "Email/query",
            Some(serde_json::json!({"text": "runtime schema drift"})),
            None,
            1,
            &query
                .ids
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<String>>(),
        )
        .await
        .context("save_jmap_query_state")?;
    storage
        .fetch_jmap_query_state(
            fixture.account_id,
            "Email/query",
            state_id,
            Some(serde_json::json!({"text": "runtime schema drift"})),
            None,
        )
        .await
        .context("fetch_jmap_query_state")?;

    Ok(())
}

async fn exercise_activesync_path(storage: &Storage, fixture: &RuntimeFixture) -> Result<()> {
    storage
        .store_activesync_sync_state(
            fixture.account_id,
            "runtime-drift-device",
            &fixture.inbox_id.to_string(),
            "sync-1",
            r#"{"ids":[]}"#,
        )
        .await
        .context("store_activesync_sync_state")?;
    storage
        .fetch_activesync_sync_state(
            fixture.account_id,
            "runtime-drift-device",
            &fixture.inbox_id.to_string(),
            "sync-1",
        )
        .await
        .context("fetch_activesync_sync_state")?;
    storage
        .fetch_activesync_email_states(fixture.account_id, fixture.inbox_id, 0, 10)
        .await
        .context("fetch_activesync_email_states")?;
    Ok(())
}

fn audit(actor: &str, action: &str, subject: &str) -> AuditEntryInput {
    AuditEntryInput {
        actor: actor.to_string(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}
