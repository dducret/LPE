use std::{env, str::FromStr};

use anyhow::{Context, Result};
use lpe_storage::{
    AttachmentUploadInput, AuditEntryInput, JmapImportedEmailInput, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, NewAccount, NewDomain, NewMailbox, NewPstTransferJob, ReminderQuery,
    SenderDelegationGrantInput, SenderDelegationRight, Storage, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientNoteInput, UpsertJournalEntryInput,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
    PgPool, Row,
};
use uuid::Uuid;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const PLATFORM_TENANT_ID: Uuid = Uuid::from_u128(1);

struct RuntimeFixture {
    tenant_id: Uuid,
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
        "blob ownership constraints",
        exercise_blob_reference_constraints(pool).await,
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
            "change log and cursor constraints",
            exercise_change_log_cursor_constraints(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "mailbox SQL path",
            exercise_mailbox_path(&storage, &fixture).await,
        );
        collect(
            &mut failures,
            "mailbox canonical name storage guards",
            exercise_mailbox_name_policy_storage_guards(&storage, pool, &fixture).await,
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

        if let Some(submitted) = submitted.as_ref() {
            collect(
                &mut failures,
                "representative index plan paths",
                exercise_index_plan_paths(pool, &fixture, submitted).await,
            );
        }

        collect(
            &mut failures,
            "MAPI cross-protocol interoperability gate",
            exercise_mapi_cross_protocol_interoperability_gate(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "canonical identity allocation beyond MAPI",
            exercise_canonical_identity_allocation(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "canonical search-folder and rule replay",
            exercise_canonical_search_folder_and_rule_replay(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "ActiveSync state SQL path",
            exercise_activesync_path(&storage, &fixture).await,
        );
        collect(
            &mut failures,
            "notes journal and reminder SQL path",
            exercise_notes_journal_reminder_path(&storage, pool, &fixture).await,
        );

        if let Some(submitted) = submitted.as_ref() {
            collect(
                &mut failures,
                "PST SQL path",
                exercise_pst_path(&storage, submitted.sent_mailbox_id).await,
            );
            collect(
                &mut failures,
                "mailbox move membership semantics",
                exercise_mailbox_move_path(&storage, pool, &fixture, submitted).await,
            );
        }

        let delete_submitted = collect(
            &mut failures,
            "submission SQL path for delete replay",
            exercise_submission_path(&storage, &fixture).await,
        );
        if let Some(delete_submitted) = delete_submitted.as_ref() {
            collect(
                &mut failures,
                "MAPI delete cross-protocol visibility",
                exercise_mapi_delete_cross_protocol_path(
                    &storage,
                    pool,
                    &fixture,
                    delete_submitted,
                )
                .await,
            );
        }
        collect(
            &mut failures,
            "MAPI Trash purge cross-protocol visibility",
            exercise_mapi_trash_purge_cross_protocol_path(&storage, pool, &fixture).await,
        );
        collect(
            &mut failures,
            "MAPI Trash purge retention and legal-hold guard",
            exercise_mapi_trash_purge_retention_guard(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "admin dashboard SQL path",
            exercise_admin_dashboard_path(&storage).await,
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

async fn exercise_blob_reference_constraints(pool: &PgPool) -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    let tenant_id = Uuid::new_v4();
    let domain_a = Uuid::new_v4();
    let domain_b = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let mailbox_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mailbox_message_id = Uuid::new_v4();
    let raw_blob_a = Uuid::new_v4();
    let raw_blob_b = Uuid::new_v4();
    let attachment_blob_a = Uuid::new_v4();
    let attachment_blob_b = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO tenants (id, slug, display_name) VALUES ($1, $2, 'Blob Constraint Tenant')",
    )
    .bind(tenant_id)
    .bind(format!("blob-{unique}"))
    .execute(pool)
    .await
    .context("seed blob constraint tenant")?;
    sqlx::query("INSERT INTO domains (id, tenant_id, name) VALUES ($1, $2, $3), ($4, $2, $5)")
        .bind(domain_a)
        .bind(tenant_id)
        .bind(format!("blob-a-{unique}.example.test"))
        .bind(domain_b)
        .bind(format!("blob-b-{unique}.example.test"))
        .execute(pool)
        .await
        .context("seed blob constraint domains")?;
    sqlx::query(
        "INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
         VALUES ($1, $2, $3, $4, 'Blob Owner')",
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_a)
    .bind(format!("blob@blob-a-{unique}.example.test"))
    .execute(pool)
    .await
    .context("seed blob constraint account")?;
    sqlx::query(
        "INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, uid_validity)
         VALUES ($1, $2, $3, 'inbox', 'Inbox', 1)",
    )
    .bind(mailbox_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(pool)
    .await
    .context("seed blob constraint mailbox")?;

    insert_blob(pool, tenant_id, domain_a, raw_blob_a, "raw_message", 1).await?;
    insert_blob(pool, tenant_id, domain_b, raw_blob_b, "raw_message", 2).await?;
    insert_blob(
        pool,
        tenant_id,
        domain_a,
        attachment_blob_a,
        "attachment",
        3,
    )
    .await?;
    insert_blob(
        pool,
        tenant_id,
        domain_b,
        attachment_blob_b,
        "attachment",
        4,
    )
    .await?;

    expect_constraint_failure(
        "raw message blobs require database bytes",
        sqlx::query(
            r#"
            INSERT INTO blobs (
                id, tenant_id, domain_id, blob_kind, content_sha256,
                media_type, size_octets, blob_bytes, magika_status, validated_at
            )
            VALUES ($1, $2, $3, 'raw_message', $4, 'message/rfc822', 1, NULL, 'valid', NOW())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(domain_a)
        .bind(hex64(9))
        .execute(pool)
        .await,
    )?;

    let external_pool_id = Uuid::new_v4();
    let external_blob_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO storage_pools (id, name, pool_kind, status, config_json)
        VALUES ($1, $2, 's3_compatible', 'active', '{}'::jsonb)
        "#,
    )
    .bind(external_pool_id)
    .bind(format!("external-{unique}"))
    .execute(pool)
    .await
    .context("seed external storage pool for nullable blob bytes")?;
    sqlx::query(
        r#"
        INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256,
            media_type, size_octets, blob_bytes, magika_status, validated_at
        )
        VALUES ($1, $2, $3, 'attachment', $4, 'application/octet-stream', 5, NULL, 'valid', NOW())
        "#,
    )
    .bind(external_blob_id)
    .bind(tenant_id)
    .bind(domain_a)
    .bind(hex64(10))
    .execute(pool)
    .await
    .context("attachment blob may omit database bytes before external placement insert")?;
    sqlx::query(
        r#"
        INSERT INTO blob_placements (
            id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
            placement_status, verified_content_sha256, verified_size_octets, verified_at
        )
        VALUES ($1, $2, $3, $4, 'attachment', $5, 'active', $6, 5, NOW())
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(domain_a)
    .bind(external_blob_id)
    .bind(external_pool_id)
    .bind(hex64(10))
    .execute(pool)
    .await
    .context("external attachment placement accepts nullable database bytes")?;

    expect_constraint_failure(
        "messages reject attachment blob as raw message",
        sqlx::query(
            "INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id,
                message_hash, normalized_subject, received_at, size_octets
             )
             VALUES ($1, $2, $3, $4, NULL, $5, 'wrong kind', NOW(), 1)",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(domain_a)
        .bind(attachment_blob_a)
        .bind(hex64(30))
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "messages reject cross-domain raw blob",
        sqlx::query(
            "INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id,
                message_hash, normalized_subject, received_at, size_octets
             )
             VALUES ($1, $2, $3, $4, NULL, $5, 'cross domain', NOW(), 1)",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(domain_a)
        .bind(raw_blob_b)
        .bind(hex64(31))
        .execute(pool)
        .await,
    )?;

    sqlx::query(
        "INSERT INTO messages (
            id, tenant_id, domain_id, blob_id, internet_message_id,
            message_hash, normalized_subject, received_at, size_octets
         )
         VALUES ($1, $2, $3, $4, NULL, $5, 'valid', NOW(), 1)",
    )
    .bind(message_id)
    .bind(tenant_id)
    .bind(domain_a)
    .bind(raw_blob_a)
    .bind(hex64(32))
    .execute(pool)
    .await
    .context("seed valid message for blob constraints")?;
    sqlx::query(
        "INSERT INTO mailbox_messages (
            id, tenant_id, account_id, mailbox_id, message_id, imap_uid, received_at
         )
         VALUES ($1, $2, $3, $4, $5, 1, NOW())",
    )
    .bind(mailbox_message_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(mailbox_id)
    .bind(message_id)
    .execute(pool)
    .await
    .context("seed valid mailbox membership for blob constraints")?;

    expect_constraint_failure(
        "mime_parts reject raw blob as attachment blob",
        sqlx::query(
            "INSERT INTO mime_parts (
                id, tenant_id, message_id, domain_id, part_path, ordinal,
                content_type, size_octets, blob_id, blob_kind
             )
             VALUES ($1, $2, $3, $4, 'wrong-kind', 1, 'text/plain', 1, $5, 'attachment')",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .bind(domain_a)
        .bind(raw_blob_a)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "mime_parts reject cross-domain attachment blob",
        sqlx::query(
            "INSERT INTO mime_parts (
                id, tenant_id, message_id, domain_id, part_path, ordinal,
                content_type, size_octets, blob_id, blob_kind
             )
             VALUES ($1, $2, $3, $4, 'cross-domain', 2, 'text/plain', 1, $5, 'attachment')",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .bind(domain_a)
        .bind(attachment_blob_b)
        .execute(pool)
        .await,
    )?;

    expect_constraint_failure(
        "attachments reject raw blob",
        sqlx::query(
            "INSERT INTO attachments (
                id, tenant_id, account_id, mailbox_message_id, message_id, domain_id,
                blob_id, file_name, disposition, ordinal, size_octets
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'wrong.txt', 'attachment', 0, 1)",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_message_id)
        .bind(message_id)
        .bind(domain_a)
        .bind(raw_blob_a)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "attachments reject cross-domain attachment blob",
        sqlx::query(
            "INSERT INTO attachments (
                id, tenant_id, account_id, mailbox_message_id, message_id, domain_id,
                blob_id, file_name, disposition, ordinal, size_octets
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'cross.txt', 'attachment', 1, 1)",
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_message_id)
        .bind(message_id)
        .bind(domain_a)
        .bind(attachment_blob_b)
        .execute(pool)
        .await,
    )?;

    Ok(())
}

async fn insert_blob(
    pool: &PgPool,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_id: Uuid,
    blob_kind: &str,
    salt: u8,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256,
            media_type, size_octets, blob_bytes, magika_status, validated_at
         )
         VALUES ($1, $2, $3, $4, $5, 'application/octet-stream', 1, $6, 'valid', NOW())",
    )
    .bind(blob_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(blob_kind)
    .bind(hex64(salt))
    .bind(vec![salt])
    .execute(pool)
    .await
    .with_context(|| format!("seed {blob_kind} blob"))?;
    Ok(())
}

fn expect_constraint_failure<T>(
    label: &str,
    result: std::result::Result<T, sqlx::Error>,
) -> Result<()> {
    anyhow::ensure!(result.is_err(), "{label} unexpectedly succeeded");
    Ok(())
}

fn expect_anyhow_failure<T>(label: &str, result: Result<T>) -> Result<()> {
    anyhow::ensure!(result.is_err(), "{label} unexpectedly succeeded");
    Ok(())
}

fn jmap_create_input(
    account_id: Uuid,
    name: &str,
    parent_id: Option<Uuid>,
) -> JmapMailboxCreateInput {
    JmapMailboxCreateInput {
        account_id,
        name: name.to_string(),
        parent_id,
        sort_order: None,
        is_subscribed: true,
    }
}

fn hex64(value: u8) -> String {
    format!("{value:064x}")
}

async fn exercise_admin_path(storage: &Storage) -> Result<()> {
    let domain_name = format!("admin-{}.example.test", Uuid::new_v4().simple());
    let account_email = format!("alice@{domain_name}");
    storage
        .create_domain(
            NewDomain {
                name: domain_name.clone(),
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
        .create_account(
            NewAccount {
                email: account_email.clone(),
                display_name: "Alice Admin Drift".to_string(),
                quota_mb: 2048,
                gal_visibility: "tenant".to_string(),
                directory_kind: "user".to_string(),
            },
            audit("test-admin", "account.create", "admin drift account"),
        )
        .await
        .context("create_account")?;

    let canonical_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM accounts a
        JOIN account_email_addresses address
          ON address.tenant_id = a.tenant_id
         AND address.account_id = a.id
         AND address.email = a.primary_email
         AND address.is_primary = TRUE
        JOIN account_identities identity
          ON identity.tenant_id = address.tenant_id
         AND identity.account_id = address.account_id
         AND identity.email_address_id = address.id
         AND identity.is_default = TRUE
         AND identity.may_send = TRUE
        WHERE a.tenant_id = $1
          AND a.primary_email = $2
        "#,
    )
    .bind(PLATFORM_TENANT_ID)
    .bind(&account_email)
    .fetch_one(storage.pool())
    .await
    .context("count canonical account identity rows after account creation")?;
    anyhow::ensure!(
        canonical_identity_count == 1,
        "account creation must allocate one canonical primary address and default send identity"
    );

    storage
        .append_audit_event(
            PLATFORM_TENANT_ID,
            audit("test-admin", "admin.audit", "admin drift audit"),
        )
        .await
        .context("append_audit_event")?;

    let dashboard = storage
        .fetch_admin_dashboard()
        .await
        .context("fetch_admin_dashboard")?;
    anyhow::ensure!(
        dashboard
            .domains
            .iter()
            .any(|domain| domain.name == domain_name),
        "created domain was not visible in admin dashboard"
    );
    anyhow::ensure!(
        dashboard
            .accounts
            .iter()
            .any(|account| account.email == account_email),
        "created account was not visible in admin dashboard"
    );
    anyhow::ensure!(
        dashboard
            .audit_log
            .iter()
            .any(|event| event.action == "admin.audit"),
        "admin audit event was not visible in admin dashboard"
    );
    Ok(())
}

async fn seed_mailbox_fixture(pool: &PgPool) -> Result<RuntimeFixture> {
    let unique = Uuid::new_v4().simple().to_string();
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let address_id = Uuid::new_v4();
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
        INSERT INTO account_email_addresses (
            id, tenant_id, account_id, domain_id, email, address_kind, is_primary
        )
        VALUES ($1, $2, $3, $4, $5, 'primary', TRUE)
        "#,
    )
    .bind(address_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(domain_id)
    .bind(&account_email)
    .execute(pool)
    .await
    .context("seed runtime primary account address")?;

    sqlx::query(
        r#"
        INSERT INTO account_identities (
            id, tenant_id, account_id, email_address_id, display_name, may_send, is_default
        )
        VALUES ($1, $2, $3, $4, 'Alice Drift', TRUE, TRUE)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(account_id)
    .bind(address_id)
    .execute(pool)
    .await
    .context("seed runtime default account identity")?;

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
        tenant_id,
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

async fn exercise_notes_journal_reminder_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let note_cursor = storage
        .fetch_jmap_object_change_cursor(fixture.account_id, "Note")
        .await?
        .unwrap_or(0);
    let journal_cursor = storage
        .fetch_jmap_object_change_cursor(fixture.account_id, "JournalEntry")
        .await?
        .unwrap_or(0);
    let note = storage
        .upsert_client_note(UpsertClientNoteInput {
            id: None,
            account_id: fixture.account_id,
            title: "Runtime note".to_string(),
            body_text: "Sticky note body".to_string(),
            color: "yellow".to_string(),
            categories_json: r#"["outlook"]"#.to_string(),
        })
        .await
        .context("create canonical note")?;
    let updated_note = storage
        .upsert_client_note(UpsertClientNoteInput {
            id: Some(note.id),
            account_id: fixture.account_id,
            title: "Runtime note updated".to_string(),
            body_text: "Updated body".to_string(),
            color: "blue".to_string(),
            categories_json: r#"["updated"]"#.to_string(),
        })
        .await
        .context("update canonical note")?;
    anyhow::ensure!(updated_note.title == "Runtime note updated");
    anyhow::ensure!(
        storage
            .fetch_client_notes_by_ids(fixture.account_id, &[note.id])
            .await?
            .len()
            == 1,
        "created note must be readable by the owning account"
    );

    let other_account_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        SELECT $1, tenant_id, primary_domain_id, 'other-' || id::text || '@' || split_part(primary_email, '@', 2), 'Other Runtime'
        FROM accounts
        WHERE id = $2
        "#,
    )
    .bind(other_account_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed second runtime account for isolation")?;
    anyhow::ensure!(
        storage
            .fetch_client_notes_by_ids(other_account_id, &[note.id])
            .await?
            .is_empty(),
        "notes must not cross account boundaries"
    );
    anyhow::ensure!(
        storage
            .upsert_client_note(UpsertClientNoteInput {
                id: Some(note.id),
                account_id: other_account_id,
                title: "Cross-account overwrite".to_string(),
                body_text: "must fail".to_string(),
                color: "blue".to_string(),
                categories_json: "[]".to_string(),
            })
            .await
            .is_err(),
        "notes must reject cross-account id updates"
    );

    let journal = storage
        .upsert_journal_entry(UpsertJournalEntryInput {
            id: None,
            account_id: fixture.account_id,
            subject: "Runtime phone call".to_string(),
            body_text: "Call notes".to_string(),
            entry_type: "phone-call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: Some("2026-05-19T09:00:00Z".to_string()),
            ends_at: Some("2026-05-19T09:10:00Z".to_string()),
            occurred_at: None,
            companies_json: r#"["Contoso"]"#.to_string(),
            contacts_json: r#"["Ada Example"]"#.to_string(),
        })
        .await
        .context("create journal entry")?;
    let updated_journal = storage
        .upsert_journal_entry(UpsertJournalEntryInput {
            id: Some(journal.id),
            account_id: fixture.account_id,
            subject: "Runtime call updated".to_string(),
            body_text: "Updated call notes".to_string(),
            entry_type: "phone-call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: Some("2026-05-19T09:00:00Z".to_string()),
            ends_at: Some("2026-05-19T09:15:00Z".to_string()),
            occurred_at: None,
            companies_json: r#"["Contoso"]"#.to_string(),
            contacts_json: r#"["Ada Example"]"#.to_string(),
        })
        .await
        .context("update journal entry")?;
    anyhow::ensure!(updated_journal.subject == "Runtime call updated");
    anyhow::ensure!(
        storage
            .fetch_journal_entries_by_ids(other_account_id, &[journal.id])
            .await?
            .is_empty(),
        "journal entries must not cross account boundaries"
    );
    anyhow::ensure!(
        storage
            .upsert_journal_entry(UpsertJournalEntryInput {
                id: Some(journal.id),
                account_id: other_account_id,
                subject: "Cross-account overwrite".to_string(),
                body_text: "must fail".to_string(),
                entry_type: "phone-call".to_string(),
                message_class: "IPM.Activity".to_string(),
                starts_at: None,
                ends_at: None,
                occurred_at: None,
                companies_json: "[]".to_string(),
                contacts_json: "[]".to_string(),
            })
            .await
            .is_err(),
        "journal entries must reject cross-account id updates"
    );

    seed_reminder_rows(pool, fixture).await?;
    let active = storage
        .query_client_reminders(
            fixture.account_id,
            ReminderQuery {
                include_inactive: false,
            },
        )
        .await
        .context("query active reminders")?;
    anyhow::ensure!(
        active.iter().any(|reminder| reminder.status == "due"),
        "active reminder query must include due reminders"
    );
    anyhow::ensure!(
        active
            .iter()
            .all(|reminder| reminder.status == "due" || reminder.status == "pending"),
        "active reminder query must exclude dismissed, completed, and excluded reminders"
    );

    let all = storage
        .query_client_reminders(
            fixture.account_id,
            ReminderQuery {
                include_inactive: true,
            },
        )
        .await
        .context("query inactive reminders")?;
    for expected in ["due", "dismissed", "completed", "excluded"] {
        anyhow::ensure!(
            all.iter().any(|reminder| reminder.status == expected),
            "inactive reminder query must include {expected} reminders"
        );
    }
    anyhow::ensure!(
        all.iter()
            .any(|reminder| reminder.title == "Recurring calendar reminder"
                && reminder.occurrence_start_at.is_some()
                && reminder.status == "dismissed"),
        "recurring calendar reminder query must apply occurrence-level dismissal"
    );
    anyhow::ensure!(
        all.iter()
            .any(|reminder| reminder.title == "Recurring task reminder"
                && reminder.occurrence_start_at.is_some()),
        "recurring task reminders must expand into occurrence rows"
    );

    storage
        .delete_client_note(fixture.account_id, note.id)
        .await
        .context("delete note")?;
    storage
        .delete_journal_entry(fixture.account_id, journal.id)
        .await
        .context("delete journal entry")?;
    let note_changes = storage
        .replay_jmap_object_changes(fixture.account_id, "Note", note_cursor, 16)
        .await?
        .context("note replay should be retained")?;
    anyhow::ensure!(
        note_changes
            .iter()
            .any(|change| change.object_id == note.id),
        "note writes must be replayable as JMAP object changes"
    );
    let other_note_changes = storage
        .replay_jmap_object_changes(other_account_id, "Note", note_cursor, 16)
        .await?
        .unwrap_or_default();
    anyhow::ensure!(
        !other_note_changes
            .iter()
            .any(|change| change.object_id == note.id),
        "note replay must not cross account boundaries"
    );
    let journal_changes = storage
        .replay_jmap_object_changes(fixture.account_id, "JournalEntry", journal_cursor, 16)
        .await?
        .context("journal replay should be retained")?;
    anyhow::ensure!(
        journal_changes
            .iter()
            .any(|change| change.object_id == journal.id),
        "journal writes must be replayable as JMAP object changes"
    );
    let other_journal_changes = storage
        .replay_jmap_object_changes(other_account_id, "JournalEntry", journal_cursor, 16)
        .await?
        .unwrap_or_default();
    anyhow::ensure!(
        !other_journal_changes
            .iter()
            .any(|change| change.object_id == journal.id),
        "journal replay must not cross account boundaries"
    );
    Ok(())
}

async fn seed_reminder_rows(pool: &PgPool, fixture: &RuntimeFixture) -> Result<()> {
    let calendar_id = Uuid::new_v4();
    let task_list_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO calendars (id, tenant_id, owner_account_id, display_name, role)
        VALUES ($1, $2, $3, 'Runtime reminders', 'custom')
        "#,
    )
    .bind(calendar_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed reminder calendar")?;
    sqlx::query(
        r#"
        INSERT INTO task_lists (id, tenant_id, owner_account_id, display_name, role)
        VALUES ($1, $2, $3, 'Runtime reminders', 'custom')
        "#,
    )
    .bind(task_list_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed reminder task list")?;
    sqlx::query(
        r#"
        INSERT INTO calendar_events (
            id, tenant_id, owner_account_id, calendar_id, uid, title,
            starts_at, ends_at, recurrence_rule, reminder_set, reminder_at, reminder_dismissed_at, status
        )
        VALUES
            ($1, $5, $6, $7, $1::text, 'Due calendar reminder', NOW(), NOW() + interval '1 hour', '', TRUE, NOW() - interval '10 minutes', NULL, 'confirmed'),
            ($2, $5, $6, $7, $2::text, 'Dismissed calendar reminder', NOW(), NOW() + interval '1 hour', '', TRUE, NOW() - interval '20 minutes', NOW() - interval '5 minutes', 'confirmed'),
            ($3, $5, $6, $7, $3::text, 'Excluded calendar reminder', NOW(), NOW() + interval '1 hour', '', TRUE, NOW() - interval '30 minutes', NULL, 'cancelled'),
            ($4, $5, $6, $7, $4::text, 'No reminder calendar event', NOW(), NOW() + interval '1 hour', '', FALSE, NULL, NULL, 'confirmed'),
            ($8, $5, $6, $7, $8::text, 'Recurring calendar reminder', date_trunc('hour', NOW()) - interval '1 hour', date_trunc('hour', NOW()), 'FREQ=DAILY;COUNT=2;BYDAY=' || upper(to_char(date_trunc('hour', NOW()) - interval '1 hour', 'DY')), TRUE, date_trunc('hour', NOW()) - interval '70 minutes', NULL, 'confirmed')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(calendar_id)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .context("seed calendar reminder rows")?;
    sqlx::query(
        r#"
        INSERT INTO reminder_occurrence_dismissals (
            tenant_id, owner_account_id, source_type, source_id, occurrence_start_at, dismissed_at
        )
        SELECT tenant_id, owner_account_id, 'calendar', id, starts_at, NOW()
        FROM calendar_events
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND title = 'Recurring calendar reminder'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed recurring reminder occurrence dismissal")?;
    sqlx::query(
        r#"
        INSERT INTO tasks (
            id, tenant_id, owner_account_id, task_list_id, uid, title,
            status, due_at, completed_at, recurrence_rule, reminder_set, reminder_at, reminder_dismissed_at
        )
        VALUES
            ($1, $5, $6, $7, $1::text, 'Due task reminder', 'needs-action', NOW() + interval '1 day', NULL, '', TRUE, NOW() - interval '10 minutes', NULL),
            ($2, $5, $6, $7, $2::text, 'Dismissed task reminder', 'needs-action', NOW() + interval '1 day', NULL, '', TRUE, NOW() - interval '20 minutes', NOW() - interval '5 minutes'),
            ($3, $5, $6, $7, $3::text, 'Completed task reminder', 'completed', NOW() + interval '1 day', NOW() - interval '1 minute', '', TRUE, NOW() - interval '30 minutes', NULL),
            ($4, $5, $6, $7, $4::text, 'No reminder task', 'needs-action', NOW() + interval '1 day', NULL, '', FALSE, NULL, NULL),
            ($8, $5, $6, $7, $8::text, 'Recurring task reminder', 'needs-action', date_trunc('hour', NOW()) - interval '1 hour', NULL, 'FREQ=DAILY;COUNT=2', TRUE, date_trunc('hour', NOW()) - interval '70 minutes', NULL)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(task_list_id)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .context("seed task reminder rows")?;
    Ok(())
}

async fn exercise_mailbox_name_policy_storage_guards(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let cafe = storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "Café", None),
            audit("test-admin", "mailbox.create", "storage guard café"),
        )
        .await
        .context("create NFC mailbox through direct JMAP storage API")?;
    anyhow::ensure!(
        cafe.name == "Café",
        "direct JMAP storage create must persist mailbox names in NFC"
    );
    let imap_nfc = storage
        .create_imap_mailbox(
            fixture.account_id,
            "IMAP Cafe\u{301}",
            audit("test-admin", "mailbox.create", "storage guard imap nfc"),
        )
        .await
        .context("create decomposed mailbox through direct IMAP storage API")?;
    anyhow::ensure!(
        imap_nfc.name == "IMAP Café",
        "direct IMAP storage create must persist mailbox names in NFC"
    );

    expect_anyhow_failure(
        "direct JMAP storage create rejects canonical-equivalent sibling",
        storage
            .create_jmap_mailbox(
                jmap_create_input(fixture.account_id, "Cafe\u{301}", None),
                audit(
                    "test-admin",
                    "mailbox.create",
                    "storage guard decomposed café",
                ),
            )
            .await,
    )?;

    let jmap_rename_source = storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "JMAP Rename Source", None),
            audit(
                "test-admin",
                "mailbox.create",
                "storage guard jmap rename source",
            ),
        )
        .await
        .context("create source mailbox for JMAP rename guard")?;
    expect_anyhow_failure(
        "direct JMAP storage rename rejects canonical-equivalent sibling",
        storage
            .update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id: fixture.account_id,
                    mailbox_id: jmap_rename_source.id,
                    name: Some("Cafe\u{301}".to_string()),
                    parent_id: None,
                    sort_order: None,
                    is_subscribed: None,
                },
                audit(
                    "test-admin",
                    "mailbox.update",
                    "storage guard jmap decomposed café",
                ),
            )
            .await,
    )?;

    let imap_rename_source = storage
        .create_imap_mailbox(
            fixture.account_id,
            "IMAP Rename Source",
            audit(
                "test-admin",
                "mailbox.create",
                "storage guard imap rename source",
            ),
        )
        .await
        .context("create source mailbox for IMAP rename guard")?;
    expect_anyhow_failure(
        "direct IMAP storage rename rejects canonical-equivalent sibling",
        storage
            .rename_imap_mailbox(
                fixture.account_id,
                imap_rename_source.id,
                "Cafe\u{301}",
                audit(
                    "test-admin",
                    "mailbox.rename",
                    "storage guard imap decomposed café",
                ),
            )
            .await,
    )?;

    let parent_a = storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "Storage Guard Parent A", None),
            audit("test-admin", "mailbox.create", "storage guard parent a"),
        )
        .await
        .context("create first parent mailbox")?;
    let parent_b = storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "Storage Guard Parent B", None),
            audit("test-admin", "mailbox.create", "storage guard parent b"),
        )
        .await
        .context("create second parent mailbox")?;
    storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "Parent Scoped Café", Some(parent_a.id)),
            audit(
                "test-admin",
                "mailbox.create",
                "storage guard scoped café a",
            ),
        )
        .await
        .context("create first parent-scoped mailbox")?;
    let scoped_sibling = storage
        .create_jmap_mailbox(
            jmap_create_input(
                fixture.account_id,
                "Parent Scoped Cafe\u{301}",
                Some(parent_b.id),
            ),
            audit(
                "test-admin",
                "mailbox.create",
                "storage guard scoped café b",
            ),
        )
        .await
        .context("same NFC display name under a different parent should be allowed")?;
    anyhow::ensure!(
        scoped_sibling.name == "Parent Scoped Café",
        "direct JMAP storage create must normalize child mailbox names to NFC"
    );

    storage
        .create_jmap_mailbox(
            jmap_create_input(fixture.account_id, "paypal", None),
            audit("test-admin", "mailbox.create", "storage guard paypal"),
        )
        .await
        .context("create baseline mailbox for confusable sibling guard")?;
    expect_anyhow_failure(
        "direct JMAP storage create rejects confusable sibling",
        storage
            .create_jmap_mailbox(
                jmap_create_input(
                    fixture.account_id,
                    "\u{440}\u{430}\u{443}\u{440}\u{430}\u{04cf}",
                    None,
                ),
                audit(
                    "test-admin",
                    "mailbox.create",
                    "storage guard confusable paypal",
                ),
            )
            .await,
    )?;

    expect_anyhow_failure(
        "direct JMAP storage rename rejects reserved role spoof",
        storage
            .update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id: fixture.account_id,
                    mailbox_id: jmap_rename_source.id,
                    name: Some("ІNBOX".to_string()),
                    parent_id: None,
                    sort_order: None,
                    is_subscribed: None,
                },
                audit(
                    "test-admin",
                    "mailbox.update",
                    "storage guard reserved spoof",
                ),
            )
            .await,
    )?;

    let stored_decomposed_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mailboxes
        WHERE tenant_id = $1
          AND account_id = $2
          AND display_name LIKE '%' || $3 || '%'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind("\u{301}")
    .fetch_one(pool)
    .await
    .context("count decomposed mailbox display names")?;
    anyhow::ensure!(
        stored_decomposed_count == 0,
        "direct storage APIs must store NFC display_name values"
    );

    Ok(())
}

async fn exercise_change_log_cursor_constraints(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    expect_constraint_failure(
        "mail_change_log rejects mailbox rows without mailbox_id",
        sqlx::query(
            r#"
            INSERT INTO mail_change_log (
                tenant_id, account_id, object_kind, object_id, change_kind,
                modseq, affected_principal_ids, summary_json
            )
            VALUES ($1, $2, 'mailbox', $3, 'updated', 1, ARRAY[$2]::uuid[], '{}'::jsonb)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(fixture.inbox_id)
        .execute(pool)
        .await,
    )?;

    expect_constraint_failure(
        "mail_change_log rejects mailbox_message rows without imapUid replay shape",
        sqlx::query(
            r#"
            INSERT INTO mail_change_log (
                tenant_id, account_id, mailbox_id, object_kind, object_id,
                change_kind, modseq, affected_principal_ids, summary_json
            )
            VALUES (
                $1, $2, $3, 'mailbox_message', $4,
                'updated', 1, ARRAY[$2]::uuid[],
                jsonb_build_object('messageId', $5::text, 'threadId', $6::text)
            )
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(fixture.inbox_id)
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .execute(pool)
        .await,
    )?;

    expect_constraint_failure(
        "MAPI content checkpoint rejects account-wide null mailbox",
        sqlx::query(
            r#"
            INSERT INTO mapi_sync_checkpoints (
                id, tenant_id, account_id, mailbox_id, checkpoint_kind,
                mapi_replica_guid, cursor_json, expires_at
            )
            VALUES ($1, $2, $3, NULL, 'content', $4, '{}'::jsonb, NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(Uuid::new_v4())
        .execute(pool)
        .await,
    )?;

    expect_constraint_failure(
        "MAPI hierarchy checkpoint rejects mailbox-scoped row",
        sqlx::query(
            r#"
            INSERT INTO mapi_sync_checkpoints (
                id, tenant_id, account_id, mailbox_id, checkpoint_kind,
                mapi_replica_guid, cursor_json, expires_at
            )
            VALUES ($1, $2, $3, $4, 'hierarchy', $5, '{}'::jsonb, NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(fixture.inbox_id)
        .bind(Uuid::new_v4())
        .execute(pool)
        .await,
    )?;

    sqlx::query(
        r#"
        INSERT INTO mapi_sync_checkpoints (
            id, tenant_id, account_id, mailbox_id, checkpoint_kind,
            mapi_replica_guid, cursor_json, expires_at
        )
        VALUES ($1, $2, $3, NULL, 'hierarchy', $4, '{}'::jsonb, NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .context("insert valid account-wide MAPI hierarchy checkpoint")?;

    sqlx::query(
        r#"
        INSERT INTO mapi_sync_checkpoints (
            id, tenant_id, account_id, mailbox_id, checkpoint_kind,
            mapi_replica_guid, cursor_json, expires_at
        )
        VALUES ($1, $2, $3, $4, 'content', $5, '{}'::jsonb, NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(fixture.inbox_id)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .context("insert valid mailbox-scoped MAPI content checkpoint")?;

    sqlx::query(
        r#"
        INSERT INTO mapi_sync_checkpoints (
            id, tenant_id, account_id, mailbox_id, checkpoint_kind,
            mapi_replica_guid, cursor_json, expires_at
        )
        VALUES ($1, $2, $3, $4, 'content', $5, '{}'::jsonb, NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(Uuid::parse_str("4c50455f-4d41-5049-0000-000000100001")?)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .context("insert valid virtual-special-folder MAPI content checkpoint")?;

    let expired_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, mailbox_id, object_kind, object_id,
            change_kind, modseq, affected_principal_ids, summary_json,
            created_at, retained_until
        )
        VALUES (
            $1, $2, $3, 'mailbox', $3,
            'destroyed', 1, ARRAY[$2]::uuid[], '{"reason":"expired"}'::jsonb,
            NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day'
        )
        RETURNING cursor
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(fixture.inbox_id)
    .fetch_one(pool)
    .await
    .context("insert expired retained mail_change_log row")?;

    sqlx::query(
        r#"
        INSERT INTO tombstones (
            id, tenant_id, account_id, mailbox_id, object_kind, object_id,
            deleted_modseq, change_cursor, reason, created_at, retained_until
        )
        VALUES (
            $1, $2, $3, $4, 'mailbox', $4,
            1, $5, 'delete', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day'
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(fixture.inbox_id)
    .bind(expired_cursor)
    .execute(pool)
    .await
    .context("insert expired retained tombstone row")?;

    let purged = storage
        .purge_expired_replay_rows()
        .await
        .context("purge_expired_replay_rows")?;
    anyhow::ensure!(
        purged >= 2,
        "expired replay cleanup did not remove tombstone and change-log rows"
    );
    let remaining = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1 AND cursor = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(expired_cursor)
    .fetch_one(pool)
    .await
    .context("count expired retained mail_change_log row after cleanup")?;
    anyhow::ensure!(
        remaining == 0,
        "expired retained mail_change_log row survived cleanup"
    );

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
            audit(
                "alice@example.test",
                "message.submit",
                "runtime drift message",
            ),
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
        let default_emails = storage
            .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
            .await
            .context("fetch_jmap_emails")?;
        anyhow::ensure!(
            default_emails.iter().all(|email| email.bcc.is_empty()),
            "default JMAP fetch must not expose protected Bcc recipients"
        );
        let protected_emails = storage
            .fetch_jmap_emails_with_protected_bcc(fixture.account_id, &[submitted.message_id])
            .await
            .context("fetch_jmap_emails_with_protected_bcc")?;
        anyhow::ensure!(
            protected_emails.iter().any(|email| email
                .bcc
                .iter()
                .any(|recipient| recipient.address == "audit-hidden@example.test")),
            "explicit protected Bcc fetch did not return submitted Bcc recipient"
        );
        let imap_emails = storage
            .fetch_imap_emails(fixture.account_id, submitted.sent_mailbox_id)
            .await
            .context("fetch_imap_emails for submitted sent mailbox")?;
        anyhow::ensure!(
            imap_emails.iter().all(|email| email.bcc.is_empty()),
            "default IMAP fetch must not expose protected Bcc recipients"
        );
        let hidden_query = storage
            .query_jmap_email_ids(fixture.account_id, None, Some("audit-hidden"), 0, 10)
            .await
            .context("query_jmap_email_ids for hidden Bcc recipient")?;
        anyhow::ensure!(
            !hidden_query.ids.contains(&submitted.message_id),
            "JMAP search documents must not match protected Bcc recipients"
        );
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

async fn exercise_index_plan_paths(
    pool: &PgPool,
    fixture: &RuntimeFixture,
    submitted: &SubmittedMessage,
) -> Result<()> {
    let blob_id = Uuid::new_v4();
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .context("disable sequential scans for representative EXPLAIN probes")?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT message_id
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(submitted.message_id)
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN visible mailbox membership lookup")?,
    )?;
    assert_plan_uses_index(
        "visible mailbox membership lookup",
        &plan,
        "mailbox_messages_visible_account_message_idx",
    )?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT s.message_id
            FROM mail_search_documents s
            WHERE s.account_id = $1
              AND s.message_id = $2
            GROUP BY s.message_id
            "#,
        )
        .bind(fixture.account_id)
        .bind(submitted.message_id)
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN JMAP search document lookup")?,
    )?;
    assert_plan_uses_index(
        "JMAP search document lookup",
        &plan,
        "mail_search_documents_account_message_idx",
    )?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT cursor
            FROM mail_change_log
            WHERE tenant_id = $1
              AND account_id = $2
              AND cursor > 0
              AND (retained_until IS NULL OR retained_until > NOW())
            ORDER BY cursor ASC
            LIMIT 20
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN account change replay")?,
    )?;
    assert_plan_uses_index(
        "account change replay",
        &plan,
        "mail_change_log_account_cursor_idx",
    )?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT q.id
            FROM submission_queue q
            WHERE q.status IN ('queued', 'ready', 'deferred')
              AND q.next_attempt_at <= NOW()
            ORDER BY q.created_at ASC, q.id ASC
            LIMIT 20
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN submission worker due queue")?,
    )?;
    assert_plan_uses_index(
        "submission worker due queue",
        &plan,
        "submission_queue_worker_due_idx",
    )?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT 1
            FROM attachment_extraction_jobs
            WHERE tenant_id = $1
              AND blob_id = $2
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(blob_id)
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN attachment extraction blob lookup")?,
    )?;
    assert_plan_uses_index(
        "attachment extraction blob lookup",
        &plan,
        "attachment_extraction_jobs_blob_idx",
    )?;

    let plan = explain_rows(
        sqlx::query(
            r#"
            EXPLAIN SELECT change_cursor
            FROM tombstones
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'mailbox_message'
              AND change_cursor > 0
            ORDER BY change_cursor ASC
            LIMIT 20
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .fetch_all(&mut *tx)
        .await
        .context("EXPLAIN tombstone replay lookup")?,
    )?;
    assert_plan_uses_index("tombstone replay lookup", &plan, "tombstones_account_idx")?;

    tx.rollback().await?;
    Ok(())
}

fn explain_rows(rows: Vec<PgRow>) -> Result<String> {
    rows.into_iter()
        .map(|row| row.try_get::<String, _>(0).map_err(Into::into))
        .collect::<Result<Vec<_>>>()
        .map(|lines| lines.join("\n"))
}

fn assert_plan_uses_index(label: &str, plan: &str, index_name: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains(index_name),
        "{label} did not use {index_name}; plan:\n{plan}"
    );
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
        .store_activesync_device_pending_policy(
            fixture.account_id,
            "runtime-drift-device",
            "phone",
            "12345",
        )
        .await
        .context("store_activesync_device_pending_policy")?;
    storage
        .acknowledge_activesync_device_policy(
            fixture.account_id,
            "runtime-drift-device",
            "phone",
            "67890",
        )
        .await
        .context("acknowledge_activesync_device_policy")?;
    storage
        .fetch_activesync_device(fixture.account_id, "runtime-drift-device")
        .await
        .context("fetch_activesync_device")?;
    storage
        .fetch_activesync_email_states(fixture.account_id, fixture.inbox_id, 0, 10)
        .await
        .context("fetch_activesync_email_states")?;
    Ok(())
}

async fn exercise_pst_path(storage: &Storage, mailbox_id: Uuid) -> Result<()> {
    let output_path = env::temp_dir().join(format!("lpe-runtime-drift-{}.pst", Uuid::new_v4()));
    let output_path_string = output_path.to_string_lossy().to_string();
    storage
        .create_pst_transfer_job(
            NewPstTransferJob {
                mailbox_id,
                direction: "export".to_string(),
                server_path: output_path_string.clone(),
                requested_by: "test-admin".to_string(),
            },
            audit("test-admin", "pst.export", "runtime drift PST export"),
        )
        .await
        .context("create_pst_transfer_job")?;

    let summary = storage
        .process_pending_pst_jobs()
        .await
        .context("process_pending_pst_jobs")?;
    anyhow::ensure!(
        summary.processed_jobs >= 1 && summary.completed_jobs >= 1,
        "PST export job did not complete"
    );
    let exported = std::fs::read_to_string(&output_path)
        .with_context(|| format!("read exported PST smoke file {output_path_string}"))?;
    let _ = std::fs::remove_file(&output_path);
    anyhow::ensure!(
        exported.contains("LPE-PST-V1"),
        "PST export smoke file was missing header"
    );
    Ok(())
}

async fn exercise_admin_dashboard_path(storage: &Storage) -> Result<()> {
    storage
        .fetch_admin_dashboard()
        .await
        .context("fetch_admin_dashboard after mailbox/submission/PST setup")?;
    Ok(())
}

async fn exercise_mailbox_move_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
    submitted: &SubmittedMessage,
) -> Result<()> {
    let target_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order,
            uid_validity, uid_next
        )
        VALUES ($1, $2, $3, 'custom', 'Runtime Move Target', 20, 9001, 42)
        "#,
    )
    .bind(target_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed target mailbox for move semantics")?;

    let source = sqlx::query(
        r#"
        SELECT id, imap_uid
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND visibility = 'visible'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load source membership before move")?;
    let source_membership_id: Uuid = source.try_get("id")?;
    let source_uid: i64 = source.try_get("imap_uid")?;
    let before_cursor = storage
        .fetch_jmap_mail_change_cursor(fixture.account_id)
        .await?
        .unwrap_or(0);
    let before_modseq = i64::try_from(
        storage
            .fetch_imap_highest_modseq(fixture.account_id)
            .await?,
    )
    .context("convert highest modseq before move")?;

    sqlx::query(
        r#"
        INSERT INTO mapi_sync_checkpoints (
            id, tenant_id, account_id, mailbox_id, checkpoint_kind,
            mapi_replica_guid, last_change_sequence, last_modseq,
            cursor_json, expires_at
        )
        VALUES ($1, $2, $3, $4, 'content', $5, $6, $7, '{}'::jsonb, NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(Uuid::new_v4())
    .bind(before_cursor)
    .bind(before_modseq)
    .execute(pool)
    .await
    .context("seed MAPI content checkpoint before move")?;

    storage
        .move_jmap_email(
            fixture.account_id,
            submitted.message_id,
            target_mailbox_id,
            audit("alice@example.test", "message.move", "runtime drift move"),
        )
        .await
        .context("move_jmap_email")?;

    let source_after = sqlx::query(
        r#"
        SELECT visibility, imap_uid
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load source membership after move")?;
    anyhow::ensure!(
        source_after.try_get::<String, _>("visibility")? == "expunged",
        "move must expunge the original source membership"
    );
    anyhow::ensure!(
        source_after.try_get::<i64, _>("imap_uid")? == source_uid,
        "source membership must retain its original IMAP UID"
    );

    let target = sqlx::query(
        r#"
        SELECT id, imap_uid, visibility
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(target_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load target membership after move")?;
    let target_membership_id: Uuid = target.try_get("id")?;
    anyhow::ensure!(
        target_membership_id != source_membership_id,
        "move must create a distinct target membership row"
    );
    anyhow::ensure!(
        target.try_get::<String, _>("visibility")? == "visible",
        "target move membership must be visible"
    );
    anyhow::ensure!(
        target.try_get::<i64, _>("imap_uid")? == 42,
        "target move membership must allocate from target mailbox uid_next"
    );
    anyhow::ensure!(
        storage
            .fetch_imap_mailbox_state(fixture.account_id, target_mailbox_id)
            .await?
            .uid_next
            == 43,
        "target mailbox UIDNEXT must advance after move"
    );

    let source_imap = storage
        .fetch_imap_emails(fixture.account_id, submitted.sent_mailbox_id)
        .await
        .context("fetch source IMAP mailbox after move")?;
    anyhow::ensure!(
        source_imap
            .iter()
            .all(|email| email.id != submitted.message_id),
        "IMAP source mailbox must not list the moved message"
    );
    let target_imap = storage
        .fetch_imap_emails(fixture.account_id, target_mailbox_id)
        .await
        .context("fetch target IMAP mailbox after move")?;
    anyhow::ensure!(
        target_imap
            .iter()
            .any(|email| email.id == submitted.message_id && email.uid == 42),
        "IMAP target mailbox must list the moved message with the target UID"
    );

    let tombstone = sqlx::query(
        r#"
        SELECT imap_uid, reason
        FROM tombstones
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND mailbox_message_id = $4
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load move tombstone")?;
    anyhow::ensure!(
        tombstone.try_get::<i64, _>("imap_uid")? == source_uid
            && tombstone.try_get::<String, _>("reason")? == "move",
        "move tombstone must preserve the original source UID and reason"
    );

    let email_changes = storage
        .replay_jmap_mail_object_changes(fixture.account_id, "Email", before_cursor, 20)
        .await
        .context("replay JMAP Email/changes after move")?
        .context("JMAP Email/changes replay was not retained")?;
    let message_changes = email_changes
        .iter()
        .filter(|change| change.object_id == submitted.message_id)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        message_changes
            .iter()
            .any(|change| change.change_kind == "updated"),
        "JMAP Email/changes must report move as an update to the Email object"
    );
    anyhow::ensure!(
        message_changes
            .iter()
            .all(|change| change.change_kind != "destroyed"),
        "JMAP Email/changes must not report a mailbox move as Email destruction"
    );

    let mailbox_changes = storage
        .replay_jmap_mail_object_changes(fixture.account_id, "Mailbox", before_cursor, 20)
        .await
        .context("replay JMAP Mailbox/changes after move")?
        .context("JMAP Mailbox/changes replay was not retained")?;
    anyhow::ensure!(
        mailbox_changes
            .iter()
            .any(|change| change.object_id == submitted.sent_mailbox_id)
            && mailbox_changes
                .iter()
                .any(|change| change.object_id == target_mailbox_id),
        "JMAP Mailbox/changes must touch both source and target mailboxes"
    );

    let mapi_replay_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND cursor > $3
          AND modseq > $4
          AND object_kind = 'mailbox_message'
          AND change_kind = 'moved'
          AND summary_json ->> 'sourceMailboxMessageId' = $5
          AND summary_json ->> 'targetMailboxMessageId' = $6
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(before_cursor)
    .bind(before_modseq)
    .bind(source_membership_id.to_string())
    .bind(target_membership_id.to_string())
    .fetch_one(pool)
    .await
    .context("query MAPI checkpoint replay change rows")?;
    anyhow::ensure!(
        mapi_replay_count == 1,
        "MAPI checkpoint replay must see exactly one moved membership change after its checkpoint"
    );

    let copied_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order,
            uid_validity, uid_next
        )
        VALUES ($1, $2, $3, 'custom', 'Runtime Copy Target', 30, 9002, 77)
        "#,
    )
    .bind(copied_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed second mailbox for JMAP mailboxIds projection")?;
    storage
        .copy_jmap_email(
            fixture.account_id,
            submitted.message_id,
            copied_mailbox_id,
            audit("alice@example.test", "message.copy", "runtime drift copy"),
        )
        .await
        .context("copy_jmap_email for multi-mailbox projection")?;

    let email = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch_jmap_emails after copy")?
        .into_iter()
        .next()
        .context("copied message missing from JMAP fetch")?;
    anyhow::ensure!(
        email.mailbox_ids.contains(&target_mailbox_id)
            && email.mailbox_ids.contains(&copied_mailbox_id)
            && email.mailbox_ids.len() == 2,
        "JMAP Email must expose all visible mailboxIds for a multi-mailbox message"
    );

    let unscoped_query = storage
        .query_jmap_email_ids(
            fixture.account_id,
            None,
            Some("runtime schema drift"),
            0,
            50,
        )
        .await
        .context("query unscoped JMAP Email ids after copy")?;
    anyhow::ensure!(
        unscoped_query
            .ids
            .iter()
            .filter(|id| **id == submitted.message_id)
            .count()
            == 1,
        "unscoped JMAP Email/query must return one id for one message with multiple memberships"
    );

    for mailbox_id in [target_mailbox_id, copied_mailbox_id] {
        let scoped_query = storage
            .query_jmap_email_ids(
                fixture.account_id,
                Some(mailbox_id),
                Some("runtime schema drift"),
                0,
                50,
            )
            .await
            .with_context(|| format!("query scoped JMAP Email ids for mailbox {mailbox_id}"))?;
        anyhow::ensure!(
            scoped_query.ids.contains(&submitted.message_id),
            "mailbox-scoped JMAP Email/query must return the message in mailbox {mailbox_id}"
        );
    }

    Ok(())
}

async fn exercise_mapi_cross_protocol_interoperability_gate(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "mapi".to_string(),
                from_display: Some("Alice MAPI".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "draft-recipient@example.test".to_string(),
                    display_name: Some("Draft Recipient".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![SubmittedRecipientInput {
                    address: "draft-hidden@example.test".to_string(),
                    display_name: Some("Draft Hidden".to_string()),
                }],
                subject: "MAPI canonical draft gate".to_string(),
                body_text: "MAPI draft canonical body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!("<mapi-draft-{}@example.test>", Uuid::new_v4())),
                mime_blob_ref: None,
                size_octets: 128,
                unread: Some(false),
                flagged: Some(true),
                attachments: Vec::new(),
            },
            audit("alice@example.test", "mapi-save-draft", "MAPI draft gate"),
        )
        .await
        .context("save MAPI-sourced canonical draft")?;
    anyhow::ensure!(
        draft.delivery_status == "draft",
        "MAPI draft save must create canonical draft state"
    );

    let draft_jmap = storage
        .fetch_jmap_emails(fixture.account_id, &[draft.message_id])
        .await
        .context("fetch JMAP projection for MAPI draft")?
        .into_iter()
        .next()
        .context("MAPI draft missing from JMAP projection")?;
    anyhow::ensure!(
        draft_jmap.mailbox_ids == vec![draft.draft_mailbox_id]
            && draft_jmap.mailbox_role == "drafts"
            && draft_jmap.delivery_status == "draft"
            && !draft_jmap.unread
            && draft_jmap.flagged
            && draft_jmap.bcc.is_empty(),
        "JMAP projection must expose canonical MAPI draft state without protected Bcc"
    );

    let draft_imap = storage
        .fetch_imap_emails(fixture.account_id, draft.draft_mailbox_id)
        .await
        .context("fetch IMAP projection for MAPI draft")?
        .into_iter()
        .find(|email| email.id == draft.message_id)
        .context("MAPI draft missing from IMAP Drafts projection")?;
    anyhow::ensure!(
        !draft_imap.unread && draft_imap.flagged && draft_imap.bcc.is_empty(),
        "IMAP projection must expose canonical MAPI draft flags without protected Bcc"
    );

    let draft_submission = storage
        .submit_draft_message(
            fixture.account_id,
            draft.message_id,
            fixture.account_id,
            "mapi",
            audit(
                "alice@example.test",
                "mapi-submit-draft",
                "MAPI draft submit",
            ),
        )
        .await
        .context("submit MAPI-sourced canonical draft")?;
    anyhow::ensure!(
        draft_submission.delivery_status == "queued",
        "MAPI draft submit must use canonical queued submission"
    );

    let old_draft_projection = storage
        .fetch_jmap_emails(fixture.account_id, &[draft.message_id])
        .await
        .context("fetch old draft projection after MAPI submit")?;
    anyhow::ensure!(
        old_draft_projection.is_empty(),
        "MAPI draft submit must remove the source draft from canonical projections"
    );

    let sent_draft_jmap = storage
        .fetch_jmap_emails(fixture.account_id, &[draft_submission.message_id])
        .await
        .context("fetch JMAP projection for MAPI-submitted draft")?
        .into_iter()
        .next()
        .context("MAPI-submitted draft missing from JMAP Sent projection")?;
    anyhow::ensure!(
        sent_draft_jmap.mailbox_ids == vec![draft_submission.sent_mailbox_id]
            && sent_draft_jmap.mailbox_role == "sent"
            && sent_draft_jmap.delivery_status == "queued"
            && sent_draft_jmap.bcc.is_empty(),
        "MAPI draft submit must create authoritative canonical Sent visible through JMAP"
    );

    let submitted = storage
        .submit_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "mapi".to_string(),
                from_display: Some("Alice MAPI".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob Example".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![SubmittedRecipientInput {
                    address: "mapi-hidden@example.test".to_string(),
                    display_name: Some("Hidden MAPI".to_string()),
                }],
                subject: "MAPI interoperability gate".to_string(),
                body_text: "MAPI gate searchable body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!("<mapi-gate-{}@example.test>", Uuid::new_v4())),
                mime_blob_ref: None,
                size_octets: 256,
                unread: Some(false),
                flagged: Some(false),
                attachments: vec![AttachmentUploadInput {
                    file_name: "mapi-gate.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    blob_bytes: b"%PDF-mapi-gate".to_vec(),
                }],
            },
            audit(
                "alice@example.test",
                "mapi-submit-message",
                "MAPI gate submit",
            ),
        )
        .await
        .context("submit MAPI-sourced canonical message")?;

    let queue_protocol = sqlx::query_scalar::<_, String>(
        r#"
        SELECT source_protocol
        FROM submission_queue
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(submitted.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("load MAPI submission source protocol")?;
    anyhow::ensure!(
        queue_protocol == "mapi",
        "MAPI send must use canonical submission_queue source_protocol=mapi"
    );

    let membership = sqlx::query(
        r#"
        SELECT id, imap_uid, modseq, is_seen, is_flagged
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND visibility = 'visible'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load MAPI sent membership")?;
    let sent_membership_id: Uuid = membership.try_get("id")?;
    let sent_uid: i64 = membership.try_get("imap_uid")?;
    let sent_modseq: i64 = membership.try_get("modseq")?;
    let sent_membership_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("count MAPI sent memberships")?;
    anyhow::ensure!(
        sent_membership_count == 1,
        "MAPI canonical submission must create exactly one visible Sent membership"
    );
    anyhow::ensure!(
        membership.try_get::<bool, _>("is_seen")?
            && !membership.try_get::<bool, _>("is_flagged")?,
        "MAPI canonical Sent membership must start with submitted read/flag state"
    );

    let jmap_email = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch JMAP projection for MAPI sent message")?
        .into_iter()
        .next()
        .context("MAPI sent message missing from JMAP projection")?;
    anyhow::ensure!(
        jmap_email.mailbox_ids == vec![submitted.sent_mailbox_id]
            && jmap_email.mailbox_role == "sent"
            && jmap_email.delivery_status == "queued"
            && jmap_email.has_attachments,
        "JMAP projection must expose the single canonical Sent message with queued submission and attachment state"
    );
    anyhow::ensure!(
        jmap_email.bcc.is_empty(),
        "normal JMAP projection must not expose MAPI submitted Bcc recipients"
    );

    let protected_jmap = storage
        .fetch_jmap_emails_with_protected_bcc(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch protected JMAP projection for MAPI sent message")?;
    anyhow::ensure!(
        protected_jmap.iter().any(|email| email
            .bcc
            .iter()
            .any(|recipient| recipient.address == "mapi-hidden@example.test")),
        "explicit protected fetch must retain the MAPI submitted Bcc recipient"
    );

    let imap_email = storage
        .fetch_imap_emails(fixture.account_id, submitted.sent_mailbox_id)
        .await
        .context("fetch IMAP projection for MAPI sent message")?
        .into_iter()
        .find(|email| email.id == submitted.message_id)
        .context("MAPI sent message missing from IMAP Sent projection")?;
    anyhow::ensure!(
        i64::from(imap_email.uid) == sent_uid && i64::try_from(imap_email.modseq)? == sent_modseq,
        "IMAP projection must expose the canonical UID and modseq for the MAPI sent membership"
    );
    anyhow::ensure!(
        imap_email.bcc.is_empty()
            && imap_email.has_attachments
            && imap_email
                .mime_parts
                .iter()
                .any(|part| part.file_name.as_deref() == Some("mapi-gate.pdf")),
        "IMAP projection must hide Bcc while exposing canonical attachment metadata"
    );

    let attachment_blob_status = sqlx::query(
        r#"
        SELECT b.extraction_status, COUNT(j.id) AS job_count
        FROM attachments a
        JOIN blobs b
          ON b.tenant_id = a.tenant_id
         AND b.domain_id = a.domain_id
         AND b.id = a.blob_id
         AND b.blob_kind = a.blob_kind
        LEFT JOIN attachment_extraction_jobs j
          ON j.tenant_id = a.tenant_id
         AND j.blob_id = a.blob_id
         AND j.blob_kind = a.blob_kind
        WHERE a.tenant_id = $1
          AND a.account_id = $2
          AND a.mailbox_message_id = $3
          AND a.message_id = $4
        GROUP BY b.extraction_status
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(sent_membership_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load MAPI sent attachment blob status")?;
    anyhow::ensure!(
        attachment_blob_status.try_get::<String, _>("extraction_status")? == "queued"
            && attachment_blob_status.try_get::<i64, _>("job_count")? == 1,
        "PDF attachment submitted through MAPI must enter the canonical attachment extraction queue"
    );

    let hidden_search = storage
        .query_jmap_email_ids(fixture.account_id, None, Some("mapi-hidden"), 0, 10)
        .await
        .context("query JMAP search for MAPI Bcc recipient")?;
    anyhow::ensure!(
        !hidden_search.ids.contains(&submitted.message_id),
        "MAPI submitted Bcc recipient must not be searchable through JMAP"
    );

    let hidden_ai_projection_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM document_projections
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND source_object_id = $3
          AND (
              participants_visible ILIKE '%mapi-hidden%'
              OR body_text ILIKE '%mapi-hidden%'
              OR preview ILIKE '%mapi-hidden%'
          )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("query AI projections for MAPI Bcc recipient")?;
    anyhow::ensure!(
        hidden_ai_projection_count == 0,
        "AI-facing document projections must not contain MAPI submitted Bcc recipients"
    );

    storage
        .update_jmap_email_flags(
            fixture.account_id,
            submitted.message_id,
            Some(true),
            Some(true),
            audit(
                "alice@example.test",
                "mapi-set-read-flags",
                "MAPI gate flags",
            ),
        )
        .await
        .context("apply MAPI-style flag mutation through canonical store")?;

    let flagged_membership = sqlx::query(
        r#"
        SELECT imap_uid, modseq, is_seen, is_flagged
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(sent_membership_id)
    .fetch_one(pool)
    .await
    .context("load MAPI sent membership after flag mutation")?;
    anyhow::ensure!(
        flagged_membership.try_get::<i64, _>("imap_uid")? == sent_uid
            && flagged_membership.try_get::<i64, _>("modseq")? > sent_modseq
            && !flagged_membership.try_get::<bool, _>("is_seen")?
            && flagged_membership.try_get::<bool, _>("is_flagged")?,
        "MAPI flag mutation must preserve IMAP UID, advance modseq, and update canonical flags"
    );

    let updated_imap = storage
        .fetch_imap_emails(fixture.account_id, submitted.sent_mailbox_id)
        .await
        .context("fetch IMAP projection after MAPI flag mutation")?
        .into_iter()
        .find(|email| email.id == submitted.message_id)
        .context("MAPI sent message missing from IMAP after flag mutation")?;
    anyhow::ensure!(
        i64::from(updated_imap.uid) == sent_uid
            && updated_imap.modseq > u64::try_from(sent_modseq)?
            && updated_imap.unread
            && updated_imap.flagged,
        "IMAP projection must reflect MAPI flag mutation without UID churn"
    );

    let updated_jmap = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch JMAP projection after MAPI flag mutation")?
        .into_iter()
        .next()
        .context("MAPI sent message missing from JMAP after flag mutation")?;
    anyhow::ensure!(
        updated_jmap.unread && updated_jmap.flagged && updated_jmap.bcc.is_empty(),
        "JMAP projection must reflect MAPI flag mutation while still hiding protected Bcc"
    );

    Ok(())
}

async fn exercise_canonical_identity_allocation(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let default_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM account_email_addresses address
        JOIN account_identities identity
          ON identity.tenant_id = address.tenant_id
         AND identity.account_id = address.account_id
         AND identity.email_address_id = address.id
         AND identity.is_default = TRUE
         AND identity.may_send = TRUE
        WHERE address.tenant_id = $1
          AND address.account_id = $2
          AND address.email = $3
          AND address.is_primary = TRUE
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(&fixture.account_email)
    .fetch_one(pool)
    .await
    .context("count fixture primary address/default identity")?;
    anyhow::ensure!(
        default_identity_count == 1,
        "fixture account must have exactly one canonical default send identity"
    );

    let alias_address_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO account_email_addresses (
            id, tenant_id, account_id, domain_id, email, address_kind, is_primary
        )
        SELECT $1, tenant_id, id, primary_domain_id, $4, 'reply_to', FALSE
        FROM accounts
        WHERE tenant_id = $2 AND id = $3
        "#,
    )
    .bind(alias_address_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(format!(
        "reply-{}@{}",
        Uuid::new_v4().simple(),
        fixture
            .account_email
            .split('@')
            .nth(1)
            .unwrap_or("example.test")
    ))
    .execute(pool)
    .await
    .context("seed secondary canonical account address")?;

    expect_constraint_failure(
        "account_identities reject a second default identity for the same account",
        sqlx::query(
            r#"
            INSERT INTO account_identities (
                id, tenant_id, account_id, email_address_id, display_name, may_send, is_default
            )
            VALUES ($1, $2, $3, $4, 'Second Default', TRUE, TRUE)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(alias_address_id)
        .execute(pool)
        .await,
    )?;

    let grantee_id = Uuid::new_v4();
    let grantee_address_id = Uuid::new_v4();
    let domain = fixture
        .account_email
        .split('@')
        .nth(1)
        .context("fixture email missing domain")?;
    let grantee_email = format!("delegate-{}@{domain}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        SELECT $1, tenant_id, primary_domain_id, $3, 'Delegate Drift'
        FROM accounts
        WHERE tenant_id = $2 AND id = $4
        "#,
    )
    .bind(grantee_id)
    .bind(fixture.tenant_id)
    .bind(&grantee_email)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed delegate account")?;
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
    .bind(grantee_address_id)
    .bind(fixture.tenant_id)
    .bind(grantee_id)
    .execute(pool)
    .await
    .context("seed delegate primary address")?;
    sqlx::query(
        r#"
        INSERT INTO account_identities (
            id, tenant_id, account_id, email_address_id, display_name, may_send, is_default
        )
        VALUES ($1, $2, $3, $4, 'Delegate Drift', TRUE, TRUE)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(fixture.tenant_id)
    .bind(grantee_id)
    .bind(grantee_address_id)
    .execute(pool)
    .await
    .context("seed delegate default identity")?;

    storage
        .upsert_sender_delegation_grant(
            SenderDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: grantee_email.clone(),
                sender_right: SenderDelegationRight::SendOnBehalf,
            },
            audit(
                "alice@example.test",
                "identity.delegate",
                "runtime drift sender identity",
            ),
        )
        .await
        .context("grant canonical send-on-behalf right")?;

    let identities = storage
        .fetch_sender_identities(grantee_id, fixture.account_id)
        .await
        .context("fetch delegated sender identities")?;
    anyhow::ensure!(
        identities.iter().any(|identity| {
            identity.owner_account_id == fixture.account_id
                && identity.email == fixture.account_email
                && identity.authorization_kind == "send-on-behalf"
                && identity.sender_address.as_deref() == Some(grantee_email.as_str())
        }),
        "delegated sender identity projection must come from canonical sender_rights and account rows"
    );

    let mapi_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id IN ($2, $3)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(grantee_id)
    .fetch_one(pool)
    .await
    .context("count MAPI identities after non-MAPI sender projection")?;
    anyhow::ensure!(
        mapi_identity_count == 0,
        "canonical sender identity allocation must not create MAPI identity rows"
    );

    Ok(())
}

async fn exercise_canonical_search_folder_and_rule_replay(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    storage
        .ensure_imap_mailboxes(fixture.account_id)
        .await
        .context("ensure canonical mailboxes and search-folder definitions")?;

    let search_folder = sqlx::query(
        r#"
        SELECT sf.id, COUNT(log.cursor) AS change_count
        FROM search_folders sf
        LEFT JOIN mail_change_log log
          ON log.tenant_id = sf.tenant_id
         AND log.account_id = sf.account_id
         AND log.object_kind = 'search_folder_definition'
         AND log.object_id = sf.id
        WHERE sf.tenant_id = $1
          AND sf.account_id = $2
          AND sf.role = 'reminders'
          AND sf.is_builtin = TRUE
        GROUP BY sf.id
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load canonical reminders search-folder definition and change row")?;
    anyhow::ensure!(
        search_folder.try_get::<i64, _>("change_count")? >= 1,
        "search-folder definitions must write canonical object change rows"
    );

    let script_name = format!("runtime-rule-{}", Uuid::new_v4().simple());
    storage
        .put_sieve_script(
            fixture.account_id,
            &script_name,
            r#"require ["fileinto"];
if header :contains "Subject" "runtime-rule" {
    keep;
}"#,
            false,
            audit(
                "alice@example.test",
                "rule.create",
                "runtime drift canonical rule",
            ),
        )
        .await
        .context("create canonical Sieve rule script")?;

    let script_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM sieve_scripts
        WHERE tenant_id = $1 AND account_id = $2 AND name = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(&script_name)
    .fetch_one(pool)
    .await
    .context("load canonical Sieve script id")?;

    let rule_change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'sieve_script'
          AND object_id = $3
          AND change_kind = 'created'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(script_id)
    .fetch_one(pool)
    .await
    .context("count canonical Sieve rule create changes")?;
    anyhow::ensure!(
        rule_change_count == 1,
        "Sieve rule creation must write one canonical rule change"
    );

    storage
        .delete_sieve_script(
            fixture.account_id,
            &script_name,
            audit(
                "alice@example.test",
                "rule.delete",
                "runtime drift canonical rule delete",
            ),
        )
        .await
        .context("delete canonical Sieve rule script")?;

    let tombstone_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM tombstones tombstone
        JOIN mail_change_log log
          ON log.tenant_id = tombstone.tenant_id
         AND log.cursor = tombstone.change_cursor
         AND log.object_kind = tombstone.object_kind
         AND log.object_id = tombstone.object_id
        WHERE tombstone.tenant_id = $1
          AND tombstone.account_id = $2
          AND tombstone.object_kind = 'sieve_script'
          AND tombstone.object_id = $3
          AND log.change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(script_id)
    .fetch_one(pool)
    .await
    .context("count canonical Sieve rule tombstones")?;
    anyhow::ensure!(
        tombstone_count == 1,
        "Sieve rule deletion must write a canonical tombstone joined to its change row"
    );

    Ok(())
}

async fn exercise_mapi_delete_cross_protocol_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
    submitted: &SubmittedMessage,
) -> Result<()> {
    let before_cursor = storage
        .fetch_jmap_mail_change_cursor(fixture.account_id)
        .await?
        .unwrap_or(0);
    let source = sqlx::query(
        r#"
        SELECT id, imap_uid
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND visibility = 'visible'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load source membership before scoped delete")?;
    let source_membership_id: Uuid = source.try_get("id")?;
    let source_uid: i64 = source.try_get("imap_uid")?;

    storage
        .delete_jmap_email_from_mailbox(
            fixture.account_id,
            submitted.sent_mailbox_id,
            submitted.message_id,
            audit(
                "alice@example.test",
                "mapi-delete-message",
                "runtime drift delete",
            ),
        )
        .await
        .context("delete_jmap_email_from_mailbox")?;

    let jmap = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch_jmap_emails after scoped delete")?;
    anyhow::ensure!(
        jmap.is_empty(),
        "JMAP Email/get must not return a message after its final visible membership is deleted"
    );

    let imap = storage
        .fetch_imap_emails(fixture.account_id, submitted.sent_mailbox_id)
        .await
        .context("fetch_imap_emails after scoped delete")?;
    anyhow::ensure!(
        imap.iter().all(|email| email.id != submitted.message_id),
        "IMAP FETCH source mailbox must not list a MAPI-deleted message"
    );

    let deleted_membership = sqlx::query_scalar::<_, String>(
        r#"
        SELECT visibility
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load membership visibility after scoped delete")?;
    anyhow::ensure!(
        deleted_membership == "expunged",
        "MAPI hard delete must expunge the addressed canonical membership"
    );

    let tombstone = sqlx::query(
        r#"
        SELECT imap_uid, reason
        FROM tombstones
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND mailbox_message_id = $4
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load scoped delete tombstone")?;
    anyhow::ensure!(
        tombstone.try_get::<i64, _>("imap_uid")? == source_uid
            && tombstone.try_get::<String, _>("reason")? == "delete",
        "MAPI delete tombstone must preserve source UID and delete reason"
    );

    let email_changes = storage
        .replay_jmap_mail_object_changes(fixture.account_id, "Email", before_cursor, 20)
        .await
        .context("replay JMAP Email/changes after scoped delete")?
        .context("JMAP Email/changes replay was not retained after scoped delete")?;
    anyhow::ensure!(
        email_changes.iter().any(|change| {
            change.object_id == submitted.message_id && change.change_kind == "destroyed"
        }),
        "JMAP Email/changes must export the MAPI delete as Email destruction"
    );

    let mapi_delete_replay_rows = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM tombstones tombstone
        JOIN mail_change_log log
          ON log.tenant_id = tombstone.tenant_id
         AND log.cursor = tombstone.change_cursor
         AND log.object_kind = tombstone.object_kind
         AND log.object_id = tombstone.object_id
        WHERE tombstone.tenant_id = $1
          AND tombstone.account_id = $2
          AND tombstone.mailbox_id = $3
          AND tombstone.message_id = $4
          AND tombstone.change_cursor > $5
          AND log.change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.sent_mailbox_id)
    .bind(submitted.message_id)
    .bind(before_cursor)
    .fetch_one(pool)
    .await
    .context("query MAPI tombstone replay rows after JMAP-visible delete")?;
    anyhow::ensure!(
        mapi_delete_replay_rows == 1,
        "MAPI content sync must be able to export the JMAP-visible delete from canonical tombstones"
    );

    Ok(())
}

async fn exercise_mapi_trash_purge_cross_protocol_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let trash_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mailboxes
        WHERE tenant_id = $1 AND account_id = $2 AND role = 'trash'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load canonical Trash mailbox")?;
    let before_cursor = storage
        .fetch_jmap_mail_change_cursor(fixture.account_id)
        .await?
        .unwrap_or(0);

    let mut message_ids = Vec::new();
    let mut membership_ids = Vec::new();
    for index in 0..2 {
        let imported = storage
            .import_jmap_email(
                JmapImportedEmailInput {
                    account_id: fixture.account_id,
                    submitted_by_account_id: fixture.account_id,
                    mailbox_id: trash_id,
                    source: "mapi-save-message".to_string(),
                    raw_message: None,
                    from_display: Some("Alice Trash".to_string()),
                    from_address: fixture.account_email.clone(),
                    sender_display: None,
                    sender_address: None,
                    to: Vec::new(),
                    cc: Vec::new(),
                    bcc: Vec::new(),
                    subject: format!("Runtime MAPI Trash purge {index}"),
                    body_text: "Trash purge body".to_string(),
                    body_html_sanitized: None,
                    internet_message_id: Some(format!(
                        "<trash-purge-{index}-{}@example.test>",
                        Uuid::new_v4()
                    )),
                    mime_blob_ref: String::new(),
                    size_octets: 64,
                    received_at: None,
                    thread_id: None,
                    attachments: Vec::new(),
                },
                audit(
                    "alice@example.test",
                    "mapi-save-message",
                    "runtime trash purge seed",
                ),
            )
            .await
            .context("seed MAPI-sourced Trash message")?;
        message_ids.push(imported.id);
        let membership_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND message_id = $4
              AND visibility = 'visible'
            LIMIT 1
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(trash_id)
        .bind(imported.id)
        .fetch_one(pool)
        .await
        .context("load seeded Trash membership")?;
        membership_ids.push(membership_id);
    }

    for message_id in &message_ids {
        storage
            .delete_jmap_email_from_mailbox(
                fixture.account_id,
                trash_id,
                *message_id,
                audit(
                    "alice@example.test",
                    "mapi-hard-delete-folder-contents",
                    "runtime trash purge",
                ),
            )
            .await
            .context("hard-delete Trash membership through canonical purge path")?;
    }

    let jmap = storage
        .fetch_jmap_emails(fixture.account_id, &message_ids)
        .await
        .context("fetch JMAP emails after Trash purge")?;
    anyhow::ensure!(
        jmap.is_empty(),
        "JMAP Email/get must not return messages after MAPI Trash purge"
    );
    let imap = storage
        .fetch_imap_emails(fixture.account_id, trash_id)
        .await
        .context("fetch IMAP Trash after purge")?;
    anyhow::ensure!(
        message_ids
            .iter()
            .all(|message_id| imap.iter().all(|email| email.id != *message_id)),
        "IMAP Trash must not list messages after MAPI Trash purge"
    );

    for (message_id, membership_id) in message_ids.iter().zip(membership_ids.iter()) {
        let tombstone_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM tombstones tombstone
            JOIN mail_change_log log
              ON log.tenant_id = tombstone.tenant_id
             AND log.cursor = tombstone.change_cursor
             AND log.object_kind = tombstone.object_kind
             AND log.object_id = tombstone.object_id
            WHERE tombstone.tenant_id = $1
              AND tombstone.account_id = $2
              AND tombstone.mailbox_id = $3
              AND tombstone.mailbox_message_id = $4
              AND tombstone.message_id = $5
              AND tombstone.change_cursor > $6
              AND log.change_kind = 'destroyed'
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(trash_id)
        .bind(*membership_id)
        .bind(*message_id)
        .bind(before_cursor)
        .fetch_one(pool)
        .await
        .context("count Trash purge tombstone replay rows")?;
        anyhow::ensure!(
            tombstone_count == 1,
            "MAPI Trash purge must write one canonical tombstone per purged membership"
        );
    }

    Ok(())
}

async fn exercise_mapi_trash_purge_retention_guard(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let trash_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mailboxes
        WHERE tenant_id = $1 AND account_id = $2 AND role = 'trash'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load canonical Trash mailbox for retention guard")?;
    let imported = storage
        .import_jmap_email(
            JmapImportedEmailInput {
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                mailbox_id: trash_id,
                source: "mapi-save-message".to_string(),
                raw_message: None,
                from_display: Some("Alice Trash".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Runtime MAPI retained Trash purge".to_string(),
                body_text: "Retained Trash purge body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!(
                    "<trash-retained-{}@example.test>",
                    Uuid::new_v4()
                )),
                mime_blob_ref: String::new(),
                size_octets: 64,
                received_at: None,
                thread_id: None,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "mapi-save-message",
                "runtime retained trash purge seed",
            ),
        )
        .await
        .context("seed retained MAPI-sourced Trash message")?;
    let membership_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND visibility = 'visible'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(trash_id)
    .bind(imported.id)
    .fetch_one(pool)
    .await
    .context("load retained Trash membership")?;
    sqlx::query(
        r#"
        UPDATE messages
        SET retained_until = NOW() + INTERVAL '7 days',
            legal_hold = TRUE
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(imported.id)
    .execute(pool)
    .await
    .context("mark Trash message retained and under legal hold")?;

    let result = storage
        .delete_jmap_email_from_mailbox(
            fixture.account_id,
            trash_id,
            imported.id,
            audit(
                "alice@example.test",
                "mapi-hard-delete-folder-contents",
                "runtime retained trash purge",
            ),
        )
        .await;
    anyhow::ensure!(
        result.is_err(),
        "MAPI Trash purge must reject retained or legal-hold messages"
    );
    let visibility = sqlx::query_scalar::<_, String>(
        r#"
        SELECT visibility
        FROM mailbox_messages
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(membership_id)
    .fetch_one(pool)
    .await
    .context("load retained Trash membership visibility")?;
    anyhow::ensure!(
        visibility == "visible",
        "retained Trash membership must remain visible after rejected purge"
    );
    let tombstone_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM tombstones
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_id = $3
          AND message_id = $4
          AND mailbox_message_id = $5
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(trash_id)
    .bind(imported.id)
    .bind(membership_id)
    .fetch_one(pool)
    .await
    .context("count retained Trash purge tombstones")?;
    anyhow::ensure!(
        tombstone_count == 0,
        "rejected retained Trash purge must not write a delete tombstone"
    );

    Ok(())
}

fn audit(actor: &str, action: &str, subject: &str) -> AuditEntryInput {
    AuditEntryInput {
        actor: actor.to_string(),
        action: action.to_string(),
        subject: subject.to_string(),
    }
}
