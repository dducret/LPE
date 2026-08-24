use std::{env, str::FromStr};

use anyhow::{Context, Result};
use lpe_domain::InboundDeliveryRequest;
use lpe_storage::{
    mapi_store_identity::{mapi_store_id, mapi_xid},
    normalize_calendar_meeting_uid, AttachmentUploadInput, AuditEntryInput, CancelSubmissionResult,
    CollaborationGrantInput, CollaborationResourceKind, CreatePublicFolderTreeInput,
    DelegatePreferencesPatch, JmapEmailFollowupUpdate, JmapImportedEmailInput,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, MailboxDelegationGrantInput,
    MailboxFolderDelegationGrantInput, ManagedRetentionFolderCreateInput, NewAccount, NewDomain,
    NewMailbox, NewPstTransferJob, PublicFolderPerUserStatePatch, PublicFolderPermissionInput,
    PublicFolderReplicaInput, ReminderQuery, SenderDelegationGrantInput, SenderDelegationRight,
    Storage, SubmissionMessageCustomPropertyInput, SubmissionSourcePatch, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientEventInput, UpsertClientNoteInput,
    UpsertJournalEntryInput, UpsertPublicFolderItemInput, UpsertSearchFolderInput,
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
            "MAPI local replica range constraints",
            exercise_mapi_local_replica_range_constraints(pool, &fixture).await,
        );

        collect(
            &mut failures,
            "MAPI WLink/configuration FAI fidelity constraints",
            exercise_mapi_outlook_cache_fidelity_constraints(pool, &fixture).await,
        );

        collect(
            &mut failures,
            "change log and cursor constraints",
            exercise_change_log_cursor_constraints(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "MAPI special-folder alias constraints",
            exercise_mapi_special_folder_alias_constraints(pool, &fixture).await,
        );

        collect(
            &mut failures,
            "mailbox SQL path",
            exercise_mailbox_path(&storage, &fixture).await,
        );
        collect(
            &mut failures,
            "inbound MIME canonical body path",
            exercise_inbound_mime_canonical_body_path(&storage, pool, &fixture).await,
        );
        collect(
            &mut failures,
            "inbound calendar meeting response correlation path",
            exercise_inbound_calendar_meeting_response_path(&storage, pool, &fixture).await,
        );
        collect(
            &mut failures,
            "durable MAPI meeting request Processed path",
            exercise_mapi_meeting_request_processed_path(&storage, pool, &fixture).await,
        );
        collect(
            &mut failures,
            "mailbox canonical name storage guards",
            exercise_mailbox_name_policy_storage_guards(&storage, pool, &fixture).await,
        );
        collect(
            &mut failures,
            "managed retention folder SQL path",
            exercise_managed_retention_folder_path(&storage, pool, &fixture).await,
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
                "cross-account JMAP copy Bcc projection",
                exercise_cross_account_jmap_copy_bcc_projection(
                    &storage, pool, &fixture, submitted,
                )
                .await,
            );
        }
        collect(
            &mut failures,
            "submission cancellation SQL path",
            exercise_submission_cancellation_path(&storage, pool, &fixture).await,
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
            "outbound meeting request Event correlation boundary",
            exercise_outbound_meeting_request_correlation(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "atomic submission source claim",
            exercise_atomic_submission_source_claim(&storage, pool, &fixture).await,
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
            "public-folder replica topology SQL path",
            exercise_public_folder_replica_path(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "public-folder permission replay SQL path",
            exercise_public_folder_permission_replay_path(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "public-folder per-user replay SQL path",
            exercise_public_folder_per_user_replay_path(&storage, pool, &fixture).await,
        );

        collect(
            &mut failures,
            "custom calendar grant visibility and replay SQL path",
            exercise_custom_calendar_grant_path(&storage, pool, &fixture).await,
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
        version == "0.5.2-sql",
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
        copy_source_mailbox_id: None,
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

async fn exercise_mapi_local_replica_range_constraints(
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    const FIRST_GLOBAL_COUNTER: i64 = 43;
    const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: i64 = 140_737_454_800_896;

    let replica_guid = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .execute(pool)
    .await
    .context("seed parent MAPI mailbox replica")?;

    sqlx::query(
        r#"
        INSERT INTO mapi_local_replica_id_ranges (
            tenant_id, account_id, replica_guid,
            first_global_counter, end_global_counter_exclusive
        )
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .bind(FIRST_GLOBAL_COUNTER)
    .bind(FIRST_GLOBAL_COUNTER + 1)
    .execute(pool)
    .await
    .context("insert a valid GetLocalReplicaIds reservation")?;

    sqlx::query(
        r#"
        INSERT INTO mapi_local_replica_deleted_ranges (
            tenant_id, account_id, folder_id, replica_guid,
            min_global_counter, max_global_counter
        )
        VALUES ($1, $2, 15, $3, $4, $4)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .bind(FIRST_GLOBAL_COUNTER)
    .execute(pool)
    .await
    .context("insert a valid folder-scoped deleted local-id range")?;

    expect_constraint_failure(
        "local replica reservations reject a counter below the dynamic range",
        sqlx::query(
            r#"
            INSERT INTO mapi_local_replica_id_ranges (
                tenant_id, account_id, replica_guid,
                first_global_counter, end_global_counter_exclusive
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(replica_guid)
        .bind(FIRST_GLOBAL_COUNTER - 1)
        .bind(FIRST_GLOBAL_COUNTER + 1)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "local replica reservations reject the high reserved counter range",
        sqlx::query(
            r#"
            INSERT INTO mapi_local_replica_id_ranges (
                tenant_id, account_id, replica_guid,
                first_global_counter, end_global_counter_exclusive
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(replica_guid)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER - 1)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER + 1)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "deleted local-id ranges reject a counter below the dynamic range",
        sqlx::query(
            r#"
            INSERT INTO mapi_local_replica_deleted_ranges (
                tenant_id, account_id, folder_id, replica_guid,
                min_global_counter, max_global_counter
            )
            VALUES ($1, $2, 16, $3, $4, $5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(replica_guid)
        .bind(FIRST_GLOBAL_COUNTER - 1)
        .bind(FIRST_GLOBAL_COUNTER)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "deleted local-id ranges reject the high reserved counter range",
        sqlx::query(
            r#"
            INSERT INTO mapi_local_replica_deleted_ranges (
                tenant_id, account_id, folder_id, replica_guid,
                min_global_counter, max_global_counter
            )
            VALUES ($1, $2, 17, $3, $4, $5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(replica_guid)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER - 1)
        .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER)
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "local replica ranges require the matching parent replica tuple",
        sqlx::query(
            r#"
            INSERT INTO mapi_local_replica_id_ranges (
                tenant_id, account_id, replica_guid,
                first_global_counter, end_global_counter_exclusive
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(fixture.account_id)
        .bind(Uuid::new_v4())
        .bind(FIRST_GLOBAL_COUNTER + 2)
        .bind(FIRST_GLOBAL_COUNTER + 3)
        .execute(pool)
        .await,
    )?;

    sqlx::query(
        "DELETE FROM mapi_mailbox_replicas WHERE tenant_id = $1 AND account_id = $2 AND replica_guid = $3",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .execute(pool)
    .await
    .context("delete parent MAPI replica to exercise child cascades")?;
    let remaining_children = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM mapi_local_replica_id_ranges
             WHERE tenant_id = $1 AND account_id = $2 AND replica_guid = $3)
          + (SELECT COUNT(*) FROM mapi_local_replica_deleted_ranges
             WHERE tenant_id = $1 AND account_id = $2 AND replica_guid = $3)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .fetch_one(pool)
    .await
    .context("count local replica child rows after parent deletion")?;
    anyhow::ensure!(
        remaining_children == 0,
        "deleting a MAPI mailbox replica must cascade to both local range tables"
    );

    Ok(())
}

async fn exercise_mapi_outlook_cache_fidelity_constraints(
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let first_shortcut_id = Uuid::new_v4();
    let second_shortcut_id = Uuid::new_v4();
    let first_ordinal = vec![0x80, 0x10, 0x20, 0x30, 0x40];
    let second_ordinal = vec![0x80, 0x11];
    let address_book_entry_id = vec![0xA1; 20];
    let address_book_store_entry_id = vec![0xB2; 22];
    let client_id = vec![0xC3; 16];

    for (id, subject, ordinal) in [
        (
            first_shortcut_id,
            "Runtime variable WLink ordinal",
            first_ordinal.as_slice(),
        ),
        (
            second_shortcut_id,
            "Runtime second WLink ordinal",
            second_ordinal.as_slice(),
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO mapi_navigation_shortcuts (
                tenant_id, id, account_id, subject, target_folder_id,
                shortcut_type, flags, save_stamp, section, ordinal,
                calendar_color, address_book_entry_id,
                address_book_store_entry_id, client_id, ro_group_type
            )
            VALUES ($1, $2, $3, $4, 9, 0, 0, 305419896, 3, $5,
                    $6, $7, $8, $9, $10)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(id)
        .bind(fixture.account_id)
        .bind(subject)
        .bind(ordinal)
        .bind((id == first_shortcut_id).then_some(7i32))
        .bind((id == first_shortcut_id).then_some(address_book_entry_id.as_slice()))
        .bind((id == first_shortcut_id).then_some(address_book_store_entry_id.as_slice()))
        .bind((id == first_shortcut_id).then_some(client_id.as_slice()))
        .bind((id == first_shortcut_id).then_some(3i32))
        .execute(pool)
        .await
        .context("insert a WLink with a variable-length binary ordinal")?;
    }

    let persisted = sqlx::query(
        r#"
        SELECT ordinal, calendar_color, address_book_entry_id,
               address_book_store_entry_id, client_id, ro_group_type
        FROM mapi_navigation_shortcuts
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(first_shortcut_id)
    .fetch_one(pool)
    .await
    .context("reload the canonical WLink client properties")?;
    anyhow::ensure!(
        persisted.get::<Vec<u8>, _>("ordinal") == first_ordinal
            && persisted.get::<Option<i32>, _>("calendar_color") == Some(7)
            && persisted.get::<Option<Vec<u8>>, _>("address_book_entry_id")
                == Some(address_book_entry_id)
            && persisted.get::<Option<Vec<u8>>, _>("address_book_store_entry_id")
                == Some(address_book_store_entry_id)
            && persisted.get::<Option<Vec<u8>>, _>("client_id") == Some(client_id)
            && persisted.get::<Option<i32>, _>("ro_group_type") == Some(3),
        "the canonical WLink row must retain the complete client property values"
    );

    let ordered_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mapi_navigation_shortcuts
        WHERE tenant_id = $1 AND account_id = $2 AND id IN ($3, $4)
        ORDER BY ordinal
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(first_shortcut_id)
    .bind(second_shortcut_id)
    .fetch_all(pool)
    .await
    .context("sort variable-length WLink ordinals")?;
    anyhow::ensure!(
        ordered_ids == vec![first_shortcut_id, second_shortcut_id],
        "WLink ordinals must use complete bytea lexicographic ordering"
    );

    expect_constraint_failure(
        "WLink ordinals reject a forbidden trailing zero byte",
        sqlx::query(
            r#"
            INSERT INTO mapi_navigation_shortcuts (
                tenant_id, id, account_id, subject, shortcut_type, ordinal
            )
            VALUES ($1, $2, $3, 'Invalid trailing WLink ordinal', 0, $4)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(Uuid::new_v4())
        .bind(fixture.account_id)
        .bind(vec![0x80, 0x00])
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "WLink CalendarColor rejects values outside the documented range",
        sqlx::query(
            r#"
            INSERT INTO mapi_navigation_shortcuts (
                tenant_id, id, account_id, subject, shortcut_type, ordinal,
                calendar_color
            )
            VALUES ($1, $2, $3, 'Invalid WLink CalendarColor', 0, $4, 15)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(Uuid::new_v4())
        .bind(fixture.account_id)
        .bind(vec![0x80, 0x01])
        .execute(pool)
        .await,
    )?;
    expect_constraint_failure(
        "WLink ROGroupType rejects values outside the documented range",
        sqlx::query(
            r#"
            INSERT INTO mapi_navigation_shortcuts (
                tenant_id, id, account_id, subject, shortcut_type, ordinal,
                ro_group_type
            )
            VALUES ($1, $2, $3, 'Invalid WLink ROGroupType', 0, $4, 5)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(Uuid::new_v4())
        .bind(fixture.account_id)
        .bind(vec![0x80, 0x01])
        .execute(pool)
        .await,
    )?;

    let duplicate_subject = "Runtime duplicate FAI identity";
    for id in [Uuid::new_v4(), Uuid::new_v4()] {
        sqlx::query(
            r#"
            INSERT INTO mapi_associated_config_messages (
                tenant_id, id, account_id, folder_id, message_class, subject
            )
            VALUES ($1, $2, $3, 10, 'IPM.Configuration.Views', $4)
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(id)
        .bind(fixture.account_id)
        .bind(duplicate_subject)
        .execute(pool)
        .await
        .context("insert distinct FAI identities with the same logical labels")?;
    }

    Ok(())
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

async fn exercise_inbound_mime_canonical_body_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let trace_id = format!("runtime-inbound-{}", Uuid::new_v4());
    let raw_message = format!(
        concat!(
            "From: \"Header, Author\" <header-author@example.test>, Secondary Author <secondary@example.test>\r\n",
            "Sender: Transport Agent <header-sender@example.test>\r\n",
            "To: {}\r\n",
            "Subject: Re: Test 10:57\r\n",
            "Message-ID: <{}@example.test>\r\n",
            "Content-Type: text/plain; charset=\"iso-8859-1\"\r\n",
            "Content-Transfer-Encoding: quoted-printable\r\n",
            "\r\n",
            "Test r=E9ussi 10:58\r\n"
        ),
        fixture.account_email, trace_id
    )
    .into_bytes();

    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "smtp-envelope@example.test".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Re: Test 10:57".to_string(),
            body_text: "Test r\u{fffd}ussi 10:58".to_string(),
            internet_message_id: Some(format!("<{trace_id}@example.test>")),
            raw_message,
        })
        .await
        .context("deliver inbound ISO-8859-1 MIME fixture")?;

    let stored = sqlx::query(
        r#"
        SELECT b.message_id, b.body_text
        FROM message_bodies b
        JOIN message_headers h
          ON h.tenant_id = b.tenant_id
         AND h.message_id = b.message_id
        WHERE h.tenant_id = $1
          AND lower(h.header_name) = 'x-lpe-ct-trace-id'
          AND h.header_value = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(&trace_id)
    .fetch_one(pool)
    .await
    .context("load canonical inbound message body")?;
    let message_id = stored.try_get::<Uuid, _>("message_id")?;
    let stored_body = stored.try_get::<String, _>("body_text")?;

    anyhow::ensure!(
        stored_body == "Test réussi 10:58",
        "core trusted the edge body projection instead of raw MIME: {stored_body:?}"
    );

    let email = storage
        .fetch_jmap_emails(fixture.account_id, &[message_id])
        .await
        .context("fetch ordinary inbound message through JMAP projection")?
        .into_iter()
        .next()
        .context("ordinary inbound message missing from JMAP projection")?;
    anyhow::ensure!(
        email.from_address == "header-author@example.test"
            && email.from_display.as_deref() == Some("Header, Author")
            && email.sender_address.as_deref() == Some("header-sender@example.test")
            && email.sender_display.as_deref() == Some("Transport Agent")
            && email.sender_authorization_kind == "external",
        "JMAP projection did not preserve distinct RFC From/Sender independently of SMTP MAIL FROM"
    );
    Ok(())
}

async fn exercise_inbound_calendar_meeting_response_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    storage
        .fetch_accessible_calendar_collections(fixture.account_id)
        .await
        .context("ensure organizer default calendar")?;
    let encoded_uid = "040000008200E00074C5B7101A82E00800000000C08470CD9E31DD01000000000000000010000000ECFF8AEC00CE584390F914BF6A87F955";
    let uid = format!("MAPI-GOID:{encoded_uid}");
    let response_uid = format!(
        "{}{}",
        &encoded_uid[..32],
        encoded_uid[32..].to_ascii_lowercase()
    );
    let mut input = runtime_calendar_event_input(
        fixture.account_id,
        None,
        "Inbound meeting response correlation",
    );
    input.uid = uid.to_string();
    input.date = "2026-08-24".to_string();
    input.time = "06:30".to_string();
    input.duration_minutes = 30;
    input.sequence = 2;
    input.attendees = "Denis Ducret".to_string();
    let organizer_attendees_json = serde_json::json!({
        "organizer": {
            "email": fixture.account_email.clone(),
            "common_name": "Alice Drift"
        },
        "attendees": [{
            "email": "denis.ducret@sdic.ch",
            "common_name": "Denis Ducret",
            "role": "REQ-PARTICIPANT",
            "partstat": "needs-action",
            "rsvp": true
        }]
    })
    .to_string();
    input.attendees_json = organizer_attendees_json.clone();
    let event = storage
        .create_accessible_event(fixture.account_id, None, input)
        .await
        .context("create organizer event for inbound meeting response")?;
    anyhow::ensure!(
        event.uid == normalize_calendar_meeting_uid(&uid),
        "calendar Event writers must canonicalize MAPI GlobalObjectId UIDs"
    );

    let trace_id = format!("runtime-counter-{}", Uuid::new_v4());
    let raw_message = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "Sender: Calendar Relay <calendar-relay@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: New Time Proposed: Inbound meeting response correlation\r\n",
            "Message-ID: <{}@sdic.ch>\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/alternative; boundary=counter-boundary\r\n",
            "\r\n",
            "--counter-boundary\r\n",
            "Content-Type: text/plain; charset=UTF-8\r\n",
            "\r\n",
            "Denis proposes a new time.\r\n",
            "--counter-boundary\r\n",
            "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:COUNTER\r\n",
            "VERSION:2.0\r\n",
            "BEGIN:VTIMEZONE\r\n",
            "TZID:Greenwich Standard Time\r\n",
            "BEGIN:STANDARD\r\n",
            "TZOFFSETTO:+0000\r\n",
            "END:STANDARD\r\n",
            "END:VTIMEZONE\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=TENTATIVE;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART;TZID=Greenwich Standard Time:20260824T063000\r\n",
            "DTEND;TZID=Greenwich Standard Time:20260824T073000\r\n",
            "X-MS-OLK-ORIGINALSTART;TZID=Greenwich Standard Time:20260824T063000\r\n",
            "X-MS-OLK-ORIGINALEND;TZID=Greenwich Standard Time:20260824T070000\r\n",
            "SEQUENCE:2\r\n",
            "DTSTAMP:20260824T060000Z\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n",
            "--counter-boundary--\r\n"
        ),
        fixture.account_email, trace_id, response_uid
    )
    .into_bytes();
    let wrong_organizer_message = String::from_utf8(raw_message.clone())?
        .replacen(
            "BEGIN:VEVENT\r\n",
            "BEGIN:VEVENT\r\nORGANIZER:mailto:other-organizer@example.test\r\n",
            1,
        )
        .into_bytes();
    for (case_name, rejected_mail_from, rejected_raw_message, delete_classification) in [
        (
            "sender-mismatch",
            "other-attendee@example.test",
            raw_message.clone(),
            true,
        ),
        (
            "organizer-mismatch",
            "denis.ducret@sdic.ch",
            wrong_organizer_message,
            false,
        ),
    ] {
        let rejected_trace_id = format!("runtime-counter-{case_name}-{}", Uuid::new_v4());
        storage
            .deliver_inbound_message(InboundDeliveryRequest {
                trace_id: rejected_trace_id.clone(),
                peer: "192.0.2.10:25".to_string(),
                helo: "mx.example.test".to_string(),
                mail_from: rejected_mail_from.to_string(),
                rcpt_to: vec![fixture.account_email.clone()],
                subject: "Rejected inbound meeting response".to_string(),
                body_text: "This response must remain ordinary mail.".to_string(),
                internet_message_id: None,
                raw_message: rejected_raw_message,
            })
            .await
            .with_context(|| format!("deliver rejected {case_name} meeting response"))?;
        let rejected = sqlx::query(
            r#"
            SELECT message.id, message.authorized_calendar_response_content_sha256,
                   classification.classification
            FROM message_headers trace
            JOIN messages message
              ON message.tenant_id = trace.tenant_id
             AND message.id = trace.message_id
            JOIN calendar_mail_classifications classification
              ON classification.tenant_id = message.tenant_id
             AND classification.message_id = message.id
            WHERE trace.tenant_id = $1
              AND lower(trace.header_name) = 'x-lpe-ct-trace-id'
              AND trace.header_value = $2
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(&rejected_trace_id)
        .fetch_one(pool)
        .await?;
        let rejected_message_id: Uuid = rejected.try_get("id")?;
        anyhow::ensure!(
            rejected.try_get::<String, _>("classification")? == "none"
                && rejected
                    .try_get::<Option<String>, _>("authorized_calendar_response_content_sha256",)?
                    .is_none(),
            "rejected {case_name} response retained actionable authorization"
        );
        let rejected_outcome =
            meeting_response_outcome_for_trace(pool, fixture.tenant_id, &rejected_trace_id).await?;
        if case_name == "sender-mismatch" {
            anyhow::ensure!(
                rejected_outcome.is_none(),
                "sender-authentication failures must not create response outcome audits"
            );
        } else {
            anyhow::ensure!(
                rejected_outcome
                    == Some((
                        "calendar.meeting-response.ignored-organizer-mismatch".to_string(),
                        false,
                    )),
                "organizer mismatch did not record the bounded unprocessed outcome"
            );
        }
        if delete_classification {
            sqlx::query(
                "DELETE FROM calendar_mail_classifications WHERE tenant_id = $1 AND message_id = $2",
            )
            .bind(fixture.tenant_id)
            .bind(rejected_message_id)
            .execute(pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                UPDATE calendar_mail_classifications
                SET needs_reclassification = TRUE,
                    scheduling_mime_part_id = NULL,
                    updated_at = NOW()
                WHERE tenant_id = $1 AND message_id = $2
                "#,
            )
            .bind(fixture.tenant_id)
            .bind(rejected_message_id)
            .execute(pool)
            .await?;
        }
        let rejected_email = storage
            .fetch_jmap_emails(fixture.account_id, &[rejected_message_id])
            .await?
            .into_iter()
            .next()
            .context("load rejected response after classification repair")?;
        anyhow::ensure!(
            rejected_email.calendar_meeting_response.is_none()
                && rejected_email.calendar_meeting_request.is_none(),
            "lazy repair promoted rejected {case_name} response to actionable mail"
        );
    }
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "New Time Proposed: Inbound meeting response correlation".to_string(),
            body_text: "Denis proposes a new time.".to_string(),
            internet_message_id: None,
            raw_message,
        })
        .await
        .context("deliver inbound COUNTER response")?;
    let applied_counter_outcome =
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &trace_id).await?;
    anyhow::ensure!(
        applied_counter_outcome == Some(("calendar.meeting-response.applied".to_string(), true,)),
        "applied COUNTER did not record the bounded processed outcome: {applied_counter_outcome:?}"
    );

    let stored_identity = sqlx::query(
        r#"
        SELECT
            sender_from.address AS from_address,
            sender_from.display_name AS from_display,
            transport_sender.address AS sender_address,
            transport_sender.display_name AS sender_display
        FROM message_headers trace
        JOIN message_recipients sender_from
          ON sender_from.tenant_id = trace.tenant_id
         AND sender_from.message_id = trace.message_id
         AND sender_from.role = 'from'
        JOIN message_recipients transport_sender
          ON transport_sender.tenant_id = trace.tenant_id
         AND transport_sender.message_id = trace.message_id
         AND transport_sender.role = 'sender'
        WHERE trace.tenant_id = $1
          AND lower(trace.header_name) = 'x-lpe-ct-trace-id'
          AND trace.header_value = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(&trace_id)
    .fetch_one(pool)
    .await
    .context("load preserved inbound From and Sender identities")?;
    anyhow::ensure!(
        stored_identity.try_get::<String, _>("from_address")? == "denis.ducret@sdic.ch"
            && stored_identity
                .try_get::<Option<String>, _>("from_display")?
                .as_deref()
                == Some("Denis Ducret")
            && stored_identity.try_get::<String, _>("sender_address")? == "calendar-relay@sdic.ch"
            && stored_identity
                .try_get::<Option<String>, _>("sender_display")?
                .as_deref()
                == Some("Calendar Relay"),
        "inbound RFC From and distinct Sender identities were not preserved"
    );

    let classification = sqlx::query(
        r#"
        SELECT
            classification.message_id,
            classification.classification,
            classification.parser_revision,
            classification.classification_generation,
            classification.needs_reclassification,
            classification.scheduling_mime_part_id,
            message.authorized_calendar_response_content_sha256,
            message.calendar_response_processed,
            projection.applied_generation,
            part.is_scheduling_body,
            part.content_disposition AS mime_disposition,
            attachment.disposition AS attachment_disposition,
            attachment.id AS attachment_id,
            blob.content_sha256 AS selected_content_sha256
        FROM message_headers trace
        JOIN messages message
          ON message.tenant_id = trace.tenant_id
         AND message.id = trace.message_id
        JOIN calendar_mail_classifications classification
          ON classification.tenant_id = trace.tenant_id
         AND classification.message_id = trace.message_id
        JOIN mime_parts part
          ON part.tenant_id = classification.tenant_id
         AND part.message_id = classification.message_id
         AND part.id = classification.scheduling_mime_part_id
        JOIN calendar_mail_classification_projections projection
          ON projection.tenant_id = classification.tenant_id
         AND projection.account_id = $3
         AND projection.message_id = classification.message_id
        JOIN attachments attachment
          ON attachment.tenant_id = classification.tenant_id
         AND attachment.account_id = $3
         AND attachment.message_id = classification.message_id
         AND attachment.mime_part_id = classification.scheduling_mime_part_id
        JOIN blobs blob
          ON blob.tenant_id = part.tenant_id
         AND blob.domain_id = part.domain_id
         AND blob.id = part.blob_id
         AND blob.blob_kind = part.blob_kind
        WHERE trace.tenant_id = $1
          AND lower(trace.header_name) = 'x-lpe-ct-trace-id'
          AND trace.header_value = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(&trace_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load eager inbound calendar classification")?;
    anyhow::ensure!(
        classification.try_get::<String, _>("classification")? == "response"
            && classification.try_get::<i32, _>("parser_revision")? > 0
            && classification.try_get::<i64, _>("classification_generation")?
                == classification.try_get::<i64, _>("applied_generation")?
            && !classification.try_get::<bool, _>("needs_reclassification")?
            && classification
                .try_get::<Option<Uuid>, _>("scheduling_mime_part_id")?
                .is_some()
            && classification.try_get::<bool, _>("is_scheduling_body")?
            && classification.try_get::<bool, _>("calendar_response_processed")?
            && classification
                .try_get::<Option<String>, _>("authorized_calendar_response_content_sha256",)?
                == Some(classification.try_get::<String, _>("selected_content_sha256")?)
            && classification
                .try_get::<Option<String>, _>("mime_disposition")?
                .is_none()
            && classification.try_get::<String, _>("attachment_disposition")? == "inline",
        "inbound meeting response classification was not persisted against its exact MIME part"
    );

    let response_message_id: Uuid = classification.try_get("message_id")?;
    let response_attachment_id: Uuid = classification.try_get("attachment_id")?;
    let response_file_reference =
        format!("attachment:{response_message_id}:{response_attachment_id}");
    expect_constraint_failure(
        "calendar classification rejects a request metadata object without a request payload",
        sqlx::query(
            r#"
            UPDATE calendar_mail_classifications
            SET classification = 'request',
                metadata_json = '{"kind":"request"}'::jsonb
            WHERE tenant_id = $1 AND message_id = $2
            "#,
        )
        .bind(fixture.tenant_id)
        .bind(response_message_id)
        .execute(pool)
        .await,
    )?;
    let modseq_before_repair = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT MAX(modseq)
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM calendar_mail_classifications
        WHERE tenant_id = $1 AND message_id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(response_message_id)
    .execute(pool)
    .await?;
    let repaired_email = storage
        .fetch_jmap_emails(fixture.account_id, &[response_message_id])
        .await?
        .into_iter()
        .next()
        .context("load lazily repaired meeting response")?;
    anyhow::ensure!(
        repaired_email
            .calendar_meeting_response
            .as_ref()
            .is_some_and(|response| response.server_processed),
        "lazy classification repair did not restore the authorized processed meeting response"
    );
    let repaired = sqlx::query(
        r#"
        SELECT classification.classification_generation,
               projection.applied_generation,
               MAX(membership.modseq) AS modseq
        FROM calendar_mail_classifications classification
        JOIN calendar_mail_classification_projections projection
          ON projection.tenant_id = classification.tenant_id
         AND projection.account_id = $2
         AND projection.message_id = classification.message_id
        JOIN mailbox_messages membership
          ON membership.tenant_id = classification.tenant_id
         AND membership.account_id = $2
         AND membership.message_id = classification.message_id
         AND membership.visibility = 'visible'
        WHERE classification.tenant_id = $1
          AND classification.message_id = $3
        GROUP BY classification.classification_generation,
                 projection.applied_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let repaired_modseq: i64 = repaired.try_get("modseq")?;
    anyhow::ensure!(
        repaired.try_get::<i64, _>("classification_generation")?
            == repaired.try_get::<i64, _>("applied_generation")?
            && repaired_modseq > modseq_before_repair,
        "lazy actionable classification repair was not versioned and acknowledged"
    );
    storage
        .fetch_jmap_emails(fixture.account_id, &[response_message_id])
        .await?;
    let second_fetch_modseq = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT MAX(modseq)
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        second_fetch_modseq == repaired_modseq,
        "an already-applied classification generation rotated the message twice"
    );

    let target_account_id = Uuid::new_v4();
    let target_mailbox_id = Uuid::new_v4();
    let domain_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT primary_domain_id FROM accounts WHERE tenant_id = $1 AND id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Calendar classification copy target')
        "#,
    )
    .bind(target_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(format!(
        "classification-copy-{}@example.test",
        Uuid::new_v4().simple()
    ))
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 1)
        "#,
    )
    .bind(target_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await?;
    storage
        .copy_jmap_email_between_accounts(
            fixture.account_id,
            target_account_id,
            response_message_id,
            target_mailbox_id,
            audit(
                "alice@example.test",
                "calendar-classification-copy",
                "runtime all-account calendar projection",
            ),
        )
        .await?;
    let original_target_membership = sqlx::query(
        r#"
        SELECT id, modseq
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        ORDER BY id
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let original_target_membership_id: Uuid = original_target_membership.try_get("id")?;
    let original_target_modseq: i64 = original_target_membership.try_get("modseq")?;
    let pending_copy_generation = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE calendar_mail_classifications
        SET classification_generation = classification_generation + 1,
            requires_projection_rotation = TRUE,
            updated_at = NOW()
        WHERE tenant_id = $1 AND message_id = $2
        RETURNING classification_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let second_target_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'custom', 'Meeting Archive', 1, 2)
        "#,
    )
    .bind(second_target_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await?;
    storage
        .copy_jmap_email(
            target_account_id,
            response_message_id,
            second_target_mailbox_id,
            audit(
                "alice@example.test",
                "calendar-classification-second-copy",
                "runtime pending generation copy",
            ),
        )
        .await?;
    let pending_copy_result = sqlx::query(
        r#"
        SELECT projection.applied_generation,
               source_projection.applied_generation AS source_generation,
               membership.modseq
        FROM calendar_mail_classification_projections projection
        JOIN calendar_mail_classification_projections source_projection
          ON source_projection.tenant_id = projection.tenant_id
         AND source_projection.account_id = $5
         AND source_projection.message_id = projection.message_id
        JOIN mailbox_messages membership
          ON membership.tenant_id = projection.tenant_id
         AND membership.account_id = projection.account_id
         AND membership.message_id = projection.message_id
         AND membership.id = $4
        WHERE projection.tenant_id = $1
          AND projection.account_id = $2
          AND projection.message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .bind(original_target_membership_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        pending_copy_result.try_get::<i64, _>("applied_generation")?
            == pending_copy_generation
            && pending_copy_result.try_get::<i64, _>("source_generation")?
                == pending_copy_generation
            && pending_copy_result.try_get::<i64, _>("modseq")? > original_target_modseq,
        "a second mailbox copy acknowledged a pending generation before rotating the existing membership"
    );
    let target_modseq_before_invalidation = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT MAX(modseq)
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let shared_attachment_delete = storage
        .delete_message_attachment(
            fixture.account_id,
            &response_file_reference,
            audit(
                "alice@example.test",
                "calendar-transport-delete",
                "runtime actionable-to-none invalidation",
            ),
        )
        .await;
    anyhow::ensure!(
        shared_attachment_delete.is_err(),
        "shared scheduling content must not be mutated while visible in another account"
    );
    let invalidated_part = sqlx::query(
        r#"
        WITH selected AS (
            UPDATE mime_parts part
            SET is_scheduling_body = FALSE
            FROM attachments attachment
            WHERE part.tenant_id = $1
              AND part.message_id = $2
              AND attachment.tenant_id = part.tenant_id
              AND attachment.message_id = part.message_id
              AND attachment.mime_part_id = part.id
              AND attachment.id = $3
            RETURNING part.id
        )
        UPDATE calendar_mail_classifications classification
        SET needs_reclassification = TRUE,
            scheduling_mime_part_id = NULL,
            updated_at = NOW()
        WHERE classification.tenant_id = $1
          AND classification.message_id = $2
          AND classification.scheduling_mime_part_id = (SELECT id FROM selected)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(response_message_id)
    .bind(response_attachment_id)
    .execute(pool)
    .await
    .context("simulate legacy scheduling-role drift for actionable-to-none repair")?;
    anyhow::ensure!(
        invalidated_part.rows_affected() == 1,
        "legacy scheduling-role drift must invalidate exactly one selected MIME part"
    );
    let target_email = storage
        .fetch_jmap_emails(target_account_id, &[response_message_id])
        .await?
        .into_iter()
        .next()
        .context("load copied email after scheduling-part deletion")?;
    anyhow::ensure!(
        target_email.calendar_meeting_response.is_none()
            && target_email.calendar_meeting_request.is_none(),
        "actionable-to-none repair left stale meeting metadata in the copied account"
    );
    let invalidated = sqlx::query(
        r#"
        SELECT classification.classification,
               classification.classification_generation,
               source_projection.applied_generation AS source_generation,
               target_projection.applied_generation AS target_generation,
               MAX(target_membership.modseq) AS target_modseq
        FROM calendar_mail_classifications classification
        JOIN calendar_mail_classification_projections source_projection
          ON source_projection.tenant_id = classification.tenant_id
         AND source_projection.account_id = $2
         AND source_projection.message_id = classification.message_id
        JOIN calendar_mail_classification_projections target_projection
          ON target_projection.tenant_id = classification.tenant_id
         AND target_projection.account_id = $3
         AND target_projection.message_id = classification.message_id
        JOIN mailbox_messages target_membership
          ON target_membership.tenant_id = classification.tenant_id
         AND target_membership.account_id = $3
         AND target_membership.message_id = classification.message_id
         AND target_membership.visibility = 'visible'
        WHERE classification.tenant_id = $1
          AND classification.message_id = $4
        GROUP BY classification.classification,
                 classification.classification_generation,
                 source_projection.applied_generation,
                 target_projection.applied_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let invalidated_generation: i64 = invalidated.try_get("classification_generation")?;
    anyhow::ensure!(
        invalidated.try_get::<String, _>("classification")? == "none"
            && invalidated_generation > repaired.try_get::<i64, _>("classification_generation")?
            && invalidated.try_get::<i64, _>("source_generation")? == invalidated_generation
            && invalidated.try_get::<i64, _>("target_generation")? == invalidated_generation
            && invalidated.try_get::<i64, _>("target_modseq")? > target_modseq_before_invalidation,
        "actionable-to-none repair did not rotate and acknowledge every visible account"
    );

    sqlx::query(
        r#"
        UPDATE mailbox_messages
        SET visibility = 'expunged', expunged_at = NOW(), updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .execute(pool)
    .await?;
    let invisible_target_modseq = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT current_modseq
        FROM account_sync_state
        WHERE tenant_id = $1 AND account_id = $2 AND category = 'mail'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .fetch_one(pool)
    .await?;
    let recopy_generation = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE calendar_mail_classifications
        SET classification_generation = classification_generation + 1,
            requires_projection_rotation = TRUE,
            updated_at = NOW()
        WHERE tenant_id = $1 AND message_id = $2
        RETURNING classification_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    let recopy_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'custom', 'Meeting Recopy', 2, 3)
        "#,
    )
    .bind(recopy_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await?;
    storage
        .copy_jmap_email_between_accounts(
            fixture.account_id,
            target_account_id,
            response_message_id,
            recopy_mailbox_id,
            audit(
                "alice@example.test",
                "calendar-classification-recopy",
                "runtime persisted invisible projection",
            ),
        )
        .await?;
    let recopy_state = sqlx::query(
        r#"
        SELECT sync.current_modseq, projection.applied_generation,
               COUNT(membership.id) AS visible_memberships
        FROM account_sync_state sync
        JOIN calendar_mail_classification_projections projection
          ON projection.tenant_id = sync.tenant_id
         AND projection.account_id = sync.account_id
         AND projection.message_id = $3
        LEFT JOIN mailbox_messages membership
          ON membership.tenant_id = sync.tenant_id
         AND membership.account_id = sync.account_id
         AND membership.message_id = $3
         AND membership.visibility = 'visible'
        WHERE sync.tenant_id = $1
          AND sync.account_id = $2
          AND sync.category = 'mail'
        GROUP BY sync.current_modseq, projection.applied_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(response_message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        recopy_state.try_get::<i64, _>("applied_generation")? == recopy_generation
            && recopy_state.try_get::<i64, _>("visible_memberships")? == 1
            && recopy_state.try_get::<i64, _>("current_modseq")? >= invisible_target_modseq + 2,
        "recopy over an invisible persisted projection suppressed its pending generation rotation"
    );

    let row = sqlx::query(
        r#"
        SELECT
            to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS starts_at,
            to_char(ends_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ends_at,
            attendees_json #>> '{attendees,0,partstat}' AS partstat,
            attendees_json #>> '{attendees,0,counter_proposal}' AS counter_proposal,
            attendees_json #>> '{attendees,0,proposed_start}' AS proposed_start,
            attendees_json #>> '{attendees,0,proposed_end}' AS proposed_end
        FROM calendar_events
        WHERE id = $1
        "#,
    )
    .bind(event.id)
    .fetch_one(pool)
    .await
    .context("load correlated organizer event")?;
    anyhow::ensure!(
        row.try_get::<String, _>("starts_at")? == "2026-08-24T06:30:00Z"
            && row.try_get::<String, _>("ends_at")? == "2026-08-24T07:00:00Z",
        "counter response must not change the organizer's scheduled time"
    );
    anyhow::ensure!(
        row.try_get::<String, _>("partstat")? == "tentative"
            && row.try_get::<String, _>("counter_proposal")? == "true"
            && row.try_get::<String, _>("proposed_start")? == "2026-08-24T06:30:00Z"
            && row.try_get::<String, _>("proposed_end")? == "2026-08-24T07:30:00Z",
        "counter response was not persisted against the matching attendee"
    );

    let reply_trace_id = format!("runtime-reply-{}", Uuid::new_v4());
    let raw_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Accepted: Inbound meeting response correlation\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
            "SEQUENCE:2\r\n",
            "DTSTAMP:20260824T061000Z\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: reply_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Accepted: Inbound meeting response correlation".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: raw_reply.clone(),
        })
        .await
        .context("deliver inbound REPLY response")?;
    let reply = sqlx::query(
        r#"
        SELECT
            attendees_json #>> '{attendees,0,partstat}' AS partstat,
            attendees_json #>> '{attendees,0,counter_proposal}' AS counter_proposal,
            attendees_json #>> '{attendees,0,proposed_start}' AS proposed_start,
            attendees_json #>> '{attendees,0,proposed_end}' AS proposed_end
        FROM calendar_events
        WHERE id = $1
        "#,
    )
    .bind(event.id)
    .fetch_one(pool)
    .await
    .context("load organizer event after reply")?;
    anyhow::ensure!(
        reply.try_get::<String, _>("partstat")? == "accepted"
            && reply.try_get::<String, _>("counter_proposal")? == "false"
            && reply
                .try_get::<Option<String>, _>("proposed_start")?
                .is_none()
            && reply
                .try_get::<Option<String>, _>("proposed_end")?
                .is_none(),
        "ordinary reply must clear the attendee's prior counter proposal"
    );
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &reply_trace_id).await?
            == Some(("calendar.meeting-response.applied".to_string(), true,)),
        "applied REPLY did not record the bounded processed outcome"
    );

    let idempotent_trace_id = format!("runtime-idempotent-reply-{}", Uuid::new_v4());
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: idempotent_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Accepted: idempotent response".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: raw_reply,
        })
        .await
        .context("deliver exact idempotent REPLY response")?;
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &idempotent_trace_id).await?
            == Some(("calendar.meeting-response.idempotent".to_string(), true,)),
        "exact response replay did not record the bounded idempotent outcome"
    );

    let superseded_trace_id = format!("runtime-superseded-reply-{}", Uuid::new_v4());
    let superseded_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Declined: superseded response\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch\r\n",
            "SEQUENCE:2\r\n",
            "DTSTAMP:20260824T060500Z\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: superseded_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Declined: superseded response".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: superseded_reply,
        })
        .await
        .context("deliver older-DTSTAMP superseded REPLY response")?;
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &superseded_trace_id).await?
            == Some(("calendar.meeting-response.superseded".to_string(), true,)),
        "older response did not record the bounded superseded outcome"
    );

    let response_state_before_invalid = sqlx::query_scalar::<_, String>(
        "SELECT meeting_response_state_json::text FROM calendar_events WHERE id = $1",
    )
    .bind(event.id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE calendar_events
        SET meeting_response_state_json = '{"denis.ducret@sdic.ch":"invalid"}'::jsonb
        WHERE id = $1
        "#,
    )
    .bind(event.id)
    .execute(pool)
    .await?;
    let invalid_state_trace_id = format!("runtime-invalid-response-state-{}", Uuid::new_v4());
    let invalid_state_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Accepted: invalid durable state\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART:20260824T063000Z\r\n",
            "DTEND:20260824T070000Z\r\n",
            "SEQUENCE:2\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: invalid_state_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Accepted: invalid durable state".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: invalid_state_reply,
        })
        .await
        .context("deliver response against invalid durable watermark state")?;
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &invalid_state_trace_id)
            .await?
            == Some((
                "calendar.meeting-response.ignored-invalid-durable-state".to_string(),
                false,
            )),
        "invalid durable response state did not record the bounded unprocessed outcome"
    );
    sqlx::query("UPDATE calendar_events SET meeting_response_state_json = $2::jsonb WHERE id = $1")
        .bind(event.id)
        .bind(response_state_before_invalid)
        .execute(pool)
        .await?;

    let duplicate_calendar = storage
        .create_accessible_calendar_collection(fixture.account_id, "Meeting response UID collision")
        .await
        .context("create a second calendar for duplicate meeting UID correlation")?;
    let mut duplicate_input =
        runtime_calendar_event_input(fixture.account_id, None, "Duplicate meeting response UID");
    duplicate_input.uid = uid.to_string();
    duplicate_input.date = "2026-08-24".to_string();
    duplicate_input.time = "09:00".to_string();
    duplicate_input.duration_minutes = 30;
    duplicate_input.sequence = 2;
    duplicate_input.attendees = "Denis Ducret".to_string();
    duplicate_input.attendees_json = organizer_attendees_json;
    let duplicate_event = storage
        .create_accessible_event(
            fixture.account_id,
            Some(&duplicate_calendar.id),
            duplicate_input,
        )
        .await
        .context("create a second active event with the same meeting UID")?;

    let ambiguous_trace_id = format!("runtime-ambiguous-reply-{}", Uuid::new_v4());
    let ambiguous_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Declined: ambiguous duplicate UID\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: ambiguous_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Declined: ambiguous duplicate UID".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: ambiguous_reply,
        })
        .await
        .context("deliver ambiguous duplicate-UID REPLY")?;
    let ambiguous_states = sqlx::query(
        r#"
        SELECT
            (SELECT attendees_json #>> '{attendees,0,partstat}'
             FROM calendar_events WHERE id = $1) AS original_partstat,
            (SELECT attendees_json #>> '{attendees,0,partstat}'
             FROM calendar_events WHERE id = $2) AS duplicate_partstat
        "#,
    )
    .bind(event.id)
    .bind(duplicate_event.id)
    .fetch_one(pool)
    .await
    .context("load events after ambiguous duplicate-UID response")?;
    anyhow::ensure!(
        ambiguous_states.try_get::<String, _>("original_partstat")? == "accepted"
            && ambiguous_states.try_get::<String, _>("duplicate_partstat")? == "needs-action",
        "a response with an ambiguous duplicate UID must fail closed"
    );
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &ambiguous_trace_id).await?
            == Some((
                "calendar.meeting-response.ignored-ambiguous-candidate".to_string(),
                false,
            )),
        "ambiguous response did not record the bounded unprocessed outcome"
    );

    let targeted_trace_id = format!("runtime-targeted-reply-{}", Uuid::new_v4());
    let targeted_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Declined: interval-correlated duplicate UID\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART:20260824T090000Z\r\n",
            "DTEND:20260824T093000Z\r\n",
            "SEQUENCE:2\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: targeted_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Declined: interval-correlated duplicate UID".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: targeted_reply,
        })
        .await
        .context("deliver interval-and-sequence-correlated duplicate-UID REPLY")?;
    let targeted_states = sqlx::query(
        r#"
        SELECT
            (SELECT attendees_json #>> '{attendees,0,partstat}'
             FROM calendar_events WHERE id = $1) AS original_partstat,
            (SELECT attendees_json #>> '{attendees,0,partstat}'
             FROM calendar_events WHERE id = $2) AS duplicate_partstat
        "#,
    )
    .bind(event.id)
    .bind(duplicate_event.id)
    .fetch_one(pool)
    .await
    .context("load events after targeted duplicate-UID response")?;
    anyhow::ensure!(
        targeted_states.try_get::<String, _>("original_partstat")? == "accepted"
            && targeted_states.try_get::<String, _>("duplicate_partstat")? == "declined",
        "REPLY interval and sequence evidence must select exactly one duplicate-UID event"
    );
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &targeted_trace_id).await?
            == Some(("calendar.meeting-response.applied".to_string(), true,)),
        "targeted response did not record the bounded applied outcome"
    );

    let stale_sequence_trace_id = format!("runtime-stale-sequence-{}", Uuid::new_v4());
    let stale_sequence_reply = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: Declined: stale meeting sequence\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART:20260824T063000Z\r\n",
            "DTEND:20260824T070000Z\r\n",
            "SEQUENCE:1\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: stale_sequence_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Declined: stale meeting sequence".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: stale_sequence_reply,
        })
        .await
        .context("deliver stale-sequence REPLY")?;
    let original_after_stale_sequence = sqlx::query_scalar::<_, String>(
        "SELECT attendees_json #>> '{attendees,0,partstat}' FROM calendar_events WHERE id = $1",
    )
    .bind(event.id)
    .fetch_one(pool)
    .await
    .context("load original event after stale-sequence response")?;
    anyhow::ensure!(
        original_after_stale_sequence == "accepted",
        "a response for an older meeting sequence must not update the current event"
    );
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &stale_sequence_trace_id)
            .await?
            == Some((
                "calendar.meeting-response.ignored-no-candidate".to_string(),
                false,
            )),
        "sequence-filtered response did not record the bounded no-candidate outcome"
    );

    let stale_trace_id = format!("runtime-stale-counter-{}", Uuid::new_v4());
    let stale_counter = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: {}\r\n",
            "Subject: New Time Proposed: stale response\r\n",
            "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:COUNTER\r\n",
            "BEGIN:VEVENT\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART:20260824T123000Z\r\n",
            "DTEND:20260824T130000Z\r\n",
            "X-MS-OLK-ORIGINALSTART:20260824T080000Z\r\n",
            "X-MS-OLK-ORIGINALEND:20260824T083000Z\r\n",
            "UID:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email, uid
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: stale_trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "New Time Proposed: stale response".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: stale_counter,
        })
        .await
        .context("deliver stale inbound COUNTER response")?;
    let stale = sqlx::query(
        r#"
        SELECT
            attendees_json #>> '{attendees,0,partstat}' AS partstat,
            attendees_json #>> '{attendees,0,counter_proposal}' AS counter_proposal
        FROM calendar_events
        WHERE id = $1
        "#,
    )
    .bind(event.id)
    .fetch_one(pool)
    .await
    .context("load organizer event after stale counter")?;
    anyhow::ensure!(
        stale.try_get::<String, _>("partstat")? == "accepted"
            && stale.try_get::<String, _>("counter_proposal")? == "false",
        "a counter for an older scheduled interval must not update attendee state"
    );
    anyhow::ensure!(
        meeting_response_outcome_for_trace(pool, fixture.tenant_id, &stale_trace_id).await?
            == Some((
                "calendar.meeting-response.ignored-no-candidate".to_string(),
                false,
            )),
        "interval-filtered response did not record the bounded no-candidate outcome"
    );
    Ok(())
}

async fn exercise_mapi_meeting_request_processed_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let trace_id = format!("runtime-request-processed-{}", Uuid::new_v4());
    let uid = format!("probe-1930-{}@sdic.ch", Uuid::new_v4());
    let raw_message = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: LPE Test <{}>\r\n",
            "Subject: Probe 1930 Processed\r\n",
            "Message-ID: <{}@sdic.ch>\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/alternative; boundary=processed-boundary\r\n",
            "\r\n",
            "--processed-boundary\r\n",
            "Content-Type: text/plain; charset=UTF-8\r\n",
            "\r\n",
            "Probe 1930\r\n",
            "--processed-boundary\r\n",
            "Content-Type: text/calendar; method=REQUEST; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "VERSION:2.0\r\n",
            "METHOD:REQUEST\r\n",
            "BEGIN:VEVENT\r\n",
            "UID:{}\r\n",
            "DTSTAMP:20260824T172750Z\r\n",
            "DTSTART:20260825T100000Z\r\n",
            "DTEND:20260825T103000Z\r\n",
            "SEQUENCE:0\r\n",
            "ORGANIZER;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
            "ATTENDEE;CN=LPE Test;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:{}\r\n",
            "SUMMARY:Probe 1930 Processed\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n",
            "--processed-boundary--\r\n"
        ),
        fixture.account_email, trace_id, uid, fixture.account_email
    )
    .into_bytes();
    storage
        .deliver_inbound_message(InboundDeliveryRequest {
            trace_id: trace_id.clone(),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.sdic.ch".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec![fixture.account_email.clone()],
            subject: "Probe 1930 Processed".to_string(),
            body_text: "Probe 1930".to_string(),
            internet_message_id: None,
            raw_message,
        })
        .await
        .context("deliver inbound Meeting Request for Processed mutation")?;
    let message_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT message_id
        FROM message_headers
        WHERE tenant_id = $1
          AND lower(header_name) = 'x-lpe-ct-trace-id'
          AND header_value = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(&trace_id)
    .fetch_one(pool)
    .await
    .context("load inbound Meeting Request id")?;
    let initial_email = storage
        .fetch_jmap_emails(fixture.account_id, &[message_id])
        .await?
        .into_iter()
        .next()
        .context("load inbound Meeting Request")?;
    anyhow::ensure!(
        initial_email
            .calendar_meeting_request
            .as_ref()
            .is_some_and(|request| !request.client_processed),
        "a new Meeting Request must omit the client Processed state"
    );

    let copy_before_mailbox_id = Uuid::new_v4();
    let move_target_mailbox_id = Uuid::new_v4();
    let copy_after_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES
            ($1, $4, $5, 'custom', $6, 7101, 7101),
            ($2, $4, $5, 'custom', $7, 7102, 7102),
            ($3, $4, $5, 'custom', $8, 7103, 7103)
        "#,
    )
    .bind(copy_before_mailbox_id)
    .bind(move_target_mailbox_id)
    .bind(copy_after_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(format!("Processed copy before {trace_id}"))
    .bind(format!("Processed move target {trace_id}"))
    .bind(format!("Processed copy after {trace_id}"))
    .execute(pool)
    .await
    .context("seed Meeting Request lifecycle mailboxes")?;
    storage
        .copy_jmap_email(
            fixture.account_id,
            message_id,
            copy_before_mailbox_id,
            audit(
                &fixture.account_email,
                "calendar-request-copy",
                "copy request before Processed",
            ),
        )
        .await
        .context("copy unprocessed Meeting Request")?;

    let before_memberships = sqlx::query(
        r#"
        SELECT COUNT(*) AS membership_count,
               COALESCE(bool_or(calendar_request_processed), FALSE) AS any_processed,
               MAX(modseq) AS max_modseq
        FROM mailbox_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        before_memberships.try_get::<i64, _>("membership_count")? == 2
            && !before_memberships.try_get::<bool, _>("any_processed")?,
        "a pre-Processed copy must retain false on both visible memberships"
    );
    let before_modseq = before_memberships.try_get::<i64, _>("max_modseq")?;

    let store_identity = sqlx::query(
        "SELECT replica_guid, next_global_counter FROM mapi_store_identity WHERE singleton = TRUE",
    )
    .fetch_one(pool)
    .await?;
    let replica_guid: Uuid = store_identity.try_get("replica_guid")?;
    let identity_counter = u64::try_from(store_identity.try_get::<i64, _>("next_global_counter")?)?;
    let source_key = mapi_xid(replica_guid, identity_counter);
    let mut predecessor_change_list = Vec::with_capacity(source_key.len() + 1);
    predecessor_change_list.push(source_key.len() as u8);
    predecessor_change_list.extend_from_slice(&source_key);
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, account_id) DO NOTHING
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list, updated_at
        )
        VALUES ($1, $2, 'message', $3, $4, $5, $6, $6, $6, $4, $7,
                NOW() - INTERVAL '1 minute')
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .bind(identity_counter as i64)
    .bind(mapi_store_id(identity_counter) as i64)
    .bind(&source_key)
    .bind(&predecessor_change_list)
    .execute(pool)
    .await
    .context("seed Meeting Request MAPI identity")?;
    sqlx::query("UPDATE mapi_store_identity SET next_global_counter = $1 WHERE singleton = TRUE")
        .bind((identity_counter + 1) as i64)
        .execute(pool)
        .await?;
    let before_identity = sqlx::query(
        r#"
        SELECT mapi_change_number, change_key, predecessor_change_list,
               updated_at::text AS updated_at
        FROM mapi_object_identities
        WHERE tenant_id = $1 AND account_id = $2
          AND object_kind = 'message' AND canonical_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    let before_cursor = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(cursor), 0) FROM mail_change_log WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id)
    .fetch_one(pool)
    .await?;
    let process_subject = format!("message:{message_id}");
    let (processed_email, changed) = storage
        .mark_mapi_calendar_meeting_request_processed(
            fixture.account_id,
            message_id,
            audit(
                &fixture.account_email,
                "mapi-process-meeting-request",
                &process_subject,
            ),
        )
        .await
        .context("mark Meeting Request Processed")?;
    anyhow::ensure!(
        changed
            && processed_email
                .calendar_meeting_request
                .as_ref()
                .is_some_and(|request| request.client_processed),
        "the first Meeting Request Processed commit was not reported as changed"
    );
    let processed_memberships = sqlx::query(
        r#"
        SELECT COUNT(*) AS membership_count,
               bool_and(calendar_request_processed) AS all_processed,
               COUNT(DISTINCT modseq) AS distinct_modseq,
               MIN(modseq) AS committed_modseq
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    let committed_modseq = processed_memberships.try_get::<i64, _>("committed_modseq")?;
    anyhow::ensure!(
        processed_memberships.try_get::<i64, _>("membership_count")? == 2
            && processed_memberships.try_get::<bool, _>("all_processed")?
            && processed_memberships.try_get::<i64, _>("distinct_modseq")? == 1
            && committed_modseq > before_modseq,
        "Processed must atomically update every visible membership at one new modseq"
    );
    let processed_replay_rows = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1 AND account_id = $2
          AND object_kind = 'mailbox_message'
          AND cursor > $3
          AND modseq = $4
          AND summary_json @> '{"calendarRequestProcessedChanged":true}'::jsonb
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(before_cursor)
    .bind(committed_modseq)
    .fetch_one(pool)
    .await?;
    let process_audits = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) FROM audit_events
        WHERE tenant_id = $1 AND action = 'mapi-process-meeting-request' AND subject = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(&process_subject)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        processed_replay_rows == 2 && process_audits == 1,
        "the first Processed transition must journal both memberships and audit once"
    );
    let after_identity = sqlx::query(
        r#"
        SELECT mapi_change_number, change_key, predecessor_change_list,
               updated_at::text AS updated_at
        FROM mapi_object_identities
        WHERE tenant_id = $1 AND account_id = $2
          AND object_kind = 'message' AND canonical_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        after_identity.try_get::<i64, _>("mapi_change_number")?
            > before_identity.try_get::<i64, _>("mapi_change_number")?
            && after_identity.try_get::<Vec<u8>, _>("change_key")?
                != before_identity.try_get::<Vec<u8>, _>("change_key")?
            && after_identity.try_get::<Vec<u8>, _>("predecessor_change_list")?
                != before_identity.try_get::<Vec<u8>, _>("predecessor_change_list")?
            && after_identity.try_get::<String, _>("updated_at")?
                != before_identity.try_get::<String, _>("updated_at")?,
        "the first Processed transition must rotate ChangeKey/PCL/LMT"
    );
    let committed_change_number = after_identity.try_get::<i64, _>("mapi_change_number")?;
    let committed_change_key = after_identity.try_get::<Vec<u8>, _>("change_key")?;
    let committed_predecessors = after_identity.try_get::<Vec<u8>, _>("predecessor_change_list")?;
    let committed_updated_at = after_identity.try_get::<String, _>("updated_at")?;
    let cursor_after_first = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(cursor), 0) FROM mail_change_log WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id)
    .fetch_one(pool)
    .await?;
    let (_, changed) = storage
        .mark_mapi_calendar_meeting_request_processed(
            fixture.account_id,
            message_id,
            audit(
                &fixture.account_email,
                "mapi-process-meeting-request",
                &process_subject,
            ),
        )
        .await?;
    let idempotent_state = sqlx::query(
        r#"
        SELECT identity.mapi_change_number, identity.change_key,
               identity.predecessor_change_list, identity.updated_at::text AS updated_at,
               (SELECT COALESCE(MAX(cursor), 0) FROM mail_change_log WHERE tenant_id = $1) AS cursor,
               (SELECT COUNT(*) FROM audit_events
                WHERE tenant_id = $1 AND action = 'mapi-process-meeting-request'
                  AND subject = $3) AS audit_count
        FROM mapi_object_identities identity
        WHERE identity.tenant_id = $1 AND identity.account_id = $2
          AND identity.object_kind = 'message' AND identity.canonical_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(&process_subject)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        !changed
            && idempotent_state.try_get::<i64, _>("mapi_change_number")? == committed_change_number
            && idempotent_state.try_get::<Vec<u8>, _>("change_key")? == committed_change_key
            && idempotent_state.try_get::<Vec<u8>, _>("predecessor_change_list")?
                == committed_predecessors
            && idempotent_state.try_get::<String, _>("updated_at")? == committed_updated_at
            && idempotent_state.try_get::<i64, _>("cursor")? == cursor_after_first
            && idempotent_state.try_get::<i64, _>("audit_count")? == 1,
        "repeated TRUE must not rotate, journal, audit, or report a durable transition"
    );

    let generation_before_parser_repair = sqlx::query_scalar::<_, i64>(
        "SELECT classification_generation FROM calendar_mail_classifications WHERE tenant_id = $1 AND message_id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE calendar_mail_classifications
        SET parser_revision = parser_revision - 1, updated_at = NOW()
        WHERE tenant_id = $1 AND message_id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(message_id)
    .execute(pool)
    .await?;
    let parser_repaired = storage
        .fetch_jmap_emails(fixture.account_id, &[message_id])
        .await?
        .into_iter()
        .next()
        .context("load parser-only repaired Meeting Request")?;
    let generation_after_parser_repair = sqlx::query_scalar::<_, i64>(
        "SELECT classification_generation FROM calendar_mail_classifications WHERE tenant_id = $1 AND message_id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        generation_after_parser_repair == generation_before_parser_repair
            && parser_repaired
                .calendar_meeting_request
                .as_ref()
                .is_some_and(|request| request.client_processed),
        "a parser-only repair of unchanged request payload must preserve Processed"
    );

    let moved = storage
        .move_jmap_email_from_mailbox(
            fixture.account_id,
            fixture.inbox_id,
            message_id,
            move_target_mailbox_id,
            audit(
                &fixture.account_email,
                "calendar-request-move",
                "move processed request",
            ),
        )
        .await?;
    anyhow::ensure!(
        moved
            .calendar_meeting_request
            .as_ref()
            .is_some_and(|request| request.client_processed),
        "move must preserve Meeting Request Processed"
    );
    let copied_after = storage
        .copy_jmap_email(
            fixture.account_id,
            message_id,
            copy_after_mailbox_id,
            audit(
                &fixture.account_email,
                "calendar-request-copy",
                "copy processed request",
            ),
        )
        .await?;
    anyhow::ensure!(
        copied_after
            .calendar_meeting_request
            .as_ref()
            .is_some_and(|request| request.client_processed),
        "a later copy must inherit Meeting Request Processed"
    );

    let target_account_id = Uuid::new_v4();
    let target_mailbox_id = Uuid::new_v4();
    let domain_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT primary_domain_id FROM accounts WHERE tenant_id = $1 AND id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Processed generation target')
        "#,
    )
    .bind(target_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(format!(
        "processed-target-{}@example.test",
        Uuid::new_v4().simple()
    ))
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 7201)
        "#,
    )
    .bind(target_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await?;
    storage
        .copy_jmap_email_between_accounts(
            fixture.account_id,
            target_account_id,
            message_id,
            target_mailbox_id,
            audit(
                &fixture.account_email,
                "calendar-request-cross-account-copy",
                "retain visible generation trigger",
            ),
        )
        .await?;

    storage
        .delete_jmap_email(
            fixture.account_id,
            message_id,
            audit(
                &fixture.account_email,
                "mapi-delete-message",
                "expunge processed request before generation change",
            ),
        )
        .await?;
    let recoverable_before_generation = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT recoverable.id
        FROM recoverable_items recoverable
        JOIN mailbox_messages source
          ON source.tenant_id = recoverable.tenant_id
         AND source.account_id = recoverable.account_id
         AND source.id = recoverable.source_mailbox_message_id
        WHERE recoverable.tenant_id = $1 AND recoverable.account_id = $2
          AND recoverable.message_id = $3 AND recoverable.status = 'active'
          AND source.calendar_request_processed
        ORDER BY recoverable.deleted_at DESC, recoverable.id
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await
    .context("load processed recoverable source before generation change")?;
    sqlx::query(
        r#"
        UPDATE calendar_mail_classifications
        SET parser_revision = parser_revision - 1,
            metadata_json = jsonb_set(
                metadata_json,
                '{request,meeting_location}',
                '"stale generation"'::jsonb,
                TRUE
            ),
            updated_at = NOW()
        WHERE tenant_id = $1 AND message_id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(message_id)
    .execute(pool)
    .await?;
    storage
        .fetch_jmap_emails(target_account_id, &[message_id])
        .await?
        .into_iter()
        .next()
        .context("visible target did not trigger classification generation repair")?;
    let reset_state = sqlx::query(
        r#"
        SELECT classification.classification_generation,
               projection.applied_generation,
               COALESCE(bool_or(source.calendar_request_processed), FALSE) AS any_processed
        FROM calendar_mail_classifications classification
        JOIN mailbox_messages source
          ON source.tenant_id = classification.tenant_id
         AND source.message_id = classification.message_id
         AND source.account_id = $2
        LEFT JOIN calendar_mail_classification_projections projection
          ON projection.tenant_id = classification.tenant_id
         AND projection.account_id = $2
         AND projection.message_id = classification.message_id
        WHERE classification.tenant_id = $1 AND classification.message_id = $3
        GROUP BY classification.classification_generation, projection.applied_generation
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        reset_state.try_get::<i64, _>("classification_generation")?
            > generation_after_parser_repair
            && reset_state.try_get::<i64, _>("applied_generation")?
                < reset_state.try_get::<i64, _>("classification_generation")?
            && !reset_state.try_get::<bool, _>("any_processed")?,
        "a payload generation change must reset retained rows for an expunged-only account"
    );
    let restored_after_generation = storage
        .restore_recoverable_item(
            fixture.account_id,
            recoverable_before_generation,
            Some(fixture.inbox_id),
            audit(
                &fixture.account_email,
                "restore-recoverable-message",
                "restore request after generation change",
            ),
        )
        .await?;
    anyhow::ensure!(
        restored_after_generation
            .calendar_meeting_request
            .as_ref()
            .is_some_and(|request| !request.client_processed),
        "Recoverable Items restore must not revive Processed from an older generation"
    );

    let (_, changed) = storage
        .mark_mapi_calendar_meeting_request_processed(
            fixture.account_id,
            message_id,
            audit(
                &fixture.account_email,
                "mapi-process-meeting-request",
                &process_subject,
            ),
        )
        .await?;
    anyhow::ensure!(
        changed,
        "the new request generation must accept Processed again"
    );
    storage
        .delete_jmap_email(
            fixture.account_id,
            message_id,
            audit(
                &fixture.account_email,
                "mapi-delete-message",
                "expunge processed request for stable restore",
            ),
        )
        .await?;
    let stable_recoverable = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT recoverable.id
        FROM recoverable_items recoverable
        JOIN mailbox_messages source
          ON source.tenant_id = recoverable.tenant_id
         AND source.account_id = recoverable.account_id
         AND source.id = recoverable.source_mailbox_message_id
        WHERE recoverable.tenant_id = $1 AND recoverable.account_id = $2
          AND recoverable.message_id = $3 AND recoverable.status = 'active'
          AND source.calendar_request_processed
        ORDER BY recoverable.deleted_at DESC, recoverable.id
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    let stable_restore = storage
        .restore_recoverable_item(
            fixture.account_id,
            stable_recoverable,
            Some(fixture.inbox_id),
            audit(
                &fixture.account_email,
                "restore-recoverable-message",
                "restore request without generation change",
            ),
        )
        .await?;
    anyhow::ensure!(
        stable_restore
            .calendar_meeting_request
            .as_ref()
            .is_some_and(|request| request.client_processed),
        "Recoverable Items restore must preserve Processed within the same generation"
    );

    Ok(())
}

async fn meeting_response_outcome_for_trace(
    pool: &PgPool,
    tenant_id: Uuid,
    trace_id: &str,
) -> Result<Option<(String, bool)>> {
    let row = sqlx::query(
        r#"
        SELECT audit.action, message.calendar_response_processed
        FROM message_headers trace
        JOIN messages message
          ON message.tenant_id = trace.tenant_id
         AND message.id = trace.message_id
        JOIN audit_events audit
          ON audit.tenant_id = message.tenant_id
         AND audit.subject = 'message:' || message.id::text
         AND audit.action LIKE 'calendar.meeting-response.%'
        WHERE trace.tenant_id = $1
          AND lower(trace.header_name) = 'x-lpe-ct-trace-id'
          AND trace.header_value = $2
        ORDER BY audit.created_at DESC, audit.id DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(trace_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get("action")?,
            row.try_get("calendar_response_processed")?,
        ))
    })
    .transpose()
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
    let occurrence = all
        .iter()
        .find(|reminder| {
            reminder.title == "Recurring calendar reminder"
                && reminder.dismissed_at.is_none()
                && reminder.occurrence_start_at.is_some()
        })
        .context("seeded recurring calendar reminder must have an active occurrence")?;
    let occurrence_start_at = occurrence.occurrence_start_at.clone().unwrap();
    storage
        .snooze_reminder_occurrence(
            fixture.account_id,
            "calendar",
            occurrence.source_id,
            &occurrence_start_at,
            "2099-01-01T00:00:00Z",
        )
        .await
        .context("snooze one recurring reminder occurrence")?;
    let snoozed = storage
        .query_client_reminders(
            fixture.account_id,
            ReminderQuery {
                include_inactive: true,
            },
        )
        .await
        .context("query persisted recurring reminder snooze")?;
    anyhow::ensure!(
        snoozed.iter().any(|reminder| {
            reminder.source_type == "calendar"
                && reminder.source_id == occurrence.source_id
                && reminder.occurrence_start_at.as_deref() == Some(occurrence_start_at.as_str())
                && reminder.reminder_at == "2099-01-01T00:00:00Z"
                && reminder.dismissed_at.is_none()
        }),
        "recurring reminder snooze must persist against only its occurrence identity"
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
            ($8, $5, $6, $7, $8::text, 'Recurring calendar reminder', date_trunc('hour', NOW()) - interval '1 hour', date_trunc('hour', NOW()), 'FREQ=DAILY;COUNT=2;BYDAY=' || CASE EXTRACT(ISODOW FROM date_trunc('hour', NOW()) - interval '1 hour')::int WHEN 1 THEN 'MO' WHEN 2 THEN 'TU' WHEN 3 THEN 'WE' WHEN 4 THEN 'TH' WHEN 5 THEN 'FR' WHEN 6 THEN 'SA' ELSE 'SU' END, TRUE, date_trunc('hour', NOW()) - interval '70 minutes', NULL, 'confirmed')
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

async fn exercise_managed_retention_folder_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let tag_id = Uuid::new_v4();
    let hidden_tag_id = Uuid::new_v4();
    let foreign_tenant_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, $2, 'Foreign Retention Tenant')
        "#,
    )
    .bind(foreign_tenant_id)
    .bind(format!("foreign-retention-{}", Uuid::new_v4().simple()))
    .execute(pool)
    .await
    .context("seed foreign tenant for managed retention isolation")?;

    sqlx::query(
        r#"
        INSERT INTO retention_policy_tags (
            id, tenant_id, display_name, tag_type, action, retention_days,
            is_visible, description
        )
        VALUES
            ($1, $2, 'Managed Archive', 'custom_folder', 'delete_and_allow_recovery', 180, TRUE, 'Managed archive'),
            ($3, $2, 'Hidden Managed Folder', 'custom_folder', 'delete_and_allow_recovery', 90, FALSE, 'Hidden managed folder'),
            ($4, $5, 'Foreign Managed Folder', 'custom_folder', 'delete_and_allow_recovery', 30, TRUE, 'Foreign managed folder')
        "#,
    )
    .bind(tag_id)
    .bind(fixture.tenant_id)
    .bind(hidden_tag_id)
    .bind(Uuid::new_v4())
    .bind(foreign_tenant_id)
    .execute(pool)
    .await
    .context("seed retention policy tags for managed folder path")?;

    let folder = storage
        .create_managed_retention_folder(
            ManagedRetentionFolderCreateInput {
                account_id: fixture.account_id,
                folder_name: "Managed Archive".to_string(),
                is_subscribed: true,
            },
            audit(
                "test-admin",
                "mailbox.create-managed-retention-folder",
                "managed archive",
            ),
        )
        .await
        .context("create managed retention folder through canonical storage API")?;

    let row = sqlx::query(
        r#"
        SELECT retention_policy_tag_id, retention_days
        FROM mailboxes
        WHERE tenant_id = $1
          AND account_id = $2
          AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(folder.id)
    .fetch_one(pool)
    .await
    .context("load managed retention folder mailbox row")?;
    anyhow::ensure!(
        row.try_get::<Option<Uuid>, _>("retention_policy_tag_id")? == Some(tag_id),
        "managed retention folder must store canonical retention tag identity"
    );
    anyhow::ensure!(
        row.try_get::<i32, _>("retention_days")? == 180,
        "managed retention folder must project tag retention days onto mailbox retention guard"
    );

    expect_anyhow_failure(
        "managed retention folder rejects hidden unassigned same-tenant tag",
        storage
            .create_managed_retention_folder(
                ManagedRetentionFolderCreateInput {
                    account_id: fixture.account_id,
                    folder_name: "Hidden Managed Folder".to_string(),
                    is_subscribed: true,
                },
                audit(
                    "test-admin",
                    "mailbox.create-managed-retention-folder",
                    "hidden managed folder",
                ),
            )
            .await,
    )?;
    expect_anyhow_failure(
        "managed retention folder rejects cross-tenant tag",
        storage
            .create_managed_retention_folder(
                ManagedRetentionFolderCreateInput {
                    account_id: fixture.account_id,
                    folder_name: "Foreign Managed Folder".to_string(),
                    is_subscribed: true,
                },
                audit(
                    "test-admin",
                    "mailbox.create-managed-retention-folder",
                    "foreign managed folder",
                ),
            )
            .await,
    )?;

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

async fn exercise_mapi_special_folder_alias_constraints(
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let canonical_junk_folder_id = (30_i64 << 16) | 1;
    let first_alias_counter = 0x230_i64;
    let second_alias_counter = first_alias_counter + 1;
    let first_alias_folder_id = (first_alias_counter << 16) | 1;
    let second_alias_folder_id = (second_alias_counter << 16) | 1;
    let first_change_number = 0x1_0030_i64;
    let second_change_number = first_change_number + 1;
    let first_source_key = mapi_source_key(first_alias_counter as u64);
    let second_source_key = mapi_source_key(second_alias_counter as u64);

    insert_mapi_special_folder_alias(
        pool,
        fixture,
        first_alias_folder_id,
        canonical_junk_folder_id,
        &first_source_key,
        first_change_number,
    )
    .await
    .context("store the first Outlook profile special-folder alias")?;
    insert_mapi_special_folder_alias(
        pool,
        fixture,
        second_alias_folder_id,
        canonical_junk_folder_id,
        &second_source_key,
        second_change_number,
    )
    .await
    .context("store a second Outlook profile alias for the same canonical special folder")?;

    expect_constraint_failure(
        "MAPI special-folder aliases reject a non-dynamic alias FID",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            (42_i64 << 16) | 1,
            canonical_junk_folder_id,
            &mapi_source_key(42),
            second_change_number + 1,
        )
        .await,
    )?;
    expect_constraint_failure(
        "MAPI special-folder aliases reject a non-special canonical FID",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            ((second_alias_counter + 1) << 16) | 1,
            (43_i64 << 16) | 1,
            &mapi_source_key((second_alias_counter + 1) as u64),
            second_change_number + 2,
        )
        .await,
    )?;
    expect_constraint_failure(
        "MAPI special-folder aliases reject malformed SourceKeys",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            ((second_alias_counter + 2) << 16) | 1,
            canonical_junk_folder_id,
            &[0_u8; 21],
            second_change_number + 3,
        )
        .await,
    )?;
    expect_constraint_failure(
        "MAPI special-folder aliases reject server CNs below the dynamic range",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            ((second_alias_counter + 3) << 16) | 1,
            canonical_junk_folder_id,
            &mapi_source_key((second_alias_counter + 3) as u64),
            42,
        )
        .await,
    )?;
    expect_constraint_failure(
        "MAPI special-folder aliases reject duplicate SourceKeys",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            ((second_alias_counter + 4) << 16) | 1,
            canonical_junk_folder_id,
            &first_source_key,
            second_change_number + 4,
        )
        .await,
    )?;
    expect_constraint_failure(
        "MAPI special-folder aliases reject duplicate server CNs",
        insert_mapi_special_folder_alias(
            pool,
            fixture,
            ((second_alias_counter + 5) << 16) | 1,
            canonical_junk_folder_id,
            &mapi_source_key((second_alias_counter + 5) as u64),
            first_change_number,
        )
        .await,
    )?;

    Ok(())
}

async fn insert_mapi_special_folder_alias(
    pool: &PgPool,
    fixture: &RuntimeFixture,
    alias_folder_id: i64,
    canonical_folder_id: i64,
    source_key: &[u8],
    change_number: i64,
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO mapi_special_folder_aliases (
            tenant_id, account_id, alias_folder_id, canonical_folder_id,
            source_key, mapi_change_number
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(alias_folder_id)
    .bind(canonical_folder_id)
    .bind(source_key)
    .bind(change_number)
    .execute(pool)
    .await
    .map(|_| ())
}

fn mapi_source_key(global_counter: u64) -> Vec<u8> {
    let mut source_key = vec![
        0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f,
        0x10,
    ];
    source_key.extend_from_slice(&global_counter.to_be_bytes()[2..]);
    source_key
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
                replace_attachments: false,
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

async fn exercise_submission_cancellation_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let submitted = exercise_submission_path(storage, fixture).await?;
    let cancelled = storage
        .cancel_queued_submission(
            fixture.account_id,
            submitted.message_id,
            audit(
                "alice@example.test",
                "mapi-abort-submit",
                "runtime cancellation",
            ),
        )
        .await
        .context("cancel queued submission")?;
    anyhow::ensure!(
        cancelled == CancelSubmissionResult::Cancelled,
        "queued submission cancellation did not report Cancelled"
    );

    let status = sqlx::query_scalar::<_, String>(
        "SELECT status FROM submission_queue WHERE tenant_id = $1 AND id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(submitted.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("fetch cancelled submission status")?;
    anyhow::ensure!(status == "cancelled", "submission queue was not cancelled");

    let event_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM submission_events
        WHERE tenant_id = $1
          AND submission_queue_id = $2
          AND event_kind = 'cancelled'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(submitted.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("count cancellation event rows")?;
    anyhow::ensure!(
        event_count == 1,
        "submission cancellation event was not written"
    );

    let change_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'submission'
          AND object_id = $3
          AND change_kind = 'updated'
          AND summary_json ->> 'messageId' = $4
          AND summary_json ->> 'status' = 'cancelled'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.outbound_queue_id)
    .bind(submitted.message_id.to_string())
    .fetch_one(pool)
    .await
    .context("count cancellation change-log rows")?;
    anyhow::ensure!(
        change_count == 1,
        "submission cancellation change-log row was not written"
    );

    let duplicate = storage
        .cancel_queued_submission(
            fixture.account_id,
            submitted.message_id,
            audit(
                "alice@example.test",
                "mapi-abort-submit",
                "runtime cancellation duplicate",
            ),
        )
        .await
        .context("cancel already cancelled submission")?;
    anyhow::ensure!(
        duplicate == CancelSubmissionResult::AlreadyCancelled,
        "duplicate cancellation was not idempotent"
    );
    let duplicate_event_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM submission_events
        WHERE tenant_id = $1
          AND submission_queue_id = $2
          AND event_kind = 'cancelled'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(submitted.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("count duplicate cancellation events")?;
    anyhow::ensure!(
        duplicate_event_count == 1,
        "idempotent cancellation wrote duplicate event rows"
    );

    Ok(())
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

async fn exercise_cross_account_jmap_copy_bcc_projection(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
    submitted: &SubmittedMessage,
) -> Result<()> {
    let target_account_id = Uuid::new_v4();
    let target_mailbox_id = Uuid::new_v4();
    let domain_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT primary_domain_id FROM accounts WHERE tenant_id = $1 AND id = $2",
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load source account domain for cross-account JMAP copy")?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Cross-account copy target')
        "#,
    )
    .bind(target_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(format!("copy-target-{}", fixture.account_email))
    .execute(pool)
    .await
    .context("seed cross-account JMAP copy target")?;
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'sent', 'Sent', 0, 1)
        "#,
    )
    .bind(target_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await
    .context("seed cross-account JMAP copy target mailbox")?;

    let decoy_mailbox_id = Uuid::new_v4();
    let decoy_membership_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'custom', 'Existing copy membership', 1, 2)
        "#,
    )
    .bind(decoy_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .execute(pool)
    .await
    .context("seed decoy mailbox before cross-account JMAP copy")?;
    sqlx::query(
        r#"
        INSERT INTO mailbox_messages (
            id, tenant_id, account_id, mailbox_id, message_id, imap_uid, received_at
        )
        VALUES ($1, $2, $3, $4, $5, 1, NOW())
        "#,
    )
    .bind(decoy_membership_id)
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(decoy_mailbox_id)
    .bind(submitted.message_id)
    .execute(pool)
    .await
    .context("seed decoy membership before cross-account JMAP copy")?;
    sqlx::query(
        r#"
        INSERT INTO mail_search_documents (
            tenant_id, account_id, mailbox_message_id, message_id,
            subject_text, participants_visible, body_text, attachment_text,
            search_vector, updated_at
        )
        VALUES (
            $1, $2, $3, $4,
            'decoy subject', 'decoy participant', 'decoy body', 'decoy attachment',
            to_tsvector('simple', 'decoy search projection'), NOW() + INTERVAL '1 minute'
        )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(decoy_membership_id)
    .bind(submitted.message_id)
    .execute(pool)
    .await
    .context("seed newer decoy search projection before cross-account JMAP copy")?;

    let source_search = sqlx::query(
        r#"
        SELECT s.mailbox_message_id, s.subject_text, s.participants_visible,
               s.body_text, s.attachment_text, s.search_vector::text AS search_vector
        FROM mail_search_documents s
        JOIN mailbox_messages mm
          ON mm.tenant_id = s.tenant_id
         AND mm.account_id = s.account_id
         AND mm.id = s.mailbox_message_id
         AND mm.message_id = s.message_id
        WHERE s.tenant_id = $1
          AND s.account_id = $2
          AND s.message_id = $3
          AND mm.visibility = 'visible'
        ORDER BY mm.updated_at DESC
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load exact source search projection before cross-account JMAP copy")?;

    storage
        .copy_jmap_email_between_accounts(
            fixture.account_id,
            target_account_id,
            submitted.message_id,
            target_mailbox_id,
            audit(
                "alice@example.test",
                "jmap-email-copy",
                "cross-account Bcc projection",
            ),
        )
        .await
        .context("copy JMAP email to a different account")?;

    let target_emails = storage
        .fetch_jmap_emails_with_protected_bcc(target_account_id, &[submitted.message_id])
        .await
        .context("fetch copied email with protected Bcc path")?;
    anyhow::ensure!(
        target_emails.len() == 1 && target_emails[0].bcc.is_empty(),
        "cross-account JMAP copy must not expose source protected Bcc"
    );

    let target_search = sqlx::query(
        r#"
        SELECT s.mailbox_message_id, s.subject_text, s.participants_visible,
               s.body_text, s.attachment_text, s.search_vector::text AS search_vector,
               mm.account_id, mm.mailbox_id, mm.message_id, mm.visibility
        FROM mail_search_documents s
        JOIN mailbox_messages mm
          ON mm.tenant_id = s.tenant_id
         AND mm.account_id = s.account_id
         AND mm.id = s.mailbox_message_id
         AND mm.message_id = s.message_id
        WHERE s.tenant_id = $1
          AND s.account_id = $2
          AND s.message_id = $3
          AND mm.mailbox_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(target_account_id)
    .bind(submitted.message_id)
    .bind(target_mailbox_id)
    .fetch_one(pool)
    .await
    .context("load target search projection after cross-account JMAP copy")?;
    let source_search_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_search_documents
        WHERE tenant_id = $1
          AND account_id = $2
          AND mailbox_message_id = $3
          AND message_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_search.try_get::<Uuid, _>("mailbox_message_id")?)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("verify source search projection after cross-account JMAP copy")?;
    anyhow::ensure!(
        target_search.try_get::<Uuid, _>("account_id")? == target_account_id
            && target_search.try_get::<Uuid, _>("mailbox_id")? == target_mailbox_id
            && target_search.try_get::<Uuid, _>("message_id")? == submitted.message_id
            && target_search.try_get::<String, _>("visibility")? == "visible"
            && target_search.try_get::<Uuid, _>("mailbox_message_id")?
                != source_search.try_get::<Uuid, _>("mailbox_message_id")?,
        "cross-account copy search projection must bind the new target membership"
    );
    anyhow::ensure!(
        source_search_count == 1,
        "cross-account copy must preserve the selected source search projection"
    );
    for field in [
        "subject_text",
        "participants_visible",
        "body_text",
        "attachment_text",
        "search_vector",
    ] {
        anyhow::ensure!(
            target_search.try_get::<String, _>(field)?
                == source_search.try_get::<String, _>(field)?,
            "cross-account copy must clone {field} from the selected source membership"
        );
    }

    let shared_subject = target_emails[0].subject.clone();
    let cross_account_mutation = storage
        .update_jmap_email_content(
            fixture.account_id,
            submitted.message_id,
            Some(format!("{shared_subject} mutated without target sync")),
            None,
            audit(
                &fixture.account_email,
                "jmap-email-update",
                "cross-account shared-content isolation",
            ),
        )
        .await;
    anyhow::ensure!(
        cross_account_mutation
            .as_ref()
            .is_err_and(|error| error.to_string().contains("visible in another account")),
        "shared canonical content mutation must fail closed while the message is visible in another account"
    );
    let source_after_rejection = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch source after rejected cross-account content mutation")?;
    let target_after_rejection = storage
        .fetch_jmap_emails(target_account_id, &[submitted.message_id])
        .await
        .context("fetch target after rejected cross-account content mutation")?;
    anyhow::ensure!(
        source_after_rejection.len() == 1
            && target_after_rejection.len() == 1
            && source_after_rejection[0].subject == shared_subject
            && target_after_rejection[0].subject == shared_subject,
        "rejected shared-content mutation must leave both account projections unchanged"
    );

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

async fn exercise_custom_calendar_grant_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let domain_id =
        sqlx::query_scalar::<_, Uuid>("SELECT primary_domain_id FROM accounts WHERE id = $1")
            .bind(fixture.account_id)
            .fetch_one(pool)
            .await
            .context("load runtime fixture domain for custom calendar grantee")?;
    let grantee_account_id = Uuid::new_v4();
    let grantee_email = format!("calendar-grantee-{}@example.test", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Calendar Grantee')
        "#,
    )
    .bind(grantee_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(&grantee_email)
    .execute(pool)
    .await
    .context("seed custom calendar grantee account")?;

    let custom_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order, uid_validity
        )
        VALUES ($1, $2, $3, 'custom', 'Runtime Delegation Custom', 30, 7301)
        "#,
    )
    .bind(custom_mailbox_id)
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .execute(pool)
    .await
    .context("seed custom mailbox for mailbox Share scope")?;
    storage
        .set_mailbox_folder_delegation_grant(
            MailboxFolderDelegationGrantInput {
                owner_account_id: fixture.account_id,
                mailbox_id: custom_mailbox_id,
                grantee_account_id,
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "mailbox-folder-share-upsert",
                "runtime custom mailbox ACL",
            ),
        )
        .await
        .context("grant custom mailbox ACL before mailbox Share")?;
    let mailbox_share = storage
        .upsert_mailbox_delegation_grant_with_preferences(
            MailboxDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: grantee_email.clone(),
                may_write: true,
            },
            DelegatePreferencesPatch {
                meeting_request_delivery: Some("owner_only".to_string()),
                receives_meeting_request_copy: Some(false),
                may_view_private_items: Some(true),
            },
            audit(
                &fixture.account_email,
                "mailbox-share-upsert",
                "runtime default Inbox Share",
            ),
        )
        .await
        .context("create default Inbox mailbox Share")?;
    let inbox_grant_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT grant_row.id
        FROM mailbox_delegation_grants grant_row
        JOIN mailboxes mailbox
          ON mailbox.tenant_id = grant_row.tenant_id
         AND mailbox.account_id = grant_row.owner_account_id
         AND mailbox.id = grant_row.mailbox_id
         AND mailbox.role = 'inbox'
        WHERE grant_row.tenant_id = $1
          AND grant_row.owner_account_id = $2
          AND grant_row.grantee_account_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(grantee_account_id)
    .fetch_one(pool)
    .await
    .context("load default Inbox grant after mailbox Share upsert")?;
    anyhow::ensure!(
        mailbox_share.id == inbox_grant_id
            && mailbox_share.delegate_preferences.meeting_request_delivery == "owner_only"
            && !mailbox_share
                .delegate_preferences
                .receives_meeting_request_copy
            && mailbox_share.delegate_preferences.may_view_private_items,
        "mailbox Share upsert must return the default Inbox relation and canonical preferences"
    );
    let mailbox_shares = storage
        .fetch_outgoing_mailbox_delegation_grants(fixture.account_id)
        .await
        .context("list mailbox Shares with a parallel custom-folder ACL")?;
    let grantee_mailbox_shares = mailbox_shares
        .iter()
        .filter(|grant| grant.grantee_account_id == grantee_account_id)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        grantee_mailbox_shares.len() == 1 && grantee_mailbox_shares[0].id == inbox_grant_id,
        "mailbox Share listing must exclude custom-folder ACL rows"
    );
    storage
        .delete_mailbox_delegation_grant(
            fixture.account_id,
            grantee_account_id,
            audit(
                &fixture.account_email,
                "mailbox-share-delete",
                "runtime default Inbox Share removal",
            ),
        )
        .await
        .context("delete default Inbox mailbox Share")?;
    let remaining_mailbox_grants = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mailbox_delegation_grants
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND grantee_account_id = $3
          AND mailbox_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(grantee_account_id)
    .bind(custom_mailbox_id)
    .fetch_one(pool)
    .await
    .context("count custom-folder ACL rows after mailbox Share deletion")?;
    let remaining_preferences = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM delegate_preferences
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND grantee_account_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(grantee_account_id)
    .fetch_one(pool)
    .await
    .context("count delegate preferences after mailbox Share deletion")?;
    anyhow::ensure!(
        remaining_mailbox_grants == 1 && remaining_preferences == 0,
        "mailbox Share deletion must preserve custom-folder ACLs and delete its preference tuple"
    );

    let mut default_event_input =
        runtime_calendar_event_input(fixture.account_id, None, "Scoped tentative availability");
    default_event_input.date = "2099-08-13".to_string();
    default_event_input.time = "12:00".to_string();
    default_event_input.status = "tentative".to_string();
    storage
        .create_accessible_event(fixture.account_id, None, default_event_input)
        .await
        .context("create tentative event in the default Calendar")?;

    let custom_calendar = storage
        .create_accessible_calendar_collection(fixture.account_id, "Runtime Shared Calendar")
        .await
        .context("create custom calendar for sharing")?;
    let calendar_id =
        Uuid::parse_str(&custom_calendar.id).context("custom calendar id should be a UUID")?;

    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id: fixture.account_id,
                grantee_email: grantee_email.clone(),
                calendar_id: Some(calendar_id),
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "calendar-share-upsert",
                "runtime custom calendar read grant",
            ),
        )
        .await
        .context("share custom calendar through collaboration grant input")?;

    let custom_only_free_busy = storage
        .fetch_free_busy_blocks(
            grantee_account_id,
            fixture.account_id,
            "2099-08-13T13:00:00Z",
            "2099-08-13T11:00:00Z",
        )
        .await
        .context("fetch default-Calendar free/busy with only custom-calendar access")?;
    anyhow::ensure!(
        custom_only_free_busy.len() == 1 && custom_only_free_busy[0].status == "busy",
        "custom-calendar-only access must not reveal default-Calendar availability detail"
    );
    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id: fixture.account_id,
                grantee_email: grantee_email.clone(),
                calendar_id: None,
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "calendar-share-upsert",
                "runtime default Calendar read grant",
            ),
        )
        .await
        .context("share the default Calendar for detailed free/busy")?;
    let default_calendar_free_busy = storage
        .fetch_free_busy_blocks(
            grantee_account_id,
            fixture.account_id,
            "2099-08-13T13:00:00Z",
            "2099-08-13T11:00:00Z",
        )
        .await
        .context("fetch free/busy with default-Calendar access")?;
    anyhow::ensure!(
        default_calendar_free_busy.len() == 1
            && default_calendar_free_busy[0].status == "tentative",
        "default-Calendar read access must reveal modeled availability detail"
    );

    let outgoing = storage
        .fetch_outgoing_collaboration_grants(
            fixture.account_id,
            CollaborationResourceKind::Calendar,
        )
        .await
        .context("fetch outgoing calendar grants after custom share")?;
    anyhow::ensure!(
        outgoing.iter().any(|grant| {
            grant.calendar_id == Some(calendar_id)
                && grant.grantee_account_id == grantee_account_id
                && grant.rights.may_read
                && !grant.rights.may_write
        }),
        "custom calendar grant must appear in outgoing calendar shares"
    );

    let incoming = storage
        .fetch_accessible_calendar_collections(grantee_account_id)
        .await
        .context("fetch incoming custom calendar collections")?;
    anyhow::ensure!(
        incoming.iter().any(|collection| {
            collection.id == custom_calendar.id
                && collection.owner_account_id == fixture.account_id
                && !collection.is_owned
                && collection.rights.may_read
                && !collection.rights.may_write
        }),
        "custom shared calendar must be visible to read grantee"
    );

    let read_only_create = storage
        .create_accessible_event(
            grantee_account_id,
            Some(&custom_calendar.id),
            runtime_calendar_event_input(grantee_account_id, None, "Read-only write should fail"),
        )
        .await;
    expect_anyhow_failure("read-only custom calendar event create", read_only_create)?;

    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id: fixture.account_id,
                grantee_email: grantee_email.clone(),
                calendar_id: Some(calendar_id),
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "calendar-share-upsert",
                "runtime custom calendar write grant",
            ),
        )
        .await
        .context("upgrade custom calendar grant to write")?;

    let before_event_sequence = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(sequence) FROM canonical_change_journal WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id)
    .fetch_one(pool)
    .await
    .context("load custom calendar event starting canonical sequence")?
    .unwrap_or(0);

    let event = storage
        .create_accessible_event(
            grantee_account_id,
            Some(&custom_calendar.id),
            runtime_calendar_event_input(grantee_account_id, None, "Writable custom event"),
        )
        .await
        .context("create event through custom calendar write grant")?;
    anyhow::ensure!(
        event.owner_account_id == fixture.account_id && event.collection_id == custom_calendar.id,
        "custom calendar grantee writes must land in the owner's canonical calendar"
    );

    let grantee_woken = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM canonical_change_journal
            WHERE tenant_id = $1
              AND category = 'calendar'
              AND sequence > $2
              AND (
                  principal_account_ids @> ARRAY[$3]::uuid[]
                  OR account_ids @> ARRAY[$3]::uuid[]
              )
        )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(before_event_sequence)
    .bind(grantee_account_id)
    .fetch_one(pool)
    .await
    .context("check custom calendar event wakeup audience")?;
    anyhow::ensure!(
        grantee_woken,
        "custom calendar event changes must wake affected grantees"
    );

    storage
        .delete_calendar_collection_grant(
            fixture.account_id,
            &custom_calendar.id,
            grantee_account_id,
            audit(
                &fixture.account_email,
                "calendar-share-delete",
                "runtime custom calendar revoke",
            ),
        )
        .await
        .context("delete custom calendar grant")?;

    let after_revoke = storage
        .fetch_accessible_calendar_collections(grantee_account_id)
        .await
        .context("fetch incoming custom calendars after revoke")?;
    anyhow::ensure!(
        after_revoke
            .iter()
            .all(|collection| collection.id != custom_calendar.id),
        "revoked custom calendar grant must remove calendar visibility"
    );
    let events_after_revoke = storage
        .fetch_accessible_events_by_ids(grantee_account_id, &[event.id])
        .await
        .context("fetch shared event after calendar revoke")?;
    anyhow::ensure!(
        events_after_revoke.is_empty(),
        "revoked custom calendar grant must remove event visibility"
    );

    Ok(())
}

fn runtime_calendar_event_input(
    account_id: Uuid,
    id: Option<Uuid>,
    title: &str,
) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id,
        account_id,
        uid: String::new(),
        date: "2026-06-06".to_string(),
        time: "09:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: title.to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "{}".to_string(),
        notes: String::new(),
        body_html: String::new(),
    }
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

    let store_identity = sqlx::query(
        r#"
        SELECT replica_guid, next_global_counter
        FROM mapi_store_identity
        WHERE singleton = TRUE
        "#,
    )
    .fetch_one(pool)
    .await
    .context("load MAPI store identity before server move")?;
    let replica_guid: Uuid = store_identity.try_get("replica_guid")?;
    let source_global_counter =
        u64::try_from(store_identity.try_get::<i64, _>("next_global_counter")?)
            .context("convert next MAPI global counter before server move")?;
    let source_mapi_object_id = mapi_store_id(source_global_counter) as i64;
    let source_key = mapi_xid(replica_guid, source_global_counter);
    let mut predecessor_change_list = Vec::with_capacity(source_key.len() + 1);
    predecessor_change_list.push(source_key.len() as u8);
    predecessor_change_list.extend_from_slice(&source_key);
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (tenant_id, account_id, replica_guid)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, account_id) DO NOTHING
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(replica_guid)
    .execute(pool)
    .await
    .context("seed local MAPI mailbox replica before server move")?;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'message', $3, $4, $5, $6, $6, $6, $4, $7)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .bind(source_global_counter as i64)
    .bind(source_mapi_object_id)
    .bind(&source_key)
    .bind(&predecessor_change_list)
    .execute(pool)
    .await
    .context("seed active normal-message MAPI identity before server move")?;
    sqlx::query("UPDATE mapi_store_identity SET next_global_counter = $1 WHERE singleton = TRUE")
        .bind((source_global_counter + 1) as i64)
        .execute(pool)
        .await
        .context("advance MAPI global counter after seeded message identity")?;

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

    let moved_email = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch submitted JMAP message after moving it out of Sent")?
        .into_iter()
        .next()
        .context("submitted message missing after moving it out of Sent")?;
    anyhow::ensure!(
        moved_email.sender_authorization_kind == "self" && moved_email.delivery_status == "queued",
        "submission provenance must survive replacement of the original Sent membership"
    );

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

    let search_memberships = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE mailbox_message_id = $4) AS source_count,
            COUNT(*) FILTER (WHERE mailbox_message_id = $5) AS target_count
        FROM mail_search_documents
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .bind(source_membership_id)
    .bind(target_membership_id)
    .fetch_one(pool)
    .await
    .context("load search membership projection after move")?;
    anyhow::ensure!(
        search_memberships.try_get::<i64, _>("source_count")? == 0
            && search_memberships.try_get::<i64, _>("target_count")? == 1,
        "move must rekey the search document from the source to target membership"
    );

    let active_identity = sqlx::query(
        r#"
        SELECT mapi_object_id, source_key
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
          AND deleted_at IS NULL
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load active MAPI message identity after server move")?;
    let target_mapi_object_id = active_identity.try_get::<i64, _>("mapi_object_id")?;
    let target_source_key = active_identity.try_get::<Vec<u8>, _>("source_key")?;
    anyhow::ensure!(
        target_mapi_object_id != source_mapi_object_id && target_source_key != source_key,
        "server move must create a distinct active MAPI message MID and SourceKey"
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
        SELECT imap_uid, mapi_object_id, reason
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
            && tombstone.try_get::<i64, _>("mapi_object_id")? == source_mapi_object_id
            && tombstone.try_get::<String, _>("reason")? == "move",
        "move tombstone must preserve the original source UID, MAPI MID, and reason"
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

    let moved_identity = sqlx::query(
        r#"
        SELECT
            (summary_json ->> 'oldMapiObjectId')::BIGINT AS old_mapi_object_id,
            (summary_json ->> 'newMapiObjectId')::BIGINT AS new_mapi_object_id
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND cursor > $3
          AND modseq > $4
          AND object_kind = 'mailbox_message'
          AND change_kind = 'moved'
          AND summary_json ->> 'sourceMailboxMessageId' = $5
          AND summary_json ->> 'targetMailboxMessageId' = $6
        LIMIT 1
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
    .context("load MAPI move identity snapshot from change log")?;
    anyhow::ensure!(
        moved_identity.try_get::<i64, _>("old_mapi_object_id")? == source_mapi_object_id
            && moved_identity.try_get::<i64, _>("new_mapi_object_id")? == target_mapi_object_id
            && moved_identity.try_get::<i64, _>("old_mapi_object_id")?
                != moved_identity.try_get::<i64, _>("new_mapi_object_id")?,
        "MAPI move replay must retain distinct old and new message MIDs"
    );

    let copied_mailbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO mailboxes (
            id, tenant_id, account_id, role, display_name, sort_order,
            uid_validity, uid_next, recoverable_items_retention_days
        )
        VALUES ($1, $2, $3, 'custom', 'Runtime Copy Target', 30, 9002, 77, 3)
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

    let copied_membership = sqlx::query(
        r#"
        SELECT id, imap_uid
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
    .bind(copied_mailbox_id)
    .bind(submitted.message_id)
    .fetch_one(pool)
    .await
    .context("load copied membership before IMAP expunge")?;
    let copied_membership_id = copied_membership.try_get::<Uuid, _>("id")?;
    let copied_imap_uid = copied_membership.try_get::<i64, _>("imap_uid")?;
    sqlx::query("UPDATE messages SET legal_hold = TRUE WHERE tenant_id = $1 AND id = $2")
        .bind(fixture.tenant_id)
        .bind(submitted.message_id)
        .execute(pool)
        .await
        .context("enable message legal hold before IMAP expunge")?;
    storage
        .update_imap_flags(
            fixture.account_id,
            copied_mailbox_id,
            &[submitted.message_id],
            None,
            None,
            Some(true),
            None,
        )
        .await
        .context("mark copied membership deleted before IMAP expunge")?;
    storage
        .expunge_imap_deleted(
            fixture.account_id,
            copied_mailbox_id,
            &[submitted.message_id],
            audit(
                "alice@example.test",
                "imap-expunge",
                "runtime drift IMAP expunge",
            ),
        )
        .await
        .context("expunge copied IMAP membership")?;

    let expunged_projection = sqlx::query(
        r#"
        SELECT
            membership.visibility,
            membership.imap_uid,
            EXISTS (
                SELECT 1
                FROM mail_search_documents search
                WHERE search.tenant_id = membership.tenant_id
                  AND search.account_id = membership.account_id
                  AND search.mailbox_message_id = membership.id
            ) AS expunged_search_exists,
            EXISTS (
                SELECT 1
                FROM mail_search_documents search
                WHERE search.tenant_id = membership.tenant_id
                  AND search.account_id = membership.account_id
                  AND search.mailbox_message_id = $4
            ) AS remaining_search_exists,
            EXISTS (
                SELECT 1
                FROM tombstones tombstone
                WHERE tombstone.tenant_id = membership.tenant_id
                  AND tombstone.account_id = membership.account_id
                  AND tombstone.mailbox_message_id = membership.id
                  AND tombstone.reason = 'expunge'
            ) AS expunge_tombstone_exists
        FROM mailbox_messages membership
        WHERE membership.tenant_id = $1
          AND membership.account_id = $2
          AND membership.id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(copied_membership_id)
    .bind(target_membership_id)
    .fetch_one(pool)
    .await
    .context("load membership and search projection after IMAP expunge")?;
    anyhow::ensure!(
        expunged_projection.try_get::<String, _>("visibility")? == "expunged"
            && expunged_projection.try_get::<i64, _>("imap_uid")? == copied_imap_uid
            && !expunged_projection.try_get::<bool, _>("expunged_search_exists")?
            && expunged_projection.try_get::<bool, _>("remaining_search_exists")?
            && expunged_projection.try_get::<bool, _>("expunge_tombstone_exists")?,
        "IMAP expunge must retain source history, delete only its search projection, and preserve other visible membership projections"
    );

    let after_expunge = storage
        .fetch_jmap_emails(fixture.account_id, &[submitted.message_id])
        .await
        .context("fetch JMAP Email after one membership is expunged")?
        .into_iter()
        .next()
        .context("remaining visible membership missing after IMAP expunge")?;
    anyhow::ensure!(
        after_expunge.mailbox_ids == vec![target_mailbox_id],
        "IMAP expunge must leave the message visible through its other mailbox membership"
    );

    let recoverable = sqlx::query(
        r#"
        SELECT
            recoverable.id,
            recoverable.source_mailbox_id,
            recoverable.source_imap_uid,
            recoverable.recoverable_folder,
            recoverable.delete_kind,
            recoverable.status,
            recoverable.legal_hold,
            recoverable.created_by_protocol,
            recoverable.retained_until > NOW() + INTERVAL '2 days'
                AND recoverable.retained_until <= NOW() + INTERVAL '4 days'
                AS retention_matches,
            EXISTS (
                SELECT 1
                FROM mail_change_log log
                WHERE log.tenant_id = recoverable.tenant_id
                  AND log.account_id = recoverable.account_id
                  AND log.object_kind = 'recoverable_item'
                  AND log.object_id = recoverable.id
                  AND log.change_kind = 'created'
                  AND log.summary_json ->> 'sourceMailboxMessageId' = $4
            ) AS created_change_exists
        FROM recoverable_items recoverable
        WHERE recoverable.tenant_id = $1
          AND recoverable.account_id = $2
          AND recoverable.source_mailbox_message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(copied_membership_id)
    .bind(copied_membership_id.to_string())
    .fetch_one(pool)
    .await
    .context("load recoverable item after IMAP expunge")?;
    let recoverable_item_id = recoverable.try_get::<Uuid, _>("id")?;
    anyhow::ensure!(
        recoverable.try_get::<Uuid, _>("source_mailbox_id")? == copied_mailbox_id
            && recoverable.try_get::<i64, _>("source_imap_uid")? == copied_imap_uid
            && recoverable.try_get::<String, _>("recoverable_folder")? == "deletions"
            && recoverable.try_get::<String, _>("delete_kind")? == "expunge"
            && recoverable.try_get::<String, _>("status")? == "active"
            && recoverable.try_get::<bool, _>("legal_hold")?
            && recoverable.try_get::<String, _>("created_by_protocol")? == "imap"
            && recoverable.try_get::<bool, _>("retention_matches")?
            && recoverable.try_get::<bool, _>("created_change_exists")?,
        "IMAP expunge must create canonical recoverable state with effective retention, legal hold, IMAP provenance, and durable replay"
    );

    let restored = storage
        .restore_recoverable_item(
            fixture.account_id,
            recoverable_item_id,
            Some(copied_mailbox_id),
            audit(
                "alice@example.test",
                "imap-restore-recoverable",
                "runtime drift restore IMAP expunge",
            ),
        )
        .await
        .context("restore recoverable IMAP-expunged membership")?;
    anyhow::ensure!(
        restored.id == submitted.message_id
            && restored.mailbox_ids.contains(&target_mailbox_id)
            && restored.mailbox_ids.contains(&copied_mailbox_id),
        "restoring an IMAP-expunged membership must preserve the other membership and recreate the source mailbox membership"
    );
    let restored_projection = sqlx::query(
        r#"
        SELECT
            recoverable.status,
            recoverable.restored_mailbox_message_id IS NOT NULL AS restored_membership_exists,
            EXISTS (
                SELECT 1
                FROM mail_search_documents search
                WHERE search.tenant_id = recoverable.tenant_id
                  AND search.account_id = recoverable.account_id
                  AND search.mailbox_message_id = recoverable.restored_mailbox_message_id
            ) AS restored_search_exists
        FROM recoverable_items recoverable
        WHERE recoverable.tenant_id = $1
          AND recoverable.account_id = $2
          AND recoverable.id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(recoverable_item_id)
    .fetch_one(pool)
    .await
    .context("load restored IMAP recoverable projection")?;
    anyhow::ensure!(
        restored_projection.try_get::<String, _>("status")? == "restored"
            && restored_projection.try_get::<bool, _>("restored_membership_exists")?
            && restored_projection.try_get::<bool, _>("restored_search_exists")?,
        "IMAP recoverable restore must publish the restored membership and its search projection"
    );

    Ok(())
}

async fn exercise_mapi_cross_protocol_interoperability_gate(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let draft_internet_message_id = format!("<mapi-draft-{}@example.test>", Uuid::new_v4());
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
                bcc: Vec::new(),
                subject: "MAPI canonical draft gate".to_string(),
                body_text: "MAPI draft canonical body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(draft_internet_message_id.clone()),
                mime_blob_ref: None,
                size_octets: 128,
                unread: Some(false),
                flagged: Some(true),
                replace_attachments: false,
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
    storage
        .add_message_attachment(
            fixture.account_id,
            draft.message_id,
            AttachmentUploadInput {
                file_name: "draft-submit.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                is_scheduling_body: false,
                blob_bytes: b"%PDF-draft-submit".to_vec(),
            },
            audit(
                "alice@example.test",
                "draft-attachment-add",
                "draft-submit.pdf",
            ),
        )
        .await
        .context("add canonical attachment to MAPI draft")?
        .context("canonical MAPI draft must accept its attachment")?;
    let draft_response_calendar = format!(
        concat!(
            "BEGIN:VCALENDAR\r\n",
            "VERSION:2.0\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "UID:runtime-draft-response@example.test\r\n",
            "DTSTAMP:20260824T080000Z\r\n",
            "ATTENDEE;PARTSTAT=ACCEPTED:mailto:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email
    );
    storage
        .add_message_attachment(
            fixture.account_id,
            draft.message_id,
            AttachmentUploadInput {
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=REPLY; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                is_scheduling_body: true,
                blob_bytes: draft_response_calendar.into_bytes(),
            },
            audit(
                "alice@example.test",
                "draft-scheduling-body-add",
                "response.ics",
            ),
        )
        .await
        .context("add scheduling MIME body to MAPI draft")?
        .context("canonical MAPI draft must accept its scheduling MIME body")?;

    let edited_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: Some(draft.message_id),
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
                bcc: Vec::new(),
                subject: "MAPI canonical draft gate edited".to_string(),
                body_text: "MAPI draft canonical body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(draft_internet_message_id),
                mime_blob_ref: None,
                size_octets: 128,
                unread: Some(false),
                flagged: Some(true),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "mapi-edit-draft-subject",
                "MAPI draft subject-only edit",
            ),
        )
        .await
        .context("edit only the MAPI draft subject while retaining persisted attachments")?;
    anyhow::ensure!(
        edited_draft.message_id == draft.message_id,
        "a MAPI draft edit must retain the canonical message identity"
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
            && draft_jmap.subject == "MAPI canonical draft gate edited"
            && !draft_jmap.unread
            && draft_jmap.flagged
            && draft_jmap.bcc.is_empty()
            && draft_jmap.calendar_meeting_response.is_some(),
        "JMAP projection must expose the edited MAPI draft and its authorized meeting response: {draft_jmap:?}"
    );
    let persisted_draft_state = sqlx::query(
        r#"
        SELECT classification.classification,
               classification.needs_reclassification,
               message.authorized_calendar_response_content_sha256,
               scheduling_blob.content_sha256 AS scheduling_content_sha256,
               (SELECT COUNT(*) FROM attachments
                WHERE tenant_id = message.tenant_id
                  AND account_id = $3
                  AND message_id = message.id) AS attachment_count,
               (SELECT COUNT(*) FROM mime_parts
                WHERE tenant_id = message.tenant_id
                  AND message_id = message.id
                  AND is_scheduling_body) AS scheduling_part_count
        FROM messages message
        JOIN calendar_mail_classifications classification
          ON classification.tenant_id = message.tenant_id
         AND classification.message_id = message.id
        JOIN mime_parts scheduling_part
          ON scheduling_part.tenant_id = classification.tenant_id
         AND scheduling_part.message_id = classification.message_id
         AND scheduling_part.id = classification.scheduling_mime_part_id
        JOIN blobs scheduling_blob
          ON scheduling_blob.tenant_id = scheduling_part.tenant_id
         AND scheduling_blob.domain_id = scheduling_part.domain_id
         AND scheduling_blob.id = scheduling_part.blob_id
         AND scheduling_blob.blob_kind = scheduling_part.blob_kind
        WHERE message.tenant_id = $1 AND message.id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(draft.message_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load persisted attachment and response state after MAPI draft subject edit")?;
    anyhow::ensure!(
        persisted_draft_state.try_get::<String, _>("classification")? == "response"
            && !persisted_draft_state.try_get::<bool, _>("needs_reclassification")?
            && persisted_draft_state.try_get::<Option<String>, _>(
                "authorized_calendar_response_content_sha256",
            )? == Some(persisted_draft_state.try_get::<String, _>("scheduling_content_sha256")?)
            && persisted_draft_state.try_get::<i64, _>("attachment_count")? == 2
            && persisted_draft_state.try_get::<i64, _>("scheduling_part_count")? == 1,
        "a subject-only Draft edit must retain both attachments and bind the authorized response to the exact selected ICS blob"
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
        "IMAP projection must expose canonical MAPI draft flags"
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
    let old_draft_search_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_search_documents
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(draft.message_id)
    .fetch_one(pool)
    .await
    .context("count source draft search projections after MAPI submit")?;
    anyhow::ensure!(
        old_draft_search_count == 0,
        "MAPI draft submit must delete the source draft search projection"
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
            && sent_draft_jmap.has_attachments
            && sent_draft_jmap.bcc.is_empty(),
        "MAPI draft submit must retain canonical attachments in Sent through JMAP"
    );
    let sent_draft_raw = storage
        .fetch_jmap_message_blob(fixture.account_id, draft_submission.message_id)
        .await
        .context("fetch raw MAPI-submitted draft")?
        .context("MAPI-submitted draft raw message is missing")?;
    let sent_draft_raw = String::from_utf8_lossy(&sent_draft_raw.blob_bytes);
    anyhow::ensure!(
        sent_draft_raw.contains("Content-Type: text/calendar; method=REPLY; charset=UTF-8")
            && sent_draft_raw.contains("METHOD:REPLY")
            && sent_draft_raw.contains("draft-submit.pdf"),
        "canonical Drafts submission hydration must preserve the selected scheduling MIME body and ordinary attachment"
    );

    let outbox_mailbox_id = storage
        .ensure_imap_mailboxes(fixture.account_id)
        .await
        .context("ensure Outbox for canonical MAPI submission source")?
        .into_iter()
        .find(|mailbox| mailbox.role == "outbox")
        .map(|mailbox| mailbox.id)
        .context("canonical Outbox mailbox is missing")?;
    let outbox_calendar = format!(
        concat!(
            "BEGIN:VCALENDAR\r\n",
            "VERSION:2.0\r\n",
            "METHOD:COUNTER\r\n",
            "BEGIN:VEVENT\r\n",
            "UID:runtime-outbox-counter@example.test\r\n",
            "DTSTAMP:20260824T081000Z\r\n",
            "DTSTART:20260824T091000Z\r\n",
            "DTEND:20260824T101000Z\r\n",
            "ATTENDEE;PARTSTAT=DECLINED:mailto:{}\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        fixture.account_email
    );
    let outbox_source = storage
        .import_jmap_email(
            JmapImportedEmailInput {
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                mailbox_id: outbox_mailbox_id,
                source: "mapi-save-message".to_string(),
                raw_message: None,
                from_display: Some("Alice MAPI".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "organizer@example.test".to_string(),
                    display_name: Some("Organizer".to_string()),
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "New Time Proposed: canonical Outbox source".to_string(),
                body_text: "Canonical Outbox scheduling response".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!(
                    "<mapi-outbox-source-{}@example.test>",
                    Uuid::new_v4()
                )),
                mime_blob_ref: format!("mapi-save-message:{}", Uuid::new_v4()),
                size_octets: outbox_calendar.len() as i64,
                received_at: None,
                thread_id: None,
                attachments: vec![AttachmentUploadInput {
                    file_name: "response.ics".to_string(),
                    media_type: "text/calendar; method=COUNTER; charset=UTF-8".to_string(),
                    disposition: Some("inline".to_string()),
                    content_id: None,
                    is_scheduling_body: true,
                    blob_bytes: outbox_calendar.into_bytes(),
                }],
            },
            audit(
                "alice@example.test",
                "mapi-save-message",
                "canonical Outbox submission source",
            ),
        )
        .await
        .context("import canonical MAPI Outbox submission source")?;

    let strict_draft_delete = storage
        .delete_draft_message(
            fixture.account_id,
            outbox_source.id,
            audit(
                "alice@example.test",
                "mapi-delete-draft",
                "Outbox must not be a public draft",
            ),
        )
        .await;
    anyhow::ensure!(
        strict_draft_delete
            .as_ref()
            .is_err_and(|error| error.to_string().contains("draft not found")),
        "public draft deletion must reject an Outbox source"
    );
    anyhow::ensure!(
        storage
            .fetch_jmap_emails(fixture.account_id, &[outbox_source.id])
            .await
            .context("fetch Outbox source after rejected public draft deletion")?
            .len()
            == 1,
        "rejected public draft deletion must leave the Outbox source visible"
    );

    let outbox_submission = storage
        .submit_message(
            SubmitMessageInput {
                draft_message_id: Some(outbox_source.id),
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "mapi".to_string(),
                from_display: outbox_source.from_display.clone(),
                from_address: outbox_source.from_address.clone(),
                sender_display: outbox_source.sender_display.clone(),
                sender_address: outbox_source.sender_address.clone(),
                to: vec![SubmittedRecipientInput {
                    address: "organizer@example.test".to_string(),
                    display_name: Some("Organizer".to_string()),
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: outbox_source.subject.clone(),
                body_text: outbox_source.body_text.clone(),
                body_html_sanitized: outbox_source.body_html_sanitized.clone(),
                internet_message_id: outbox_source.internet_message_id.clone(),
                mime_blob_ref: None,
                size_octets: outbox_source.size_octets,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "mapi-submit-message",
                "canonical Outbox submission source",
            ),
        )
        .await
        .context("submit canonical MAPI Outbox source")?;
    anyhow::ensure!(
        storage
            .fetch_jmap_emails(fixture.account_id, &[outbox_source.id])
            .await
            .context("fetch old Outbox projection after canonical submit")?
            .is_empty(),
        "canonical submission must remove its Outbox source"
    );
    let sent_outbox_raw = storage
        .fetch_jmap_message_blob(fixture.account_id, outbox_submission.message_id)
        .await
        .context("fetch raw MAPI-submitted Outbox source")?
        .context("MAPI-submitted Outbox raw message is missing")?;
    let sent_outbox_raw = String::from_utf8_lossy(&sent_outbox_raw.blob_bytes);
    anyhow::ensure!(
        sent_outbox_raw.contains("Content-Type: text/calendar; method=COUNTER; charset=UTF-8")
            && sent_outbox_raw.contains("METHOD:COUNTER"),
        "canonical Outbox submission hydration must preserve the selected scheduling MIME body"
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
                replace_attachments: false,
                attachments: vec![AttachmentUploadInput {
                    file_name: "mapi-gate.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    is_scheduling_body: false,
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
    let updated_sent_mailbox = storage
        .fetch_jmap_mailboxes(fixture.account_id)
        .await
        .context("fetch Sent mailbox after MAPI flag mutation")?
        .into_iter()
        .find(|mailbox| mailbox.id == submitted.sent_mailbox_id)
        .context("Sent mailbox missing after MAPI flag mutation")?;
    anyhow::ensure!(
        updated_sent_mailbox.unread_emails == 1,
        "mailbox unread count must track the canonical read state changed through MAPI"
    );

    Ok(())
}

async fn exercise_outbound_meeting_request_correlation(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    storage
        .fetch_accessible_calendar_collections(fixture.account_id)
        .await
        .context("ensure default calendar for outbound request correlation")?;
    let attendee = "meeting-attendee@example.test";
    let uid = format!("outbound-request-{}@example.test", Uuid::new_v4());
    create_request_correlation_event(
        storage,
        fixture,
        None,
        &uid,
        "2026-09-10",
        "09:00",
        3,
        "confirmed",
        "",
        &fixture.account_email,
        attendee,
    )
    .await?;

    let persisted_input = request_submission_input(
        fixture,
        None,
        &uid,
        "20260910T090000Z",
        "20260910T100000Z",
        3,
        &fixture.account_email,
        attendee,
    );
    let persisted_draft = storage
        .save_draft_message(
            persisted_input,
            audit(
                "alice@example.test",
                "mapi-save-request",
                "persisted meeting request correlation",
            ),
        )
        .await
        .context("save persisted meeting REQUEST source")?;
    let persisted_draft_classification = sqlx::query(
        r#"
        SELECT classification, needs_reclassification, classification_generation,
               (SELECT COUNT(*) FROM mime_parts
                WHERE tenant_id = classification.tenant_id
                  AND message_id = classification.message_id
                  AND is_scheduling_body) AS scheduling_part_count,
               (SELECT COUNT(*) FROM calendar_mail_classification_projections
                WHERE tenant_id = classification.tenant_id
                  AND account_id = $3
                  AND message_id = classification.message_id
                  AND applied_generation = 1) AS applied_projection_count
        FROM calendar_mail_classifications classification
        WHERE classification.tenant_id = $1 AND classification.message_id = $2
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(persisted_draft.message_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        persisted_draft_classification.try_get::<String, _>("classification")? == "request"
            && !persisted_draft_classification.try_get::<bool, _>("needs_reclassification")?
            && persisted_draft_classification.try_get::<i64, _>("classification_generation")? == 1
            && persisted_draft_classification.try_get::<i64, _>("scheduling_part_count")? == 1
            && persisted_draft_classification.try_get::<i64, _>("applied_projection_count")? == 1,
        "a newly saved REQUEST Draft must persist one clean applied scheduling classification"
    );
    let persisted_submission = storage
        .submit_draft_message(
            fixture.account_id,
            persisted_draft.message_id,
            fixture.account_id,
            "mapi",
            audit(
                "alice@example.test",
                "mapi-submit-request",
                "persisted meeting request correlation",
            ),
        )
        .await
        .context("submit exact persisted meeting REQUEST source")?;
    let persisted_state = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*) FROM submission_queue
             WHERE tenant_id = $1 AND id = $2) AS queue_count,
            (SELECT COUNT(*) FROM mailbox_messages
             WHERE tenant_id = $1 AND account_id = $3 AND message_id = $4
               AND visibility = 'visible') AS source_visible_count,
            (SELECT classification FROM calendar_mail_classifications
             WHERE tenant_id = $1 AND message_id = $5) AS classification,
            (SELECT needs_reclassification FROM calendar_mail_classifications
             WHERE tenant_id = $1 AND message_id = $5) AS needs_reclassification,
            (SELECT classification_generation FROM calendar_mail_classifications
             WHERE tenant_id = $1 AND message_id = $5) AS classification_generation,
            (SELECT COUNT(*) FROM mime_parts
             WHERE tenant_id = $1 AND message_id = $5
               AND is_scheduling_body) AS scheduling_part_count,
            (SELECT COUNT(*) FROM calendar_mail_classification_projections
             WHERE tenant_id = $1 AND account_id = $3 AND message_id = $5
               AND applied_generation = 1) AS applied_projection_count
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(persisted_submission.outbound_queue_id)
    .bind(fixture.account_id)
    .bind(persisted_draft.message_id)
    .bind(persisted_submission.message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        persisted_state.try_get::<i64, _>("queue_count")? == 1
            && persisted_state.try_get::<i64, _>("source_visible_count")? == 0
            && persisted_state.try_get::<String, _>("classification")? == "request"
            && !persisted_state.try_get::<bool, _>("needs_reclassification")?
            && persisted_state.try_get::<i64, _>("classification_generation")? == 1
            && persisted_state.try_get::<i64, _>("scheduling_part_count")? == 1
            && persisted_state.try_get::<i64, _>("applied_projection_count")? == 1,
        "a correlated persisted REQUEST must queue once, expunge its source, and persist one clean applied Sent scheduling classification"
    );

    let higher = request_submission_input(
        fixture,
        None,
        &uid,
        "20260910T090000Z",
        "20260910T100000Z",
        4,
        &fixture.account_email,
        attendee,
    );
    storage
        .submit_message(
            higher,
            audit(
                "alice@example.test",
                "mapi-submit-request",
                "higher sequence meeting request correlation",
            ),
        )
        .await
        .context("higher REQUEST sequence should correlate")?;

    let stale_draft = storage
        .save_draft_message(
            request_submission_input(
                fixture,
                None,
                &uid,
                "20260910T090000Z",
                "20260910T100000Z",
                2,
                &fixture.account_email,
                attendee,
            ),
            audit(
                "alice@example.test",
                "mapi-save-request",
                "stale persisted meeting request",
            ),
        )
        .await?;
    let before_stale = outbound_submission_counts(pool, fixture).await?;
    anyhow::ensure!(
        storage
            .submit_draft_message(
                fixture.account_id,
                stale_draft.message_id,
                fixture.account_id,
                "mapi",
                audit(
                    "alice@example.test",
                    "mapi-submit-request",
                    "stale persisted meeting request",
                ),
            )
            .await
            .is_err(),
        "a REQUEST sequence lower than the Event sequence must fail"
    );
    let stale_visible = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(stale_draft.message_id)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        stale_visible == 1 && outbound_submission_counts(pool, fixture).await? == before_stale,
        "a rejected persisted REQUEST must preserve its source and create no Sent/queue state"
    );

    for (label, input) in [
        (
            "request before Event Save",
            request_submission_input(
                fixture,
                None,
                &format!("missing-{}@example.test", Uuid::new_v4()),
                "20260911T090000Z",
                "20260911T100000Z",
                0,
                &fixture.account_email,
                attendee,
            ),
        ),
        (
            "interval mismatch",
            request_submission_input(
                fixture,
                None,
                &uid,
                "20260910T110000Z",
                "20260910T120000Z",
                3,
                &fixture.account_email,
                attendee,
            ),
        ),
        (
            "request organizer mismatch",
            request_submission_input(
                fixture,
                None,
                &uid,
                "20260910T090000Z",
                "20260910T100000Z",
                3,
                "other-organizer@example.test",
                attendee,
            ),
        ),
        (
            "request attendee mismatch",
            request_submission_input(
                fixture,
                None,
                &uid,
                "20260910T090000Z",
                "20260910T100000Z",
                3,
                &fixture.account_email,
                "other-attendee@example.test",
            ),
        ),
    ] {
        expect_request_rejected_without_submission(storage, pool, fixture, input, label).await?;
    }

    for (label, status, recurrence_rule, move_to_deleted) in [
        ("cancelled Event", "cancelled", "", false),
        ("deleted Event", "confirmed", "", true),
        ("recurring Event", "confirmed", "FREQ=DAILY", false),
    ] {
        let candidate_uid = format!(
            "{}-{}@example.test",
            label.replace(' ', "-"),
            Uuid::new_v4()
        );
        let event_id = create_request_correlation_event(
            storage,
            fixture,
            None,
            &candidate_uid,
            "2026-09-12",
            "09:00",
            0,
            status,
            recurrence_rule,
            &fixture.account_email,
            attendee,
        )
        .await?;
        if move_to_deleted {
            storage
                .move_accessible_event_to_deleted_items(fixture.account_id, event_id, None)
                .await?;
        }
        expect_request_rejected_without_submission(
            storage,
            pool,
            fixture,
            request_submission_input(
                fixture,
                None,
                &candidate_uid,
                "20260912T090000Z",
                "20260912T100000Z",
                0,
                &fixture.account_email,
                attendee,
            ),
            label,
        )
        .await?;
    }

    let organizer_mismatch_uid = format!("organizer-mismatch-{}@example.test", Uuid::new_v4());
    create_request_correlation_event(
        storage,
        fixture,
        None,
        &organizer_mismatch_uid,
        "2026-09-13",
        "09:00",
        0,
        "confirmed",
        "",
        "other-organizer@example.test",
        attendee,
    )
    .await?;
    expect_request_rejected_without_submission(
        storage,
        pool,
        fixture,
        request_submission_input(
            fixture,
            None,
            &organizer_mismatch_uid,
            "20260913T090000Z",
            "20260913T100000Z",
            0,
            &fixture.account_email,
            attendee,
        ),
        "canonical organizer mismatch",
    )
    .await?;

    let duplicate_calendar = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outbound request UID collision")
        .await?;
    create_request_correlation_event(
        storage,
        fixture,
        Some(&duplicate_calendar.id),
        &uid,
        "2026-09-10",
        "09:00",
        3,
        "confirmed",
        "",
        &fixture.account_email,
        attendee,
    )
    .await?;
    expect_request_rejected_without_submission(
        storage,
        pool,
        fixture,
        request_submission_input(
            fixture,
            None,
            &uid,
            "20260910T090000Z",
            "20260910T100000Z",
            3,
            &fixture.account_email,
            attendee,
        ),
        "ambiguous duplicate Event",
    )
    .await?;

    let bcc_uid = format!("bcc-request-{}@example.test", Uuid::new_v4());
    create_request_correlation_event(
        storage,
        fixture,
        None,
        &bcc_uid,
        "2026-09-14",
        "09:00",
        0,
        "confirmed",
        "",
        &fixture.account_email,
        attendee,
    )
    .await?;
    let mut bcc_request = request_submission_input(
        fixture,
        None,
        &bcc_uid,
        "20260914T090000Z",
        "20260914T100000Z",
        0,
        &fixture.account_email,
        attendee,
    );
    bcc_request.bcc.push(SubmittedRecipientInput {
        address: "hidden-attendee@example.test".to_string(),
        display_name: None,
    });
    expect_request_rejected_without_submission(
        storage,
        pool,
        fixture,
        bcc_request,
        "scheduling Bcc",
    )
    .await?;

    Ok(())
}

async fn create_request_correlation_event(
    storage: &Storage,
    fixture: &RuntimeFixture,
    collection_id: Option<&str>,
    uid: &str,
    date: &str,
    time: &str,
    sequence: i32,
    status: &str,
    recurrence_rule: &str,
    organizer: &str,
    attendee: &str,
) -> Result<Uuid> {
    let mut input =
        runtime_calendar_event_input(fixture.account_id, None, "Outbound request correlation");
    input.uid = uid.to_string();
    input.date = date.to_string();
    input.time = time.to_string();
    input.duration_minutes = 60;
    input.sequence = sequence;
    input.status = status.to_string();
    input.recurrence_rule = recurrence_rule.to_string();
    input.organizer_json = serde_json::json!({
        "email": organizer,
        "common_name": "Organizer",
        "is_meeting": true
    })
    .to_string();
    input.attendees = attendee.to_string();
    input.attendees_json = serde_json::json!({
        "organizer": {"email": organizer, "common_name": "Organizer"},
        "attendees": [{
            "email": attendee,
            "common_name": "Attendee",
            "role": "REQ-PARTICIPANT",
            "partstat": "needs-action",
            "rsvp": true
        }]
    })
    .to_string();
    Ok(storage
        .create_accessible_event(fixture.account_id, collection_id, input)
        .await?
        .id)
}

fn request_submission_input(
    fixture: &RuntimeFixture,
    draft_message_id: Option<Uuid>,
    uid: &str,
    start: &str,
    end: &str,
    sequence: i32,
    organizer: &str,
    attendee: &str,
) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id,
        account_id: fixture.account_id,
        submitted_by_account_id: fixture.account_id,
        source: "mapi".to_string(),
        from_display: Some("Alice Drift".to_string()),
        from_address: fixture.account_email.clone(),
        sender_display: None,
        sender_address: None,
        to: vec![SubmittedRecipientInput {
            address: attendee.to_string(),
            display_name: Some("Attendee".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Outbound request correlation".to_string(),
        body_text: "Meeting request".to_string(),
        body_html_sanitized: None,
        internet_message_id: None,
        mime_blob_ref: None,
        size_octets: 512,
        unread: Some(false),
        flagged: Some(false),
        replace_attachments: false,
        attachments: vec![AttachmentUploadInput {
            file_name: "invite.ics".to_string(),
            media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
            disposition: Some("inline".to_string()),
            content_id: None,
            is_scheduling_body: true,
            blob_bytes: format!(
                concat!(
                    "BEGIN:VCALENDAR\r\n",
                    "VERSION:2.0\r\n",
                    "METHOD:REQUEST\r\n",
                    "BEGIN:VEVENT\r\n",
                    "UID:{uid}\r\n",
                    "DTSTAMP:20260824T080000Z\r\n",
                    "DTSTART:{start}\r\n",
                    "DTEND:{end}\r\n",
                    "SEQUENCE:{sequence}\r\n",
                    "ORGANIZER:mailto:{organizer}\r\n",
                    "ATTENDEE;RSVP=TRUE:mailto:{attendee}\r\n",
                    "SUMMARY:Outbound request correlation\r\n",
                    "END:VEVENT\r\n",
                    "END:VCALENDAR\r\n"
                ),
                uid = uid,
                start = start,
                end = end,
                sequence = sequence,
                organizer = organizer,
                attendee = attendee,
            )
            .into_bytes(),
        }],
    }
}

async fn outbound_submission_counts(pool: &PgPool, fixture: &RuntimeFixture) -> Result<(i64, i64)> {
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*) FROM submission_queue
             WHERE tenant_id = $1 AND account_id = $2) AS queue_count,
            (SELECT COUNT(*)
             FROM mailbox_messages membership
             JOIN mailboxes mailbox
               ON mailbox.tenant_id = membership.tenant_id
              AND mailbox.account_id = membership.account_id
              AND mailbox.id = membership.mailbox_id
             WHERE membership.tenant_id = $1
               AND membership.account_id = $2
               AND membership.visibility = 'visible'
               AND mailbox.role = 'sent') AS sent_count
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await?;
    Ok((row.try_get("queue_count")?, row.try_get("sent_count")?))
}

async fn expect_request_rejected_without_submission(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
    input: SubmitMessageInput,
    label: &str,
) -> Result<()> {
    let before = outbound_submission_counts(pool, fixture).await?;
    let result = storage
        .submit_message(
            input,
            audit("alice@example.test", "mapi-submit-request", label),
        )
        .await;
    anyhow::ensure!(result.is_err(), "{label} REQUEST unexpectedly submitted");
    anyhow::ensure!(
        outbound_submission_counts(pool, fixture).await? == before,
        "{label} REQUEST created Sent or queue state before correlation failed"
    );
    Ok(())
}

async fn exercise_atomic_submission_source_claim(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let web_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "web-client".to_string(),
                from_display: Some("Alice Web".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "old-recipient@example.test".to_string(),
                    display_name: None,
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Old saved editor subject".to_string(),
                body_text: "Old saved editor body".to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 64,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "web-save-draft",
                "atomic editor source",
            ),
        )
        .await
        .context("save source for atomic web update-and-submit")?;
    let web_submission = storage
        .submit_message(
            SubmitMessageInput {
                draft_message_id: Some(web_draft.message_id),
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "web-client".to_string(),
                from_display: Some("Alice Web".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "current-recipient@example.test".to_string(),
                    display_name: Some("Current Recipient".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![SubmittedRecipientInput {
                    address: "current-hidden@example.test".to_string(),
                    display_name: Some("Current Hidden".to_string()),
                }],
                subject: "Current unsaved editor subject".to_string(),
                body_text: "Current unsaved editor body".to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 96,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: true,
                attachments: vec![AttachmentUploadInput {
                    file_name: "current-editor.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    is_scheduling_body: false,
                    blob_bytes: b"atomic-editor-attachment-bytes".to_vec(),
                }],
            },
            audit(
                "alice@example.test",
                "web-submit-message",
                "atomic editor source",
            ),
        )
        .await
        .context("atomically update and submit web editor source")?;
    let web_raw = storage
        .fetch_jmap_message_blob(fixture.account_id, web_submission.message_id)
        .await
        .context("fetch atomic web submission raw message")?
        .context("atomic web submission raw message is missing")?;
    let web_raw = String::from_utf8_lossy(&web_raw.blob_bytes);
    anyhow::ensure!(
        web_raw.contains("Subject: Current unsaved editor subject")
            && web_raw.contains("Current unsaved editor body")
            && web_raw.contains("current-recipient@example.test")
            && !web_raw.contains("Old saved editor subject")
            && !web_raw.contains("old-recipient@example.test"),
        "web update-and-submit must send the complete current editor fields and new attachment, not the prior persisted version"
    );
    let editor_attachment = storage
        .fetch_activesync_message_attachments(fixture.account_id, web_submission.message_id)
        .await
        .context("fetch Sent attachments after atomic editor submission")?
        .into_iter()
        .find(|attachment| attachment.file_name == "current-editor.bin")
        .context("atomic editor attachment is missing from the exact Sent version")?;
    let editor_attachment_content = storage
        .fetch_activesync_attachment_content(fixture.account_id, &editor_attachment.file_reference)
        .await
        .context("fetch atomic editor Sent attachment content")?
        .context("atomic editor Sent attachment content is missing")?;
    anyhow::ensure!(
        editor_attachment_content.blob_bytes == b"atomic-editor-attachment-bytes",
        "web update-and-submit must carry the exact new editor attachment bytes into Sent"
    );
    let ordinary_sent_classification = sqlx::query(
        r#"
        SELECT classification.classification,
               classification.classification_generation,
               classification.needs_reclassification,
               COUNT(projection.message_id) AS projection_count,
               COALESCE(MAX(projection.applied_generation), 0) AS applied_generation
        FROM calendar_mail_classifications classification
        LEFT JOIN calendar_mail_classification_projections projection
          ON projection.tenant_id = classification.tenant_id
         AND projection.account_id = $2
         AND projection.message_id = classification.message_id
        WHERE classification.tenant_id = $1 AND classification.message_id = $3
        GROUP BY classification.classification, classification.classification_generation,
                 classification.needs_reclassification
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(web_submission.message_id)
    .fetch_one(pool)
    .await
    .context("load initial ordinary Sent calendar-mail classification")?;
    anyhow::ensure!(
        ordinary_sent_classification.try_get::<String, _>("classification")? == "none"
            && ordinary_sent_classification
                .try_get::<i64, _>("classification_generation")?
                == 1
            && !ordinary_sent_classification.try_get::<bool, _>("needs_reclassification")?
            && ordinary_sent_classification.try_get::<i64, _>("projection_count")? == 1
            && ordinary_sent_classification.try_get::<i64, _>("applied_generation")? == 1,
        "ordinary Sent mail must persist and acknowledge its initial none classification without lazy repair"
    );
    let protected_bcc = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM submission_recipients
        WHERE tenant_id = $1
          AND submission_queue_id = $2
          AND role = 'bcc'
          AND address = 'current-hidden@example.test'
          AND protected_metadata = TRUE
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(web_submission.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("count protected Bcc on atomic web submission")?;
    anyhow::ensure!(
        protected_bcc == 1,
        "web update-and-submit must carry the current protected Bcc into the exact queued version"
    );

    let source_patch_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "mapi".to_string(),
                from_display: Some("Alice Source Patch".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "source-patch@example.test".to_string(),
                    display_name: None,
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Persisted source patch baseline".to_string(),
                body_text: "Persisted source patch baseline body".to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 128,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: vec![
                    AttachmentUploadInput {
                        file_name: "delete-before-submit.bin".to_string(),
                        media_type: "application/octet-stream".to_string(),
                        disposition: Some("attachment".to_string()),
                        content_id: None,
                        is_scheduling_body: false,
                        blob_bytes: b"delete-before-submit".to_vec(),
                    },
                    AttachmentUploadInput {
                        file_name: "keep-before-submit.bin".to_string(),
                        media_type: "application/octet-stream".to_string(),
                        disposition: Some("attachment".to_string()),
                        content_id: None,
                        is_scheduling_body: false,
                        blob_bytes: b"keep-before-submit".to_vec(),
                    },
                ],
            },
            audit(
                "alice@example.test",
                "mapi-save-draft",
                "atomic source patch baseline",
            ),
        )
        .await
        .context("save source for atomic selective patch submission")?;
    let retained_parent_mime_part_id = Uuid::new_v4();
    let renamed_body_part = sqlx::query(
        r#"
        UPDATE mime_parts part
        SET part_path = 'body.original'
        WHERE part.tenant_id = $1
          AND part.message_id = $2
          AND EXISTS (
              SELECT 1
              FROM message_bodies body
              WHERE body.tenant_id = part.tenant_id
                AND body.message_id = part.message_id
                AND body.mime_part_id = part.id
          )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(source_patch_draft.message_id)
    .execute(pool)
    .await
    .context("free the legacy body path for retained MIME-ancestor coverage")?;
    anyhow::ensure!(
        renamed_body_part.rows_affected() == 1,
        "the sparse MIME fixture must contain exactly one original body part"
    );
    sqlx::query(
        r#"
        INSERT INTO mime_parts (
            id, tenant_id, message_id, domain_id, parent_part_id,
            part_path, ordinal, content_type, size_octets
        )
        SELECT $1, message.tenant_id, message.id, message.domain_id, NULL,
               '1', 99, 'multipart/mixed', 0
        FROM messages message
        WHERE message.tenant_id = $2 AND message.id = $3
        "#,
    )
    .bind(retained_parent_mime_part_id)
    .bind(fixture.tenant_id)
    .bind(source_patch_draft.message_id)
    .execute(pool)
    .await
    .context("seed retained multipart ancestor on the legacy body path")?;
    let parented_attachment = sqlx::query(
        r#"
        UPDATE mime_parts part
        SET parent_part_id = $4
        FROM attachments attachment
        WHERE attachment.tenant_id = $1
          AND attachment.account_id = $2
          AND attachment.message_id = $3
          AND attachment.file_name = 'keep-before-submit.bin'
          AND part.tenant_id = attachment.tenant_id
          AND part.message_id = attachment.message_id
          AND part.id = attachment.mime_part_id
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .bind(retained_parent_mime_part_id)
    .execute(pool)
    .await
    .context("attach the retained MIME leaf to its multipart ancestor")?;
    anyhow::ensure!(
        parented_attachment.rows_affected() == 1,
        "the sparse MIME fixture must parent exactly one retained attachment"
    );
    let delete_attachment_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM attachments
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND file_name = 'delete-before-submit.bin'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .fetch_one(pool)
    .await
    .context("load attachment selected for atomic source deletion")?;
    let source_patch_modseq = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT modseq
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .fetch_one(pool)
    .await
    .context("load source modseq for optimistic overlay claim")?
    .try_into()
    .context("source modseq is outside the canonical unsigned range")?;
    sqlx::query(
        r#"
        INSERT INTO mapi_custom_property_values (
            tenant_id, account_id, object_kind, canonical_id,
            property_tag, property_type, property_value
        )
        VALUES
            ($1, $2, 'message', $3, $4, $5, $6),
            ($1, $2, 'message', $3, $7, $8, $9)
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .bind(i64::from(0x9000_001Fu32))
    .bind(i32::from(0x001Fu16))
    .bind(b"old-custom-value".as_slice())
    .bind(i64::from(0x9001_0003u32))
    .bind(i32::from(0x0003u16))
    .bind(b"delete-custom-value".as_slice())
    .execute(pool)
    .await
    .context("seed persisted custom Message properties for atomic source patch")?;

    let source_patch_input = SubmitMessageInput {
        draft_message_id: Some(source_patch_draft.message_id),
        account_id: fixture.account_id,
        submitted_by_account_id: fixture.account_id,
        source: "mapi".to_string(),
        from_display: Some("Alice Source Patch".to_string()),
        from_address: fixture.account_email.clone(),
        sender_display: None,
        sender_address: None,
        to: vec![SubmittedRecipientInput {
            address: "source-patch@example.test".to_string(),
            display_name: None,
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Effective source patch version".to_string(),
        body_text: "Effective source patch body".to_string(),
        body_html_sanitized: None,
        internet_message_id: None,
        mime_blob_ref: None,
        size_octets: 160,
        unread: Some(true),
        flagged: Some(true),
        replace_attachments: false,
        attachments: vec![AttachmentUploadInput {
            file_name: "append-after-gap.bin".to_string(),
            media_type: "application/octet-stream".to_string(),
            disposition: Some("attachment".to_string()),
            content_id: None,
            is_scheduling_body: false,
            blob_bytes: b"append-after-gap".to_vec(),
        }],
    };
    let stale_patch = storage
        .submit_message_with_source_patch(
            source_patch_input.clone(),
            SubmissionSourcePatch {
                expected_source_modseq: Some(source_patch_modseq + 1),
                ..Default::default()
            },
            audit(
                "alice@example.test",
                "mapi-submit-message",
                "reject stale source overlay",
            ),
        )
        .await;
    anyhow::ensure!(
        stale_patch.is_err(),
        "a source patch must reject a stale pre-claim client snapshot"
    );
    let invalid_patch = storage
        .submit_message_with_source_patch(
            source_patch_input.clone(),
            SubmissionSourcePatch {
                expected_source_modseq: Some(source_patch_modseq),
                delete_attachment_ids: vec![Uuid::new_v4()],
                ..Default::default()
            },
            audit(
                "alice@example.test",
                "mapi-submit-message",
                "reject foreign source attachment patch",
            ),
        )
        .await;
    anyhow::ensure!(
        invalid_patch.is_err(),
        "a source patch must reject an attachment id outside the claimed message"
    );
    let source_still_visible = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .fetch_one(pool)
    .await
    .context("check source visibility after rejected selective patch")?;
    anyhow::ensure!(
        source_still_visible == 1,
        "a rejected source patch must roll back without expunging its source"
    );

    let source_patch_submission = storage
        .submit_message_with_source_patch(
            source_patch_input,
            SubmissionSourcePatch {
                expected_source_modseq: Some(source_patch_modseq),
                delete_attachment_ids: vec![delete_attachment_id],
                custom_property_upserts: vec![
                    SubmissionMessageCustomPropertyInput {
                        property_tag: 0x9000_001F,
                        property_type: 0x001F,
                        property_value: b"updated-custom-value".to_vec(),
                    },
                    SubmissionMessageCustomPropertyInput {
                        property_tag: 0x9002_0102,
                        property_type: 0x0102,
                        property_value: b"new-custom-value".to_vec(),
                    },
                ],
                delete_custom_property_tags: vec![0x9001_0003],
                canonical_followup_update: Some(JmapEmailFollowupUpdate {
                    followup_flag_status: Some("flagged".to_string()),
                    followup_icon: Some(4),
                    todo_item_flags: Some(8),
                    followup_request: Some("Follow up after send".to_string()),
                    categories: Some(vec![
                        "Red".to_string(),
                        "Blue".to_string(),
                        "Red".to_string(),
                    ]),
                    ..Default::default()
                }),
            },
            audit(
                "alice@example.test",
                "mapi-submit-message",
                "atomic selective source patch",
            ),
        )
        .await
        .context("apply selective source patch and submit exact result")?;
    let source_attachment_rows = sqlx::query(
        r#"
        SELECT a.file_name, a.ordinal, part.part_path
        FROM attachments a
        JOIN mime_parts part
          ON part.tenant_id = a.tenant_id
         AND part.message_id = a.message_id
         AND part.id = a.mime_part_id
        WHERE a.tenant_id = $1 AND a.account_id = $2 AND a.message_id = $3
        ORDER BY a.ordinal, a.id
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .fetch_all(pool)
    .await
    .context("load sparse source attachment graph after selective append")?;
    let source_attachment_graph = source_attachment_rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("file_name")?,
                row.try_get::<i32, _>("ordinal")?,
                row.try_get::<String, _>("part_path")?,
            ))
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
    anyhow::ensure!(
        source_attachment_graph
            == vec![
                (
                    "keep-before-submit.bin".to_string(),
                    1,
                    "attachment.2".to_string(),
                ),
                (
                    "append-after-gap.bin".to_string(),
                    2,
                    "attachment.3".to_string(),
                ),
            ],
        "an append after selective deletion must allocate after the locked maximum ordinal without colliding with a sparse MIME graph; got {source_attachment_graph:?}"
    );
    let preserved_mime_ancestry = sqlx::query(
        r#"
        SELECT parent.id AS parent_id,
               parent.part_path AS parent_path,
               child.parent_part_id,
               body_part.part_path AS body_part_path
        FROM attachments attachment
        JOIN mime_parts child
          ON child.tenant_id = attachment.tenant_id
         AND child.message_id = attachment.message_id
         AND child.id = attachment.mime_part_id
        JOIN mime_parts parent
          ON parent.tenant_id = child.tenant_id
         AND parent.message_id = child.message_id
         AND parent.id = child.parent_part_id
        JOIN message_bodies body
          ON body.tenant_id = attachment.tenant_id
         AND body.message_id = attachment.message_id
         AND body.body_kind = 'text'
        JOIN mime_parts body_part
          ON body_part.tenant_id = body.tenant_id
         AND body_part.message_id = body.message_id
         AND body_part.id = body.mime_part_id
        WHERE attachment.tenant_id = $1
          AND attachment.account_id = $2
          AND attachment.message_id = $3
          AND attachment.file_name = 'keep-before-submit.bin'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_draft.message_id)
    .fetch_one(pool)
    .await
    .context("load retained MIME ancestry after body rewrite")?;
    anyhow::ensure!(
        preserved_mime_ancestry.try_get::<Uuid, _>("parent_id")?
            == retained_parent_mime_part_id
            && preserved_mime_ancestry.try_get::<String, _>("parent_path")? == "1"
            && preserved_mime_ancestry.try_get::<Option<Uuid>, _>("parent_part_id")?
                == Some(retained_parent_mime_part_id)
            && preserved_mime_ancestry
                .try_get::<String, _>("body_part_path")?
                .starts_with("body.text."),
        "body replacement must preserve attachment MIME ancestry and allocate a noncolliding body path"
    );
    let sent_attachment_names = sqlx::query_scalar::<_, Vec<String>>(
        r#"
        SELECT COALESCE(array_agg(file_name ORDER BY ordinal, id), ARRAY[]::TEXT[])
        FROM attachments
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_submission.message_id)
    .fetch_one(pool)
    .await
    .context("load Sent attachments after selective source patch")?;
    anyhow::ensure!(
        sent_attachment_names
            == vec![
                "keep-before-submit.bin".to_string(),
                "append-after-gap.bin".to_string(),
            ],
        "Sent must contain the exact preserved-plus-appended attachment set after selective deletion"
    );
    let sent_custom_properties = sqlx::query(
        r#"
        SELECT property_tag, property_type, property_value
        FROM mapi_custom_property_values
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'message'
          AND canonical_id = $3
        ORDER BY property_tag
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_submission.message_id)
    .fetch_all(pool)
    .await
    .context("load Sent custom Message property bag after source patch")?;
    anyhow::ensure!(
        sent_custom_properties.len() == 2
            && sent_custom_properties[0].try_get::<i64, _>("property_tag")?
                == i64::from(0x9000_001Fu32)
            && sent_custom_properties[0].try_get::<i32, _>("property_type")?
                == i32::from(0x001Fu16)
            && sent_custom_properties[0].try_get::<Vec<u8>, _>("property_value")?
                == b"updated-custom-value"
            && sent_custom_properties[1].try_get::<i64, _>("property_tag")?
                == i64::from(0x9002_0102u32)
            && sent_custom_properties[1].try_get::<Vec<u8>, _>("property_value")?
                == b"new-custom-value",
        "Sent must preserve the effective pre-existing plus patched custom Message property bag and omit deletes"
    );
    let sent_followup = sqlx::query(
        r#"
        SELECT is_seen, is_flagged, followup_flag_status, followup_icon,
               todo_item_flags, followup_request, keywords
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
          AND visibility = 'visible'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(source_patch_submission.message_id)
    .fetch_one(pool)
    .await
    .context("load effective follow-up state on patched Sent message")?;
    anyhow::ensure!(
        !sent_followup.try_get::<bool, _>("is_seen")?
            && sent_followup.try_get::<bool, _>("is_flagged")?
            && sent_followup.try_get::<String, _>("followup_flag_status")? == "flagged"
            && sent_followup.try_get::<i32, _>("followup_icon")? == 4
            && sent_followup.try_get::<i32, _>("todo_item_flags")? == 8
            && sent_followup.try_get::<String, _>("followup_request")? == "Follow up after send"
            && sent_followup.try_get::<Vec<String>, _>("keywords")?
                == vec!["Blue".to_string(), "Red".to_string()],
        "Sent must preserve the effective canonical follow-up state from the claimed source"
    );

    let attachment_race_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "jmap".to_string(),
                from_display: Some("Alice Attachment Race".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "attachment-race@example.test".to_string(),
                    display_name: None,
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Attachment source claim race".to_string(),
                body_text: "The queued version must match the attachment mutation outcome"
                    .to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 96,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "jmap-save-draft",
                "attachment source claim race",
            ),
        )
        .await
        .context("save source for attachment mutation race")?;
    let add_attachment = storage.add_message_attachment(
        fixture.account_id,
        attachment_race_draft.message_id,
        AttachmentUploadInput {
            file_name: "attachment-race.bin".to_string(),
            media_type: "application/octet-stream".to_string(),
            disposition: Some("attachment".to_string()),
            content_id: None,
            is_scheduling_body: false,
            blob_bytes: b"attachment-source-claim-race-bytes".to_vec(),
        },
        audit(
            "alice@example.test",
            "jmap-add-attachment",
            "attachment source claim race",
        ),
    );
    let submit_attachment_source = storage.submit_draft_message(
        fixture.account_id,
        attachment_race_draft.message_id,
        fixture.account_id,
        "jmap",
        audit(
            "alice@example.test",
            "jmap-submit-draft",
            "attachment source claim race",
        ),
    );
    let (attachment_result, attachment_submission) =
        tokio::join!(add_attachment, submit_attachment_source);
    if let Err(error) = attachment_result.as_ref() {
        anyhow::ensure!(
            error
                .to_string()
                .contains("message not found after attachment creation"),
            "attachment mutation race returned an unexpected error: {error:#}"
        );
    }
    let attachment_submission = attachment_submission
        .context("submit the source participating in the attachment mutation race")?;
    let source_attachment_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM attachments
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND file_name = 'attachment-race.bin'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(attachment_race_draft.message_id)
    .fetch_one(pool)
    .await
    .context("count source attachments after attachment mutation race")?;
    let sent_attachment_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM attachments
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND file_name = 'attachment-race.bin'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(attachment_submission.message_id)
    .fetch_one(pool)
    .await
    .context("count Sent attachments after attachment mutation race")?;
    anyhow::ensure!(
        source_attachment_count <= 1 && source_attachment_count == sent_attachment_count,
        "a concurrent attachment mutation must either precede the claim and appear in Sent, or lose visibility without mutating the source"
    );

    let recipient_race_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "jmap".to_string(),
                from_display: Some("Alice Recipient Race".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "old-race-recipient@example.test".to_string(),
                    display_name: None,
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Recipient source claim race".to_string(),
                body_text: "The queue envelope must match the claimed recipient version"
                    .to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 96,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "jmap-save-draft",
                "recipient source claim race",
            ),
        )
        .await
        .context("save source for recipient mutation race")?;
    let replacement_to = vec![SubmittedRecipientInput {
        address: "new-race-recipient@example.test".to_string(),
        display_name: None,
    }];
    let replace_recipients = storage.replace_message_recipients(
        fixture.account_id,
        recipient_race_draft.message_id,
        &replacement_to,
        &[],
        &[],
        audit(
            "alice@example.test",
            "jmap-replace-recipients",
            "recipient source claim race",
        ),
    );
    let submit_recipient_source = storage.submit_draft_message(
        fixture.account_id,
        recipient_race_draft.message_id,
        fixture.account_id,
        "jmap",
        audit(
            "alice@example.test",
            "jmap-submit-draft",
            "recipient source claim race",
        ),
    );
    let (recipient_result, recipient_submission) =
        tokio::join!(replace_recipients, submit_recipient_source);
    if let Err(error) = recipient_result.as_ref() {
        anyhow::ensure!(
            error.to_string().contains("message not found"),
            "recipient mutation race returned an unexpected error: {error:#}"
        );
    }
    let recipient_submission = recipient_submission
        .context("submit the source participating in the recipient mutation race")?;
    let source_to = sqlx::query_scalar::<_, Vec<String>>(
        r#"
        SELECT COALESCE(array_agg(address ORDER BY ordinal, id), ARRAY[]::TEXT[])
        FROM message_recipients
        WHERE tenant_id = $1 AND message_id = $2 AND role = 'to'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(recipient_race_draft.message_id)
    .fetch_one(pool)
    .await
    .context("load source recipients after recipient mutation race")?;
    let queued_to = sqlx::query_scalar::<_, Vec<String>>(
        r#"
        SELECT COALESCE(array_agg(address ORDER BY ordinal, id), ARRAY[]::TEXT[])
        FROM submission_recipients
        WHERE tenant_id = $1 AND submission_queue_id = $2 AND role = 'to'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(recipient_submission.outbound_queue_id)
    .fetch_one(pool)
    .await
    .context("load queued recipients after recipient mutation race")?;
    anyhow::ensure!(
        source_to == queued_to
            && matches!(
                source_to.as_slice(),
                [address]
                    if address == "old-race-recipient@example.test"
                        || address == "new-race-recipient@example.test"
            ),
        "a concurrent recipient mutation must either precede the claim in both source and queue state, or fail without rewriting the expunged source"
    );

    let race_draft = storage
        .save_draft_message(
            SubmitMessageInput {
                draft_message_id: None,
                account_id: fixture.account_id,
                submitted_by_account_id: fixture.account_id,
                source: "jmap".to_string(),
                from_display: Some("Alice Race".to_string()),
                from_address: fixture.account_email.clone(),
                sender_display: None,
                sender_address: None,
                to: vec![SubmittedRecipientInput {
                    address: "race-recipient@example.test".to_string(),
                    display_name: None,
                }],
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Single source claim race".to_string(),
                body_text: "Only one concurrent submit may commit".to_string(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 64,
                unread: Some(false),
                flagged: Some(false),
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit(
                "alice@example.test",
                "jmap-save-draft",
                "single source claim race",
            ),
        )
        .await
        .context("save source for concurrent submission claim")?;
    let first = storage.submit_draft_message(
        fixture.account_id,
        race_draft.message_id,
        fixture.account_id,
        "jmap",
        audit(
            "alice@example.test",
            "jmap-submit-draft",
            "single source claim race first",
        ),
    );
    let second = storage.submit_draft_message(
        fixture.account_id,
        race_draft.message_id,
        fixture.account_id,
        "jmap",
        audit(
            "alice@example.test",
            "jmap-submit-draft",
            "single source claim race second",
        ),
    );
    let (first, second) = tokio::join!(first, second);
    let outcomes = [first, second];
    anyhow::ensure!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count() == 1
            && outcomes.iter().filter(|outcome| outcome.is_err()).count() == 1,
        "two concurrent persisted-source submits must produce exactly one committed submission"
    );
    let membership_state = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE visibility = 'visible') AS visible_count,
            COUNT(*) FILTER (WHERE visibility = 'expunged') AS expunged_count
        FROM mailbox_messages
        WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(race_draft.message_id)
    .fetch_one(pool)
    .await
    .context("load source membership state after concurrent submit")?;
    anyhow::ensure!(
        membership_state.try_get::<i64, _>("visible_count")? == 0
            && membership_state.try_get::<i64, _>("expunged_count")? == 1,
        "concurrent submit must expunge exactly the one claimed source membership"
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

    let custom_search = storage
        .upsert_search_folder(UpsertSearchFolderInput {
            id: None,
            account_id: fixture.account_id,
            display_name: "Runtime unread from Alice".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({
                "kind": "mapi_bounded",
                "all": [
                    {"field": "sender", "contains": "alice"},
                    {"field": "hasAttachment", "equals": true}
                ]
            }),
            excluded_folder_roles: vec!["trash".to_string()],
        })
        .await
        .context("create user-saved search folder")?;
    anyhow::ensure!(
        !custom_search.is_builtin && custom_search.definition_kind == "user_saved",
        "created search folder must be user-saved canonical state"
    );
    let duplicate_name_update = storage
        .upsert_search_folder(UpsertSearchFolderInput {
            id: None,
            account_id: fixture.account_id,
            display_name: " Runtime unread from Alice ".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "inbox"}),
            restriction_json: serde_json::json!({
                "kind": "mapi_bounded",
                "all": [
                    {"field": "sender", "contains": "alice duplicate"}
                ]
            }),
            excluded_folder_roles: vec!["junk".to_string()],
        })
        .await
        .context("upsert duplicate user-saved search folder name")?;
    anyhow::ensure!(
        duplicate_name_update.id == custom_search.id
            && duplicate_name_update.display_name == "Runtime unread from Alice",
        "duplicate user-saved search folder name must update the existing row"
    );
    let duplicate_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM search_folders
        WHERE tenant_id = $1
          AND account_id = $2
          AND NOT is_builtin
          AND definition_kind = 'user_saved'
          AND lower(btrim(display_name)) = lower(btrim($3))
          AND result_object_kind = 'message'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind("Runtime unread from Alice")
    .fetch_one(pool)
    .await
    .context("count duplicate user-saved search folder names")?;
    anyhow::ensure!(
        duplicate_count == 1,
        "duplicate user-saved search folder names must be prevented"
    );

    let fetched_custom = storage
        .fetch_search_folders_by_ids(fixture.account_id, &[custom_search.id])
        .await
        .context("fetch user-saved search folder by id")?;
    anyhow::ensure!(
        fetched_custom
            .iter()
            .any(|folder| folder.display_name == "Runtime unread from Alice"
                && folder
                    .restriction_json
                    .get("all")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|clauses| clauses.iter().any(|clause| clause
                        == &serde_json::json!({
                            "field": "sender",
                            "contains": "alice duplicate"
                        })))),
        "created search folder must be readable by id"
    );

    let updated_custom = storage
        .upsert_search_folder(UpsertSearchFolderInput {
            id: Some(custom_search.id),
            account_id: fixture.account_id,
            display_name: "Runtime unread from Alice updated".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({
                "kind": "mapi_bounded",
                "all": [
                    {"field": "sender", "contains": "alice updated"},
                    {"field": "hasAttachment", "equals": false}
                ]
            }),
            excluded_folder_roles: vec!["trash".to_string(), "junk".to_string()],
        })
        .await
        .context("update user-saved search folder")?;
    anyhow::ensure!(
        updated_custom.display_name == "Runtime unread from Alice updated"
            && updated_custom.excluded_folder_roles
                == vec!["trash".to_string(), "junk".to_string()],
        "updated search folder must return canonical updated values"
    );

    storage
        .delete_search_folder(fixture.account_id, custom_search.id)
        .await
        .context("delete user-saved search folder")?;
    let deleted_custom = storage
        .fetch_search_folders_by_ids(fixture.account_id, &[custom_search.id])
        .await
        .context("fetch deleted user-saved search folder by id")?;
    anyhow::ensure!(
        deleted_custom.is_empty(),
        "deleted search folder must no longer be readable"
    );

    let search_folder_change_counts = sqlx::query(
        r#"
        SELECT change_kind, COUNT(*) AS change_count
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'search_folder_definition'
          AND object_id = $3
        GROUP BY change_kind
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(custom_search.id)
    .fetch_all(pool)
    .await
    .context("count user-saved search folder change rows")?;
    for (expected_kind, expected_count) in [("created", 1), ("updated", 2), ("destroyed", 1)] {
        let mut count = 0;
        for row in &search_folder_change_counts {
            if row.try_get::<String, _>("change_kind")? == expected_kind {
                count = row.try_get::<i64, _>("change_count")?;
            }
        }
        anyhow::ensure!(
            count == expected_count,
            "search folder {expected_kind} must write {expected_count} canonical change row(s)"
        );
    }

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

    let mailbox_rules = storage
        .list_mailbox_rules(fixture.account_id)
        .await
        .context("list canonical mailbox rule projection")?;
    let mailbox_rule = mailbox_rules
        .iter()
        .find(|rule| rule.id == script_id)
        .context("created Sieve script is projected as a mailbox rule")?;
    anyhow::ensure!(
        mailbox_rule.name == script_name,
        "mailbox rule keeps script name"
    );
    anyhow::ensure!(
        mailbox_rule.source_kind == "sieve_script",
        "mailbox rule projection must stay backed by Sieve state"
    );
    anyhow::ensure!(
        mailbox_rule
            .condition_summary
            .contains("header Subject contains runtime-rule"),
        "mailbox rule condition summary should describe the Sieve header test"
    );
    anyhow::ensure!(
        mailbox_rule.action_summary == "keep",
        "mailbox rule action summary should describe the Sieve action"
    );

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

async fn exercise_public_folder_replica_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let root = storage
        .create_public_folder_tree(
            CreatePublicFolderTreeInput {
                account_id: fixture.account_id,
                display_name: format!("Runtime PF {}", Uuid::new_v4().simple()),
            },
            audit(
                &fixture.account_email,
                "public-folder-tree.create",
                "runtime public folder tree",
            ),
        )
        .await
        .context("create public folder tree for replica runtime path")?;

    let initial = storage
        .fetch_public_folder_replicas(fixture.account_id, root.id)
        .await
        .context("fetch empty public folder replica set")?;
    anyhow::ensure!(
        initial.is_empty(),
        "new public folder tree must not have implicit replica rows"
    );

    let mbx02 = storage
        .upsert_public_folder_replica(
            PublicFolderReplicaInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                server_name: "LPE-MBX-02".to_string(),
                sort_order: Some(20),
            },
            audit(
                &fixture.account_email,
                "public-folder-replica.upsert",
                "runtime public folder replica",
            ),
        )
        .await
        .context("create second public folder replica")?;
    storage
        .upsert_public_folder_replica(
            PublicFolderReplicaInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                server_name: "LPE-MBX-01".to_string(),
                sort_order: Some(10),
            },
            audit(
                &fixture.account_email,
                "public-folder-replica.upsert",
                "runtime public folder replica",
            ),
        )
        .await
        .context("create first public folder replica")?;

    let ordered = storage
        .fetch_public_folder_replicas(fixture.account_id, root.id)
        .await
        .context("fetch ordered public folder replica set")?;
    let ordered_names = ordered
        .iter()
        .map(|replica| replica.server_name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        ordered_names == ["LPE-MBX-01", "LPE-MBX-02"],
        "public folder replicas must be ordered by sort order then server name"
    );

    let reordered = storage
        .upsert_public_folder_replica(
            PublicFolderReplicaInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                server_name: "LPE-MBX-02".to_string(),
                sort_order: Some(5),
            },
            audit(
                &fixture.account_email,
                "public-folder-replica.upsert",
                "runtime public folder replica reorder",
            ),
        )
        .await
        .context("update public folder replica sort order")?;
    anyhow::ensure!(
        reordered.id == mbx02.id,
        "upserting an existing replica server must update the canonical row"
    );

    let reordered_set = storage
        .fetch_public_folder_replicas(fixture.account_id, root.id)
        .await
        .context("fetch reordered public folder replica set")?;
    let reordered_names = reordered_set
        .iter()
        .map(|replica| replica.server_name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        reordered_names == ["LPE-MBX-02", "LPE-MBX-01"],
        "updated public folder replica sort order must affect canonical reads"
    );

    let blank_server = storage
        .upsert_public_folder_replica(
            PublicFolderReplicaInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                server_name: "  ".to_string(),
                sort_order: Some(0),
            },
            audit(
                &fixture.account_email,
                "public-folder-replica.upsert",
                "runtime blank public folder replica",
            ),
        )
        .await;
    anyhow::ensure!(
        blank_server.is_err(),
        "blank public folder replica server name must be rejected"
    );

    storage
        .delete_public_folder_replica(
            fixture.account_id,
            root.id,
            mbx02.id,
            audit(
                &fixture.account_email,
                "public-folder-replica.delete",
                "runtime public folder replica",
            ),
        )
        .await
        .context("delete public folder replica")?;

    let after_delete = storage
        .fetch_public_folder_replicas(fixture.account_id, root.id)
        .await
        .context("fetch public folder replicas after delete")?;
    anyhow::ensure!(
        after_delete.len() == 1 && after_delete[0].server_name == "LPE-MBX-01",
        "deleted public folder replica must be hidden from active replica reads"
    );

    let deleted_state = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lifecycle_state
        FROM public_folder_replicas
        WHERE tenant_id = $1 AND public_folder_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(root.id)
    .bind(mbx02.id)
    .fetch_one(pool)
    .await
    .context("load deleted public folder replica row state")?;
    anyhow::ensure!(
        deleted_state == "deleted",
        "deleted public folder replica must remain as a lifecycle tombstone row"
    );

    let replica_change_counts = sqlx::query(
        r#"
        SELECT change_kind, COUNT(*) AS change_count
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'public_folder_replica'
          AND summary_json ->> 'folderId' = $3
        GROUP BY change_kind
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id.to_string())
    .fetch_all(pool)
    .await
    .context("count public folder replica change rows")?;
    for (expected_kind, expected_count) in [("created", 2), ("updated", 1), ("destroyed", 1)] {
        let mut count = 0;
        for row in &replica_change_counts {
            if row.try_get::<String, _>("change_kind")? == expected_kind {
                count = row.try_get::<i64, _>("change_count")?;
            }
        }
        anyhow::ensure!(
            count == expected_count,
            "public folder replica {expected_kind} replay count must be {expected_count}"
        );
    }

    let replica_tombstone_count = sqlx::query_scalar::<_, i64>(
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
          AND tombstone.collection_id = $3
          AND tombstone.object_kind = 'public_folder_replica'
          AND tombstone.object_id = $4
          AND log.change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id)
    .bind(mbx02.id)
    .fetch_one(pool)
    .await
    .context("count public folder replica tombstones")?;
    anyhow::ensure!(
        replica_tombstone_count == 1,
        "public folder replica deletion must write a canonical tombstone"
    );

    Ok(())
}

async fn exercise_public_folder_permission_replay_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let root = storage
        .create_public_folder_tree(
            CreatePublicFolderTreeInput {
                account_id: fixture.account_id,
                display_name: format!("Runtime ACL PF {}", Uuid::new_v4().simple()),
            },
            audit(
                &fixture.account_email,
                "public-folder-tree.create",
                "runtime public folder permission tree",
            ),
        )
        .await
        .context("create public folder tree for permission replay path")?;
    let grantee_account_id = Uuid::new_v4();
    let grantee_email = format!("bob-acl-{}@example.test", Uuid::new_v4().simple());
    let domain_id =
        sqlx::query_scalar::<_, Uuid>("SELECT primary_domain_id FROM accounts WHERE id = $1")
            .bind(fixture.account_id)
            .fetch_one(pool)
            .await
            .context("load runtime fixture account domain for public folder ACL grantee")?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Bob ACL')
        "#,
    )
    .bind(grantee_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(&grantee_email)
    .execute(pool)
    .await
    .context("seed public folder ACL grantee account")?;

    storage
        .upsert_public_folder_permission(
            PublicFolderPermissionInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                principal_account_id: fixture.account_id,
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "public-folder-permission.upsert",
                "runtime public folder permission",
            ),
        )
        .await
        .context("create public folder permission")?;
    storage
        .upsert_public_folder_permission(
            PublicFolderPermissionInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                principal_account_id: fixture.account_id,
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "public-folder-permission.upsert",
                "runtime public folder permission update",
            ),
        )
        .await
        .context("update public folder permission")?;
    storage
        .upsert_public_folder_permission(
            PublicFolderPermissionInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                principal_account_id: grantee_account_id,
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "public-folder-permission.upsert",
                "runtime public folder grantee permission",
            ),
        )
        .await
        .context("create public folder grantee permission")?;
    storage
        .upsert_public_folder_permission(
            PublicFolderPermissionInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                principal_account_id: grantee_account_id,
                may_read: false,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "public-folder-permission.upsert",
                "runtime public folder grantee permission revoke update",
            ),
        )
        .await
        .context("update public folder grantee permission to no rights")?;
    let revoked_principal_in_update_change = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT affected_principal_ids @> ARRAY[$4]::uuid[]
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'public_folder_permission'
          AND change_kind = 'updated'
          AND summary_json ->> 'folderId' = $3
          AND summary_json ->> 'principalAccountId' = $4::text
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id.to_string())
    .bind(grantee_account_id)
    .fetch_one(pool)
    .await
    .context("load public folder permission no-rights update affected principals")?;
    anyhow::ensure!(
        revoked_principal_in_update_change,
        "public folder permission no-rights update replay must include the affected principal"
    );
    let before_revocation_canonical_sequence = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(sequence) FROM canonical_change_journal WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id)
    .fetch_one(pool)
    .await
    .context("load public folder ACL revocation starting canonical sequence")?
    .unwrap_or(0);
    storage
        .delete_public_folder_permission(
            fixture.account_id,
            root.id,
            grantee_account_id,
            audit(
                &fixture.account_email,
                "public-folder-permission.delete",
                "runtime public folder grantee permission",
            ),
        )
        .await
        .context("delete public folder permission")?;

    let permission_change_counts = sqlx::query(
        r#"
        SELECT change_kind, COUNT(*) AS change_count
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'public_folder_permission'
          AND summary_json ->> 'folderId' = $3
        GROUP BY change_kind
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id.to_string())
    .fetch_all(pool)
    .await
    .context("count public folder permission change rows")?;
    for (expected_kind, expected_count) in [("created", 2), ("updated", 2), ("destroyed", 1)] {
        let mut count = 0;
        for row in &permission_change_counts {
            if row.try_get::<String, _>("change_kind")? == expected_kind {
                count = row.try_get::<i64, _>("change_count")?;
            }
        }
        anyhow::ensure!(
            count == expected_count,
            "public folder permission {expected_kind} replay count must be {expected_count}"
        );
    }

    let permission_tombstone_count = sqlx::query_scalar::<_, i64>(
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
          AND tombstone.collection_id = $3
          AND tombstone.object_kind = 'public_folder_permission'
          AND log.change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id)
    .fetch_one(pool)
    .await
    .context("count public folder permission tombstones")?;
    anyhow::ensure!(
        permission_tombstone_count == 1,
        "public folder permission deletion must write a canonical tombstone"
    );
    let revoked_principal_in_destroyed_change = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT affected_principal_ids @> ARRAY[$4]::uuid[]
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'public_folder_permission'
          AND change_kind = 'destroyed'
          AND summary_json ->> 'folderId' = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id.to_string())
    .bind(grantee_account_id)
    .fetch_one(pool)
    .await
    .context("load public folder permission revocation affected principals")?;
    anyhow::ensure!(
        revoked_principal_in_destroyed_change,
        "public folder permission revocation replay must include the revoked principal"
    );
    let revoked_principal_in_canonical_scope = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT principal_account_ids @> ARRAY[$2]::uuid[]
           AND account_ids @> ARRAY[$2]::uuid[]
        FROM canonical_change_journal
        WHERE tenant_id = $1
          AND category = 'public_folders'
          AND principal_account_ids @> ARRAY[$2]::uuid[]
          AND account_ids @> ARRAY[$2]::uuid[]
          AND sequence > $3
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(grantee_account_id)
    .bind(before_revocation_canonical_sequence)
    .fetch_optional(pool)
    .await
    .context("load public folder permission revocation canonical scope")?
    .unwrap_or(false);
    anyhow::ensure!(
        revoked_principal_in_canonical_scope,
        "public folder permission revocation push scope must include the revoked principal"
    );

    Ok(())
}

async fn exercise_public_folder_per_user_replay_path(
    storage: &Storage,
    pool: &PgPool,
    fixture: &RuntimeFixture,
) -> Result<()> {
    let root = storage
        .create_public_folder_tree(
            CreatePublicFolderTreeInput {
                account_id: fixture.account_id,
                display_name: format!("Runtime PerUser PF {}", Uuid::new_v4().simple()),
            },
            audit(
                &fixture.account_email,
                "public-folder-tree.create",
                "runtime public folder per-user tree",
            ),
        )
        .await
        .context("create public folder tree for per-user replay path")?;
    let item = storage
        .upsert_public_folder_item(
            UpsertPublicFolderItemInput {
                id: None,
                account_id: fixture.account_id,
                public_folder_id: root.id,
                item_kind: "post".to_string(),
                message_class: "IPM.Post".to_string(),
                subject: "Runtime read-state post".to_string(),
                body_text: "Runtime read-state body".to_string(),
                body_html_sanitized: None,
                source_payload_json: "{}".to_string(),
            },
            audit(
                &fixture.account_email,
                "public-folder-item.create",
                "runtime public folder per-user item",
            ),
        )
        .await
        .context("create public folder item for per-user replay path")?;
    let reader_account_id = Uuid::new_v4();
    let reader_email = format!("reader-pu-{}@example.test", Uuid::new_v4().simple());
    let domain_id =
        sqlx::query_scalar::<_, Uuid>("SELECT primary_domain_id FROM accounts WHERE id = $1")
            .bind(fixture.account_id)
            .fetch_one(pool)
            .await
            .context("load runtime fixture account domain for public folder reader")?;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, $4, 'Public Folder Reader')
        "#,
    )
    .bind(reader_account_id)
    .bind(fixture.tenant_id)
    .bind(domain_id)
    .bind(&reader_email)
    .execute(pool)
    .await
    .context("seed public folder per-user reader account")?;
    storage
        .upsert_public_folder_permission(
            PublicFolderPermissionInput {
                account_id: fixture.account_id,
                public_folder_id: root.id,
                principal_account_id: reader_account_id,
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            audit(
                &fixture.account_email,
                "public-folder-permission.upsert",
                "runtime public folder per-user reader permission",
            ),
        )
        .await
        .context("grant public folder reader access before private state patches")?;
    let before_private_state_sequence = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(sequence) FROM canonical_change_journal WHERE tenant_id = $1",
    )
    .bind(fixture.tenant_id)
    .fetch_one(pool)
    .await
    .context("load public folder per-user starting canonical sequence")?
    .unwrap_or(0);

    storage
        .patch_public_folder_per_user_state(
            fixture.account_id,
            root.id,
            &[PublicFolderPerUserStatePatch {
                item_id: item.id,
                is_read: true,
                last_seen_change: Some(item.change_counter),
                private_json: Some(r#"{"source":"runtime"}"#.to_string()),
            }],
        )
        .await
        .context("create public folder per-user read state")?;
    storage
        .patch_public_folder_per_user_state(
            fixture.account_id,
            root.id,
            &[PublicFolderPerUserStatePatch {
                item_id: item.id,
                is_read: false,
                last_seen_change: Some(item.change_counter),
                private_json: Some(r#"{"source":"runtime","read":false}"#.to_string()),
            }],
        )
        .await
        .context("update public folder per-user read state")?;
    let leaked_private_state_change = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM mail_change_log
            WHERE tenant_id = $1
              AND object_kind = 'public_folder_per_user_state'
              AND summary_json ->> 'folderId' = $2
              AND summary_json ->> 'itemId' = $3
              AND affected_principal_ids @> ARRAY[$4]::uuid[]
        )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(root.id.to_string())
    .bind(item.id.to_string())
    .bind(reader_account_id)
    .fetch_one(pool)
    .await
    .context("check public folder private state replay audience")?;
    anyhow::ensure!(
        !leaked_private_state_change,
        "public folder per-user state replay must not notify other readers"
    );
    let leaked_private_state_push = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM canonical_change_journal
            WHERE tenant_id = $1
              AND category = 'public_folders'
              AND sequence > $2
              AND (
                  principal_account_ids @> ARRAY[$3]::uuid[]
                  OR account_ids @> ARRAY[$3]::uuid[]
              )
        )
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(before_private_state_sequence)
    .bind(reader_account_id)
    .fetch_one(pool)
    .await
    .context("check public folder private state push audience")?;
    anyhow::ensure!(
        !leaked_private_state_push,
        "public folder per-user state push scope must stay private to the changed account"
    );

    let states = storage
        .fetch_public_folder_per_user_state(fixture.account_id, root.id)
        .await
        .context("fetch public folder per-user state after patches")?;
    let state = states
        .iter()
        .find(|state| state.item_id == item.id)
        .context("patched public folder per-user state is readable")?;
    anyhow::ensure!(
        !state.is_read && state.private_json.contains(r#""read": false"#),
        "updated public folder per-user state must expose the latest private facts"
    );

    let state_change_counts = sqlx::query(
        r#"
        SELECT change_kind, COUNT(*) AS change_count
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'public_folder_per_user_state'
          AND summary_json ->> 'folderId' = $3
          AND summary_json ->> 'itemId' = $4
        GROUP BY change_kind
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id.to_string())
    .bind(item.id.to_string())
    .fetch_all(pool)
    .await
    .context("count public folder per-user state change rows")?;
    for (expected_kind, expected_count) in [("created", 1), ("updated", 1)] {
        let mut count = 0;
        for row in &state_change_counts {
            if row.try_get::<String, _>("change_kind")? == expected_kind {
                count = row.try_get::<i64, _>("change_count")?;
            }
        }
        anyhow::ensure!(
            count == expected_count,
            "public folder per-user state {expected_kind} replay count must be {expected_count}"
        );
    }

    storage
        .delete_public_folder_item(
            fixture.account_id,
            root.id,
            item.id,
            audit(
                &fixture.account_email,
                "public-folder-item.delete",
                "runtime public folder item tombstone",
            ),
        )
        .await
        .context("delete public folder item for tombstone modseq check")?;
    let item_tombstone = sqlx::query(
        r#"
        SELECT tombstone.deleted_modseq, log.change_kind
        FROM tombstones tombstone
        JOIN mail_change_log log
          ON log.tenant_id = tombstone.tenant_id
         AND log.cursor = tombstone.change_cursor
         AND log.object_kind = tombstone.object_kind
         AND log.object_id = tombstone.object_id
        WHERE tombstone.tenant_id = $1
          AND tombstone.account_id = $2
          AND tombstone.collection_id = $3
          AND tombstone.object_kind = 'public_folder_item'
          AND tombstone.object_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(root.id)
    .bind(item.id)
    .fetch_one(pool)
    .await
    .context("load public folder item tombstone")?;
    anyhow::ensure!(
        item_tombstone.try_get::<i64, _>("deleted_modseq")? == item.change_counter + 1
            && item_tombstone.try_get::<String, _>("change_kind")? == "destroyed",
        "public folder item tombstone must preserve the post-delete item change counter"
    );
    let visible_states_after_delete = storage
        .fetch_public_folder_per_user_state(fixture.account_id, root.id)
        .await
        .context("fetch public folder per-user state after item delete")?;
    anyhow::ensure!(
        visible_states_after_delete
            .iter()
            .all(|state| state.item_id != item.id),
        "public folder per-user state reads must not project deleted items"
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

    let recoverable = sqlx::query(
        r#"
        SELECT source_imap_uid, recoverable_folder, delete_kind, status, legal_hold, created_by_protocol
        FROM recoverable_items
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND source_mailbox_message_id = $4
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load recoverable item after scoped delete")?;
    anyhow::ensure!(
        recoverable.try_get::<i64, _>("source_imap_uid")? == source_uid
            && recoverable.try_get::<String, _>("recoverable_folder")? == "deletions"
            && recoverable.try_get::<String, _>("delete_kind")? == "hard_delete"
            && recoverable.try_get::<String, _>("status")? == "active"
            && !recoverable.try_get::<bool, _>("legal_hold")?
            && recoverable.try_get::<String, _>("created_by_protocol")? == "mapi",
        "MAPI hard delete must create canonical active recoverable item state"
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

    let recoverable_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM recoverable_items
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND source_mailbox_message_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(submitted.message_id)
    .bind(source_membership_id)
    .fetch_one(pool)
    .await
    .context("load recoverable item id before restore")?;
    let listed_recoverable = storage
        .list_recoverable_items(fixture.account_id, Some("deletions"))
        .await
        .context("list active recoverable items")?;
    anyhow::ensure!(
        listed_recoverable
            .iter()
            .any(|item| item.id == recoverable_id),
        "recoverable item browse API must list active deleted items"
    );
    let restored = storage
        .restore_recoverable_item(
            fixture.account_id,
            recoverable_id,
            Some(submitted.sent_mailbox_id),
            audit(
                "alice@example.test",
                "restore-recoverable-message",
                "runtime drift restore recoverable item",
            ),
        )
        .await
        .context("restore recoverable item")?;
    anyhow::ensure!(
        restored.id == submitted.message_id
            && restored
                .mailbox_states
                .iter()
                .any(|state| state.mailbox_id == submitted.sent_mailbox_id),
        "recoverable restore must recreate normal mailbox visibility in the target mailbox"
    );
    let recoverable_status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status
        FROM recoverable_items
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(recoverable_id)
    .fetch_one(pool)
    .await
    .context("load recoverable item status after restore")?;
    anyhow::ensure!(
        recoverable_status == "restored",
        "recoverable restore must mark the source recoverable item restored"
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
    let inbox_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM mailboxes
        WHERE tenant_id = $1 AND account_id = $2 AND role = 'inbox'
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .fetch_one(pool)
    .await
    .context("load canonical Inbox mailbox for recoverable restore")?;
    let restore_imported = storage
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
                subject: "Runtime MAPI recoverable restore".to_string(),
                body_text: "Recoverable restore body".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!(
                    "<trash-restore-{}@example.test>",
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
                "runtime recoverable restore seed",
            ),
        )
        .await
        .context("seed recoverable restore Trash message")?;
    let restore_source = sqlx::query(
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
    .bind(trash_id)
    .bind(restore_imported.id)
    .fetch_one(pool)
    .await
    .context("load recoverable restore source membership")?;
    let restore_source_membership_id: Uuid = restore_source.try_get("id")?;
    let restore_source_imap_uid: i64 = restore_source.try_get("imap_uid")?;
    storage
        .delete_jmap_email_from_mailbox(
            fixture.account_id,
            trash_id,
            restore_imported.id,
            audit(
                "alice@example.test",
                "mapi-hard-delete-folder-contents",
                "runtime recoverable restore hard delete",
            ),
        )
        .await
        .context("hard-delete restore seed into recoverable items")?;
    let restore_recoverable_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM recoverable_items
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND source_mailbox_message_id = $4
          AND status = 'active'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(restore_imported.id)
    .bind(restore_source_membership_id)
    .fetch_one(pool)
    .await
    .context("load active recoverable item for restore")?;
    sqlx::query(
        r#"
        UPDATE recoverable_items
        SET recoverable_folder = 'versions'
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(restore_recoverable_id)
    .execute(pool)
    .await
    .context("move restore seed to bounded Versions projection")?;
    storage
        .restore_recoverable_item(
            fixture.account_id,
            restore_recoverable_id,
            Some(inbox_id),
            audit(
                "alice@example.test",
                "restore-recoverable-message",
                "runtime recoverable restore",
            ),
        )
        .await
        .context("restore recoverable item through canonical path")?;
    let restored_membership = sqlx::query(
        r#"
        SELECT id, imap_uid, visibility
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
    .bind(inbox_id)
    .bind(restore_imported.id)
    .fetch_one(pool)
    .await
    .context("load restored visible Inbox membership")?;
    let restored_membership_id: Uuid = restored_membership.try_get("id")?;
    let restored_imap_uid: i64 = restored_membership.try_get("imap_uid")?;
    anyhow::ensure!(
        restored_membership_id != restore_source_membership_id
            && restored_imap_uid != restore_source_imap_uid
            && restored_membership.try_get::<String, _>("visibility")? == "visible",
        "recoverable restore must create a fresh visible membership with a new UID"
    );
    let restore_status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status
        FROM recoverable_items
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(restore_recoverable_id)
    .fetch_one(pool)
    .await
    .context("load recoverable status after restore")?;
    anyhow::ensure!(
        restore_status == "restored",
        "recoverable restore must mark the canonical recoverable item restored"
    );
    let restore_replay_rows = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'recoverable_item'
          AND object_id = $3
          AND change_kind = 'moved'
          AND summary_json->>'sourceMailboxMessageId' = $4
          AND summary_json->>'restoredMailboxMessageId' = $5
          AND summary_json->>'sourceImapUid' = $6
          AND summary_json->>'targetMailboxId' = $7
          AND summary_json->>'recoverableFolder' = 'versions'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(restore_recoverable_id)
    .bind(restore_source_membership_id.to_string())
    .bind(restored_membership_id.to_string())
    .bind(restore_source_imap_uid.to_string())
    .bind(inbox_id.to_string())
    .fetch_one(pool)
    .await
    .context("count recoverable restore replay rows")?;
    anyhow::ensure!(
        restore_replay_rows == 1,
        "recoverable restore replay must preserve original source and restored membership ids"
    );

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

    storage
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
        .await
        .context("hard-delete retained Trash membership into recoverable items")?;
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
        visibility == "expunged",
        "retained Trash membership must leave normal folder visibility after hard delete"
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
        tombstone_count == 1,
        "retained Trash hard delete must still write the normal-folder delete tombstone"
    );
    let recoverable = sqlx::query(
        r#"
        SELECT status, recoverable_folder, legal_hold, retained_until::text AS retained_until
        FROM recoverable_items
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND source_mailbox_message_id = $4
        LIMIT 1
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(imported.id)
    .bind(membership_id)
    .fetch_one(pool)
    .await
    .context("load retained recoverable item")?;
    anyhow::ensure!(
        recoverable.try_get::<String, _>("status")? == "active"
            && recoverable.try_get::<String, _>("recoverable_folder")? == "deletions"
            && recoverable.try_get::<bool, _>("legal_hold")?
            && recoverable
                .try_get::<Option<String>, _>("retained_until")?
                .is_some(),
        "retained legal-hold hard delete must preserve active recoverable item state"
    );
    let recoverable_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM recoverable_items
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_id = $3
          AND source_mailbox_message_id = $4
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(imported.id)
    .bind(membership_id)
    .fetch_one(pool)
    .await
    .context("load retained recoverable item id")?;
    let blocked_purge = storage
        .purge_recoverable_item(
            fixture.account_id,
            recoverable_id,
            audit(
                "alice@example.test",
                "purge-recoverable-message",
                "runtime retained recoverable purge",
            ),
        )
        .await;
    anyhow::ensure!(
        blocked_purge.is_err(),
        "recoverable purge must reject active legal hold"
    );
    sqlx::query(
        r#"
        UPDATE recoverable_items
        SET legal_hold = FALSE,
            deleted_at = NOW() - INTERVAL '2 seconds',
            retained_until = NOW() - INTERVAL '1 second'
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(recoverable_id)
    .execute(pool)
    .await
    .context("expire retained recoverable item for purge")?;
    storage
        .purge_recoverable_item(
            fixture.account_id,
            recoverable_id,
            audit(
                "alice@example.test",
                "purge-recoverable-message",
                "runtime expired recoverable purge",
            ),
        )
        .await
        .context("purge expired recoverable item")?;
    let purged_status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT status
        FROM recoverable_items
        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(recoverable_id)
    .fetch_one(pool)
    .await
    .context("load recoverable status after purge")?;
    anyhow::ensure!(
        purged_status == "purged",
        "expired unheld recoverable purge must mark the item purged"
    );
    let recoverable_purge_replay_rows = sqlx::query_scalar::<_, i64>(
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
          AND tombstone.object_kind = 'recoverable_item'
          AND tombstone.object_id = $3
          AND tombstone.message_id = $4
          AND tombstone.reason = 'purge'
          AND log.change_kind = 'destroyed'
        "#,
    )
    .bind(fixture.tenant_id)
    .bind(fixture.account_id)
    .bind(recoverable_id)
    .bind(imported.id)
    .fetch_one(pool)
    .await
    .context("count recoverable purge tombstone replay rows")?;
    anyhow::ensure!(
        recoverable_purge_replay_rows == 1,
        "recoverable purge must write a canonical tombstone and destroyed change-log row"
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
