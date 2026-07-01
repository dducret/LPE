use super::*;
use crate::{AttachmentUploadInput, Storage};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;

const SCHEMA: &str = include_str!("../../sql/schema.sql");

async fn test_storage() -> Option<Storage> {
    let database_url = match std::env::var("LPE_STORAGE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => return None,
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("connect to LPE_STORAGE_TEST_DATABASE_URL");
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .execute(&pool)
        .await
        .expect("reset test database schema");
    sqlx::raw_sql(SCHEMA)
        .execute(&pool)
        .await
        .expect("apply schema.sql to test database");
    Some(Storage::new(pool))
}

async fn insert_tenant_domain(storage: &Storage, tenant_id: Uuid, domain_id: Uuid) {
    sqlx::query(
        r#"
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, 'blob-test', 'Blob Test')
        "#,
    )
    .bind(tenant_id)
    .execute(storage.pool())
    .await
    .expect("insert tenant");
    sqlx::query(
        r#"
        INSERT INTO domains (id, tenant_id, name)
        VALUES ($1, $2, 'example.test')
        "#,
    )
    .bind(domain_id)
    .bind(tenant_id)
    .execute(storage.pool())
    .await
    .expect("insert domain");
}

fn s3_test_config() -> Option<Value> {
    let endpoint_url = std::env::var("LPE_S3_TEST_ENDPOINT_URL").ok()?;
    let bucket = std::env::var("LPE_S3_TEST_BUCKET").ok()?;
    let signing_region = std::env::var("LPE_S3_TEST_SIGNING_REGION")
        .or_else(|_| std::env::var("LPE_S3_TEST_REGION"))
        .ok()?;
    if std::env::var("LPE_S3_TEST_ACCESS_KEY_ID").is_err()
        || std::env::var("LPE_S3_TEST_SECRET_ACCESS_KEY").is_err()
    {
        return None;
    }
    let addressing_style =
        std::env::var("LPE_S3_TEST_ADDRESSING_STYLE").unwrap_or_else(|_| "path".to_string());
    let object_prefix = std::env::var("LPE_S3_TEST_OBJECT_PREFIX")
        .map(|prefix| format!("{}/{}", prefix.trim_matches('/'), Uuid::new_v4()))
        .unwrap_or_else(|_| format!("lpe-storage-tests/{}", Uuid::new_v4()));
    Some(json!({
        "endpointUrl": endpoint_url,
        "bucket": bucket,
        "signingRegion": signing_region,
        "addressingStyle": addressing_style,
        "objectPrefix": object_prefix,
        "credentialsRef": "env:LPE_S3_TEST"
    }))
}

fn s3_placeholder_config() -> Value {
    json!({
        "endpointUrl": "http://127.0.0.1:9000",
        "bucket": "lpe-test",
        "signingRegion": "test-region",
        "addressingStyle": "path",
        "credentialsRef": "env:LPE_S3_PLACEHOLDER"
    })
}

async fn insert_s3_storage_pool(storage: &Storage, config: Value) -> Uuid {
    let pool_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO storage_pools (id, name, pool_kind, status, config_json)
        VALUES ($1, $2, 's3_compatible', 'active', $3)
        "#,
    )
    .bind(pool_id)
    .bind(format!("object-{}", pool_id.simple()))
    .bind(config)
    .execute(storage.pool())
    .await
    .expect("insert s3-compatible pool");
    pool_id
}

async fn configure_s3_platform_pool(storage: &Storage, config: Value) -> Uuid {
    let pool_id = insert_s3_storage_pool(storage, config).await;
    sqlx::query(
        r#"
        UPDATE storage_policy_assignments
        SET storage_pool_id = $1, updated_at = NOW()
        WHERE scope_kind = 'platform'
        "#,
    )
    .bind(pool_id)
    .execute(storage.pool())
    .await
    .expect("set platform storage policy");
    pool_id
}

async fn insert_account_mailbox(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    account_id: Uuid,
    mailbox_id: Uuid,
) {
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'quota-user@example.test', 'Quota User')
        "#,
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await
    .expect("insert account");
    sqlx::query(
        r#"
        INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, uid_validity)
        VALUES ($1, $2, $3, 'inbox', 'INBOX', 1)
        "#,
    )
    .bind(mailbox_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(storage.pool())
    .await
    .expect("insert mailbox");
}

async fn insert_logical_message_with_attachment(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    account_id: Uuid,
    mailbox_id: Uuid,
    imap_uid: i64,
    logical_size_octets: i64,
    attachment_bytes: &[u8],
) -> Uuid {
    let tenant = tenant_id;
    let message_id = Uuid::new_v4();
    let mailbox_message_id = Uuid::new_v4();
    let raw_message = format!("Subject: quota-{imap_uid}\r\n\r\nbody").into_bytes();
    let mut tx = storage.pool().begin().await.expect("begin quota tx");
    let raw_blob_id = storage
        .store_message_blob_in_tx(
            &mut tx,
            &tenant,
            domain_id,
            "raw_message",
            "message/rfc822",
            &raw_message,
        )
        .await
        .expect("store raw message blob");
    sqlx::query(
        r#"
        INSERT INTO messages (
            id, tenant_id, domain_id, blob_id, message_hash,
            normalized_subject, received_at, size_octets
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
        "#,
    )
    .bind(message_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(raw_blob_id)
    .bind(sha256_hex(&raw_message))
    .bind(format!("quota-{imap_uid}"))
    .bind(logical_size_octets)
    .execute(&mut *tx)
    .await
    .expect("insert quota message");
    let attachment_ids = storage
        .ingest_message_attachments_in_tx(
            &mut tx,
            &tenant,
            account_id,
            message_id,
            &[AttachmentUploadInput {
                file_name: "shared.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                blob_bytes: attachment_bytes.to_vec(),
            }],
        )
        .await
        .expect("ingest quota attachment");
    sqlx::query(
        r#"
        INSERT INTO mailbox_messages (
            id, tenant_id, account_id, mailbox_id, message_id, imap_uid, received_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        "#,
    )
    .bind(mailbox_message_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(mailbox_id)
    .bind(message_id)
    .bind(imap_uid)
    .execute(&mut *tx)
    .await
    .expect("insert mailbox message");
    storage
        .assign_message_attachments_membership_in_tx(
            &mut tx,
            &tenant,
            account_id,
            message_id,
            mailbox_message_id,
        )
        .await
        .expect("assign attachment membership");
    tx.commit().await.expect("commit quota message");

    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT blob_id
        FROM attachments
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(attachment_ids[0])
    .fetch_one(storage.pool())
    .await
    .expect("load attachment blob id")
}

async fn logical_quota_snapshot(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    account_id: Uuid,
    mailbox_id: Uuid,
) -> (u64, u64, u64) {
    let account_used = storage
        .fetch_jmap_quota(account_id)
        .await
        .expect("fetch account quota")
        .used;
    let mailbox_used = storage
        .fetch_mailbox_logical_quota_used_octets(account_id, mailbox_id)
        .await
        .expect("fetch mailbox quota");
    let domain_used = storage
        .fetch_domain_logical_quota_used_octets(&tenant_id, domain_id)
        .await
        .expect("fetch domain quota");
    (account_used, mailbox_used, domain_used)
}

async fn expire_retiring_placement(storage: &Storage, tenant_id: Uuid, placement_id: Uuid) {
    sqlx::query(
        r#"
        UPDATE blob_placements
        SET rollback_until = NOW() - INTERVAL '1 minute'
        WHERE tenant_id = $1
          AND id = $2
          AND placement_status = 'retiring'
        "#,
    )
    .bind(tenant_id)
    .bind(placement_id)
    .execute(storage.pool())
    .await
    .expect("expire retiring placement rollback window");
}

async fn mark_active_replacement_failed(
    storage: &Storage,
    tenant_id: Uuid,
    blob_id: Uuid,
    source_placement_id: Uuid,
) {
    sqlx::query(
        r#"
        UPDATE blob_placements
        SET placement_status = 'failed', updated_at = NOW()
        WHERE tenant_id = $1
          AND blob_id = $2
          AND id <> $3
          AND placement_status = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .bind(source_placement_id)
    .execute(storage.pool())
    .await
    .expect("mark active replacement failed");
}

async fn cleanup_blockers(storage: &Storage, placement_id: Uuid) -> Vec<String> {
    PostgresBlobStore
        .old_placement_cleanup_eligibility(storage.pool(), placement_id)
        .await
        .expect("load cleanup eligibility")
        .blockers
}

async fn placement_status_by_id(storage: &Storage, placement_id: Uuid) -> String {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT placement_status
        FROM blob_placements
        WHERE id = $1
        "#,
    )
    .bind(placement_id)
    .fetch_one(storage.pool())
    .await
    .expect("load placement status")
}

async fn active_placement_id(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Uuid {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM blob_placements
        WHERE tenant_id = $1
          AND blob_id = $2
          AND placement_status = 'active'
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .fetch_one(storage.pool())
    .await
    .expect("load active placement id")
}

async fn assert_active_blob_read(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_id: Uuid,
    expected_bytes: &[u8],
) {
    let blob = PostgresBlobStore
        .read_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob_id,
        )
        .await
        .expect("read active blob")
        .expect("active blob exists");
    assert_eq!(blob.bytes, expected_bytes);
}

async fn active_placement_count(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM blob_placements
        WHERE tenant_id = $1
          AND blob_id = $2
          AND placement_status = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .fetch_one(storage.pool())
    .await
    .expect("count active placements")
}

async fn placement_count_for_pool(
    storage: &Storage,
    tenant_id: Uuid,
    blob_id: Uuid,
    storage_pool_id: Uuid,
) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM blob_placements
        WHERE tenant_id = $1
          AND blob_id = $2
          AND storage_pool_id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .bind(storage_pool_id)
    .fetch_one(storage.pool())
    .await
    .expect("count pool placements")
}

async fn active_storage_pool_id(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Uuid {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT storage_pool_id
        FROM blob_placements
        WHERE tenant_id = $1
          AND blob_id = $2
          AND placement_status = 'active'
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .fetch_one(storage.pool())
    .await
    .expect("load active storage pool")
}

async fn database_blob_bytes_len(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Option<i64> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT octet_length(blob_bytes)::bigint
        FROM blobs
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .fetch_one(storage.pool())
    .await
    .expect("load database blob byte length")
}

async fn insert_secondary_storage_pool(storage: &Storage) -> Uuid {
    let pool_id = Uuid::from_u128(2);
    sqlx::query(
        r#"
        INSERT INTO storage_pools (id, name, pool_kind)
        VALUES ($1, 'postgres-secondary', 'postgres')
        "#,
    )
    .bind(pool_id)
    .execute(storage.pool())
    .await
    .expect("insert secondary storage pool");
    pool_id
}

async fn put_test_blob(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    kind: DurableBlobKind,
    bytes: &[u8],
) -> StoredBlobRef {
    let blob_store = PostgresBlobStore;
    let tenant = tenant_id;
    let mut tx = storage.pool().begin().await.expect("begin blob tx");
    let blob = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id,
                kind,
                media_type: "application/octet-stream",
                bytes,
                magika_status: "valid",
                extraction_status: "unsupported",
                validated: true,
            },
        )
        .await
        .expect("store test blob");
    tx.commit().await.expect("commit blob tx");
    blob
}

async fn create_verified_migration_job(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_id: Uuid,
    target_pool_id: Uuid,
) -> BlobMigrationJob {
    let blob_store = PostgresBlobStore;
    blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob_id,
            target_pool_id,
        )
        .await
        .expect("create migration job");
    blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("copy and verify migration job")
        .expect("verified job")
}

#[tokio::test]
async fn create_blob_migration_job_accepts_attachment_and_mime_part_blobs() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let attachment = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"attachment-migrate",
    )
    .await;
    let mime_part = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::MimePart,
        b"mime-part-migrate",
    )
    .await;
    let blob_store = PostgresBlobStore;
    let tenant = tenant_id;

    let attachment_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            attachment.id,
            target_pool_id,
        )
        .await
        .expect("create attachment migration job");
    assert_eq!(attachment_job.blob_id, attachment.id);
    assert_eq!(attachment_job.blob_kind, "attachment");
    assert_eq!(attachment_job.target_storage_pool_id, target_pool_id);
    assert_eq!(attachment_job.status, "pending");
    assert_eq!(attachment_job.attempts, 0);
    assert!(attachment_job.target_placement_id.is_none());

    let mime_part_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "mime_part",
            mime_part.id,
            target_pool_id,
        )
        .await
        .expect("create mime-part migration job");
    assert_eq!(mime_part_job.blob_id, mime_part.id);
    assert_eq!(mime_part_job.blob_kind, "mime_part");
    assert_eq!(mime_part_job.status, "pending");
}

#[tokio::test]
async fn create_blob_migration_job_accepts_s3_compatible_target_pool() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_s3_storage_pool(&storage, s3_placeholder_config()).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"create-s3-migration",
    )
    .await;

    let job = PostgresBlobStore
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create s3-compatible migration job");

    assert_eq!(job.source_storage_pool_id, POSTGRES_PRIMARY_STORAGE_POOL_ID);
    assert_eq!(job.target_storage_pool_id, target_pool_id);
    assert_eq!(job.status, "pending");
    assert!(job.target_placement_id.is_none());
}

#[tokio::test]
async fn duplicate_blob_migration_job_create_returns_existing_open_job() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"duplicate-migrate",
    )
    .await;
    let blob_store = PostgresBlobStore;
    let tenant = tenant_id;

    let first = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create first migration job");
    let duplicate = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create duplicate migration job");

    assert_eq!(duplicate.id, first.id);
    assert_eq!(duplicate.source_placement_id, first.source_placement_id);
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM blob_migration_jobs
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(blob.id)
    .fetch_one(storage.pool())
    .await
    .expect("count migration jobs");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn create_blob_migration_job_rejects_raw_message_kind() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let error = PostgresBlobStore
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "raw_message",
            Uuid::new_v4(),
            target_pool_id,
        )
        .await
        .expect_err("raw message migration must fail")
        .to_string();
    assert!(
        error.contains("only support durable attachment and MIME-part blobs"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn create_blob_migration_job_rejects_missing_active_source_placement() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"missing-placement",
    )
    .await;
    sqlx::query(
        r#"
        DELETE FROM blob_placements
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(blob.id)
    .execute(storage.pool())
    .await
    .expect("delete source placement");

    let error = PostgresBlobStore
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect_err("missing source placement must fail")
        .to_string();
    assert!(
        error.contains("no active source storage placement"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn create_blob_migration_job_rejects_same_source_and_target_pool() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"same-pool-migrate",
    )
    .await;

    let error = PostgresBlobStore
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            POSTGRES_PRIMARY_STORAGE_POOL_ID,
        )
        .await
        .expect_err("same-pool migration must fail")
        .to_string();
    assert!(
        error.contains("source and target storage pools must differ"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn pending_blob_migration_jobs_are_loaded_in_deterministic_retry_order() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob_store = PostgresBlobStore;
    let tenant = tenant_id;

    let future = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"future-migrate",
    )
    .await;
    let first_due = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"first-due-migrate",
    )
    .await;
    let second_due = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"second-due-migrate",
    )
    .await;

    let future_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            future.id,
            target_pool_id,
        )
        .await
        .expect("create future job");
    let first_due_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            first_due.id,
            target_pool_id,
        )
        .await
        .expect("create first due job");
    let second_due_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            second_due.id,
            target_pool_id,
        )
        .await
        .expect("create second due job");

    sqlx::query(
        r#"
        UPDATE blob_migration_jobs
        SET next_attempt_at = CASE
                WHEN id = $1 THEN NOW() + INTERVAL '1 hour'
                WHEN id = $2 THEN NOW() - INTERVAL '2 hours'
                ELSE NOW() - INTERVAL '1 hour'
            END,
            updated_at = NOW()
        WHERE tenant_id = $4 AND id IN ($1, $2, $3)
        "#,
    )
    .bind(future_job.id)
    .bind(first_due_job.id)
    .bind(second_due_job.id)
    .bind(tenant_id)
    .execute(storage.pool())
    .await
    .expect("schedule migration jobs");

    let pending = blob_store
        .load_pending_blob_migration_jobs(storage.pool(), 10)
        .await
        .expect("load pending migration jobs");
    let pending_ids = pending.into_iter().map(|job| job.id).collect::<Vec<_>>();
    assert_eq!(pending_ids, vec![first_due_job.id, second_due_job.id]);
}

#[tokio::test]
async fn copy_verify_worker_reuses_target_placement_across_repeated_execution() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"copy-verify-repeat",
    )
    .await;
    let blob_store = PostgresBlobStore;
    let job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create migration job");

    let verified = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("run worker")
        .expect("worker claimed job");
    assert_eq!(verified.id, job.id);
    assert_eq!(verified.status, "verified");
    assert!(verified.target_placement_id.is_some());
    assert_eq!(
        placement_count_for_pool(&storage, tenant_id, blob.id, target_pool_id).await,
        1
    );

    sqlx::query(
        r#"
        UPDATE blob_migration_jobs
        SET status = 'pending',
            target_placement_id = NULL,
            verified_at = NULL,
            next_attempt_at = NOW(),
            updated_at = NOW()
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(job.id)
    .execute(storage.pool())
    .await
    .expect("simulate interrupted job metadata");

    let repeated = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("rerun worker")
        .expect("worker reclaimed job");
    assert_eq!(repeated.status, "verified");
    assert_eq!(repeated.target_placement_id, verified.target_placement_id);
    assert_eq!(
        placement_count_for_pool(&storage, tenant_id, blob.id, target_pool_id).await,
        1
    );
    assert!(blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("rerun verified worker")
        .is_none());
}

#[tokio::test]
async fn copy_verify_worker_leaves_active_source_read_path_unchanged() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"source-read-continues",
    )
    .await;
    let blob_store = PostgresBlobStore;
    blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create migration job");

    blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("run worker")
        .expect("worker claimed job");

    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );
    assert_eq!(
        active_placement_count(&storage, tenant_id, blob.id).await,
        1
    );
    let target_active = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM blob_placements
        WHERE tenant_id = $1
          AND blob_id = $2
          AND storage_pool_id = $3
          AND placement_status = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(blob.id)
    .bind(target_pool_id)
    .fetch_one(storage.pool())
    .await
    .expect("count active target placements");
    assert_eq!(target_active, 0);

    let read = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("read through active source")
        .expect("blob exists");
    assert_eq!(read.bytes, b"source-read-continues");
}

#[tokio::test]
async fn copy_verify_worker_records_retryable_failure_without_switching_source() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"verify-failure",
    )
    .await;
    let blob_store = PostgresBlobStore;
    let job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create migration job");

    sqlx::query(
        r#"
        UPDATE blobs
        SET blob_bytes = $1
        WHERE tenant_id = $2 AND id = $3
        "#,
    )
    .bind(b"tampered".as_slice())
    .bind(tenant_id)
    .bind(blob.id)
    .execute(storage.pool())
    .await
    .expect("tamper blob bytes");

    let failed = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("run worker")
        .expect("worker claimed job");
    assert_eq!(failed.id, job.id);
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.attempts, 1);
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );
    assert_eq!(
        placement_count_for_pool(&storage, tenant_id, blob.id, target_pool_id).await,
        0
    );
    let last_error = sqlx::query_scalar::<_, String>(
        r#"
        SELECT last_error
        FROM blob_migration_jobs
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(job.id)
    .fetch_one(storage.pool())
    .await
    .expect("load migration job error");
    assert!(
        last_error.contains("checksum or size verification failed"),
        "unexpected error: {last_error}"
    );
}

#[tokio::test]
async fn switch_verified_migration_job_leaves_one_active_target_placement() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"switch-active-target",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;

    let switched = PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    assert_eq!(switched.status, "switched");
    assert_eq!(
        active_placement_count(&storage, tenant_id, blob.id).await,
        1
    );
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        target_pool_id
    );

    let source_status = sqlx::query_scalar::<_, String>(
        r#"
        SELECT placement_status
        FROM blob_placements
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(verified.source_placement_id)
    .fetch_one(storage.pool())
    .await
    .expect("load source placement status");
    assert_eq!(source_status, "retiring");
}

#[tokio::test]
async fn repeated_switch_verified_migration_job_is_idempotent() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"switch-idempotent",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;
    let blob_store = PostgresBlobStore;

    let first = blob_store
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("first switch")
        .expect("first switched job");
    let second = blob_store
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("second switch")
        .expect("second switched job");
    assert_eq!(first.id, second.id);
    assert_eq!(second.status, "switched");
    assert_eq!(second.target_placement_id, first.target_placement_id);
    assert_eq!(
        active_placement_count(&storage, tenant_id, blob.id).await,
        1
    );
    assert_eq!(
        placement_count_for_pool(&storage, tenant_id, blob.id, target_pool_id).await,
        1
    );
}

#[tokio::test]
async fn switch_preserves_reads_stats_and_verification_across_phases() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"switch-read-path",
    )
    .await;
    let blob_store = PostgresBlobStore;
    let before = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("read before copy")
        .expect("blob before copy");
    assert_eq!(before.bytes, b"switch-read-path");

    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;
    let during = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("read before switch")
        .expect("blob before switch");
    assert_eq!(during.bytes, b"switch-read-path");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );

    blob_store
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        target_pool_id
    );
    let after = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("read after switch")
        .expect("blob after switch");
    assert_eq!(after.bytes, b"switch-read-path");
    let stat = blob_store
        .stat_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("stat after switch")
        .expect("stat exists");
    assert_eq!(stat.size_octets, b"switch-read-path".len() as i64);
    assert!(blob_store
        .verify_durable_blob(
            storage.pool(),
            &tenant_id,
            domain_id,
            DurableBlobKind::Attachment,
            blob.id,
        )
        .await
        .expect("verify after switch"));
}

#[tokio::test]
async fn switch_writes_rollback_window_to_retiring_source_placement() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"switch-rollback-window",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;

    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");

    let rollback_window_present = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT rollback_until IS NOT NULL AND rollback_until > NOW()
        FROM blob_placements
        WHERE tenant_id = $1
          AND id = $2
          AND placement_status = 'retiring'
        "#,
    )
    .bind(tenant_id)
    .bind(verified.source_placement_id)
    .fetch_one(storage.pool())
    .await
    .expect("load rollback window");
    assert!(rollback_window_present);
}

#[tokio::test]
async fn logical_quota_is_stable_across_deduplicated_blob_migration() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let mailbox_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    insert_account_mailbox(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;

    let first_blob_id = insert_logical_message_with_attachment(
        &storage,
        tenant_id,
        domain_id,
        account_id,
        mailbox_id,
        1,
        1_024,
        b"deduplicated attachment bytes",
    )
    .await;
    let second_blob_id = insert_logical_message_with_attachment(
        &storage,
        tenant_id,
        domain_id,
        account_id,
        mailbox_id,
        2,
        2_048,
        b"deduplicated attachment bytes",
    )
    .await;
    assert_eq!(second_blob_id, first_blob_id);

    let before =
        logical_quota_snapshot(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    assert_eq!(before, (3_072, 3_072, 3_072));

    let verified = create_verified_migration_job(
        &storage,
        tenant_id,
        domain_id,
        first_blob_id,
        target_pool_id,
    )
    .await;
    let after_copy =
        logical_quota_snapshot(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    assert_eq!(after_copy, before);

    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    let during_retiring =
        logical_quota_snapshot(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    assert_eq!(during_retiring, before);
    assert_eq!(
        active_placement_count(&storage, tenant_id, first_blob_id).await,
        1
    );

    sqlx::query(
        r#"
        UPDATE blob_placements
        SET rollback_until = NOW() - INTERVAL '1 minute'
        WHERE tenant_id = $1
          AND id = $2
          AND placement_status = 'retiring'
        "#,
    )
    .bind(tenant_id)
    .bind(verified.source_placement_id)
    .execute(storage.pool())
    .await
    .expect("expire rollback window");
    let cleanup_eligible =
        logical_quota_snapshot(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    assert_eq!(cleanup_eligible, before);
}

#[tokio::test]
async fn retiring_placement_cleanup_is_blocked_by_rollback_window() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"rollback-guard",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;

    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");

    let eligibility = PostgresBlobStore
        .old_placement_cleanup_eligibility(storage.pool(), verified.source_placement_id)
        .await
        .expect("load cleanup eligibility");
    assert!(!eligibility.is_eligible());
    assert_eq!(eligibility.placement_id, verified.source_placement_id);
    assert!(eligibility
        .blockers
        .contains(&"rollback_window_active".to_string()));
}

#[tokio::test]
async fn retiring_placement_cleanup_is_blocked_when_live_references_need_it() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let mailbox_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    insert_account_mailbox(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob_id = insert_logical_message_with_attachment(
        &storage,
        tenant_id,
        domain_id,
        account_id,
        mailbox_id,
        1,
        1_024,
        b"live-reference-guard",
    )
    .await;
    sqlx::query(
        r#"
        INSERT INTO attachment_texts (
            tenant_id, blob_id, extracted_text, content_hash, search_vector
        )
        VALUES ($1, $2, 'indexed text', $3, to_tsvector('simple', 'indexed text'))
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .bind(sha256_hex(b"indexed text"))
    .execute(storage.pool())
    .await
    .expect("insert attachment text");

    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob_id, target_pool_id)
            .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;
    mark_active_replacement_failed(&storage, tenant_id, blob_id, verified.source_placement_id)
        .await;

    let blockers = cleanup_blockers(&storage, verified.source_placement_id).await;
    for expected in [
        "active_replacement_missing",
        "live_attachment_reference",
        "live_attachment_text_reference",
        "live_extraction_job_reference",
        "live_mime_part_reference",
    ] {
        assert!(
            blockers.contains(&expected.to_string()),
            "missing blocker {expected}; blockers: {blockers:?}"
        );
    }
}

#[tokio::test]
async fn retiring_placement_cleanup_is_blocked_by_retention_and_legal_hold() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let mailbox_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    insert_account_mailbox(&storage, tenant_id, domain_id, account_id, mailbox_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob_id = insert_logical_message_with_attachment(
        &storage,
        tenant_id,
        domain_id,
        account_id,
        mailbox_id,
        1,
        1_024,
        b"retention-legal-hold-guard",
    )
    .await;
    let message_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT message_id
        FROM attachments
        WHERE tenant_id = $1 AND blob_id = $2
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .fetch_one(storage.pool())
    .await
    .expect("load retained message id");
    sqlx::query(
        r#"
        UPDATE blobs
        SET retained_until = NOW() + INTERVAL '1 day',
            legal_hold = TRUE
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(blob_id)
    .execute(storage.pool())
    .await
    .expect("protect blob");
    sqlx::query(
        r#"
        UPDATE messages
        SET retained_until = NOW() + INTERVAL '1 day',
            legal_hold = TRUE
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(message_id)
    .execute(storage.pool())
    .await
    .expect("protect message");

    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob_id, target_pool_id)
            .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;

    let blockers = cleanup_blockers(&storage, verified.source_placement_id).await;
    for expected in [
        "blob_legal_hold_active",
        "blob_retention_active",
        "message_legal_hold_active",
        "message_retention_active",
    ] {
        assert!(
            blockers.contains(&expected.to_string()),
            "missing blocker {expected}; blockers: {blockers:?}"
        );
    }
}

#[tokio::test]
async fn cleanup_worker_deletes_old_placement_metadata_and_preserves_active_reads() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"cleanup-preserves-active-read",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;

    let results = PostgresBlobStore
        .cleanup_old_retiring_placements(storage.pool(), 10)
        .await
        .expect("cleanup old placements");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].placement_id, verified.source_placement_id);
    assert!(results[0].cleaned);
    assert_eq!(results[0].status, "deleted");
    assert_eq!(
        placement_status_by_id(&storage, verified.source_placement_id).await,
        "deleted"
    );
    assert_eq!(
        active_placement_count(&storage, tenant_id, blob.id).await,
        1
    );
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        blob.id,
        b"cleanup-preserves-active-read",
    )
    .await;
}

#[tokio::test]
async fn cleanup_worker_refuses_the_only_active_placement() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"only-active-cleanup-refused",
    )
    .await;
    let active_id = active_placement_id(&storage, tenant_id, blob.id).await;

    let result = PostgresBlobStore
        .cleanup_one_old_placement(storage.pool(), active_id)
        .await
        .expect("cleanup active placement");
    assert!(!result.cleaned);
    assert_eq!(result.status, "active");
    assert!(result
        .blockers
        .contains(&"placement_not_retiring".to_string()));
    assert_eq!(placement_status_by_id(&storage, active_id).await, "active");
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        blob.id,
        b"only-active-cleanup-refused",
    )
    .await;
}

#[tokio::test]
async fn cleanup_worker_repeated_execution_is_idempotent() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"cleanup-idempotent",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;

    let first = PostgresBlobStore
        .cleanup_one_old_placement(storage.pool(), verified.source_placement_id)
        .await
        .expect("first cleanup");
    assert!(first.cleaned);
    let second = PostgresBlobStore
        .cleanup_one_old_placement(storage.pool(), verified.source_placement_id)
        .await
        .expect("second cleanup");
    assert!(!second.cleaned);
    assert_eq!(second.status, "deleted");
    assert!(second.blockers.is_empty());
    let worker_results = PostgresBlobStore
        .cleanup_old_retiring_placements(storage.pool(), 10)
        .await
        .expect("repeat worker cleanup");
    assert!(worker_results.is_empty());
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        blob.id,
        b"cleanup-idempotent",
    )
    .await;
}

#[tokio::test]
async fn cleanup_worker_records_retryable_failure_without_breaking_active_reads() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"cleanup-retryable-failure",
    )
    .await;
    let verified =
        create_verified_migration_job(&storage, tenant_id, domain_id, blob.id, target_pool_id)
            .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified migration job")
        .expect("switched job");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;

    let failure = PostgresBlobStore
        .simulate_old_placement_cleanup_failure(
            storage.pool(),
            verified.source_placement_id,
            "simulated metadata cleanup failure",
        )
        .await
        .expect("simulate cleanup failure");
    assert!(!failure.cleaned);
    assert_eq!(failure.status, "cleanup_failed");
    assert_eq!(
        placement_status_by_id(&storage, verified.source_placement_id).await,
        "cleanup_failed"
    );
    let retry_not_due = PostgresBlobStore
        .cleanup_old_retiring_placements(storage.pool(), 10)
        .await
        .expect("cleanup before retry due");
    assert!(retry_not_due.is_empty());
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        blob.id,
        b"cleanup-retryable-failure",
    )
    .await;

    sqlx::query(
        r#"
        UPDATE blob_placements
        SET next_cleanup_attempt_at = NOW() - INTERVAL '1 minute'
        WHERE id = $1
        "#,
    )
    .bind(verified.source_placement_id)
    .execute(storage.pool())
    .await
    .expect("make cleanup retry due");
    let retry = PostgresBlobStore
        .cleanup_old_retiring_placements(storage.pool(), 10)
        .await
        .expect("retry cleanup");
    assert_eq!(retry.len(), 1);
    assert!(retry[0].cleaned);
    assert_eq!(retry[0].status, "deleted");
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        blob.id,
        b"cleanup-retryable-failure",
    )
    .await;
}

#[tokio::test]
async fn cleanup_worker_claims_due_old_placements_deterministically() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let first_blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"cleanup-order-first",
    )
    .await;
    let second_blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"cleanup-order-second",
    )
    .await;
    let first = create_verified_migration_job(
        &storage,
        tenant_id,
        domain_id,
        first_blob.id,
        target_pool_id,
    )
    .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), first.id)
        .await
        .expect("switch first job")
        .expect("first switched");
    let second = create_verified_migration_job(
        &storage,
        tenant_id,
        domain_id,
        second_blob.id,
        target_pool_id,
    )
    .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), second.id)
        .await
        .expect("switch second job")
        .expect("second switched");

    sqlx::query(
        r#"
        UPDATE blob_placements
        SET rollback_until = CASE
                WHEN id = $2 THEN NOW() - INTERVAL '2 hours'
                WHEN id = $3 THEN NOW() - INTERVAL '1 hour'
                ELSE rollback_until
            END
        WHERE tenant_id = $1
          AND id IN ($2, $3)
        "#,
    )
    .bind(tenant_id)
    .bind(first.source_placement_id)
    .bind(second.source_placement_id)
    .execute(storage.pool())
    .await
    .expect("set deterministic rollback order");

    let results = PostgresBlobStore
        .cleanup_old_retiring_placements(storage.pool(), 1)
        .await
        .expect("cleanup one placement");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].placement_id, first.source_placement_id);
    assert_eq!(
        placement_status_by_id(&storage, first.source_placement_id).await,
        "deleted"
    );
    assert_eq!(
        placement_status_by_id(&storage, second.source_placement_id).await,
        "retiring"
    );
}

#[tokio::test]
async fn switch_ignores_unverified_migration_jobs() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"switch-pending",
    )
    .await;
    let pending = PostgresBlobStore
        .create_blob_migration_job(
            storage.pool(),
            &tenant_id,
            domain_id,
            "attachment",
            blob.id,
            target_pool_id,
        )
        .await
        .expect("create pending migration job");

    assert!(PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), pending.id)
        .await
        .expect("switch pending job")
        .is_none());
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );
}

#[tokio::test]
async fn durable_blob_store_writes_reads_stats_and_verifies() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let other_domain_id = Uuid::new_v4();
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    sqlx::query(
        r#"
        INSERT INTO domains (id, tenant_id, name)
        VALUES ($1, $2, 'other.example.test')
        "#,
    )
    .bind(other_domain_id)
    .bind(tenant_id)
    .execute(storage.pool())
    .await
    .expect("insert second domain");

    let blob_store = PostgresBlobStore;
    let bytes = b"attachment-bytes";
    let tenant = tenant_id;
    let mut tx = storage.pool().begin().await.expect("begin transaction");
    let first = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id,
                kind: DurableBlobKind::Attachment,
                media_type: "application/octet-stream",
                bytes,
                magika_status: "valid",
                extraction_status: "unsupported",
                validated: true,
            },
        )
        .await
        .expect("store attachment blob");
    let duplicate = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id,
                kind: DurableBlobKind::Attachment,
                media_type: "application/octet-stream",
                bytes,
                magika_status: "valid",
                extraction_status: "unsupported",
                validated: true,
            },
        )
        .await
        .expect("dedupe attachment blob");
    let mime_part = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id,
                kind: DurableBlobKind::MimePart,
                media_type: "text/plain",
                bytes,
                magika_status: "not_required",
                extraction_status: "not_requested",
                validated: false,
            },
        )
        .await
        .expect("store mime-part blob");
    let other_domain = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id: other_domain_id,
                kind: DurableBlobKind::Attachment,
                media_type: "application/octet-stream",
                bytes,
                magika_status: "valid",
                extraction_status: "unsupported",
                validated: true,
            },
        )
        .await
        .expect("store same bytes in other domain");
    tx.commit().await.expect("commit blob transaction");

    assert!(first.created);
    assert!(!duplicate.created);
    assert_eq!(first.id, duplicate.id);
    assert_ne!(first.id, mime_part.id);
    assert_ne!(first.id, other_domain.id);
    assert_eq!(first.content_sha256, sha256_hex(bytes));
    assert_eq!(first.size_octets, bytes.len() as i64);
    assert_eq!(
        active_placement_count(&storage, tenant_id, first.id).await,
        1
    );
    assert_eq!(
        active_placement_count(&storage, tenant_id, mime_part.id).await,
        1
    );
    assert_eq!(
        active_placement_count(&storage, tenant_id, other_domain.id).await,
        1
    );

    let read = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            first.id,
        )
        .await
        .expect("read attachment blob")
        .expect("attachment blob exists");
    assert_eq!(read.id, first.id);
    assert_eq!(read.media_type, "application/octet-stream");
    assert_eq!(read.bytes, bytes);

    let stat = blob_store
        .stat_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            first.id,
        )
        .await
        .expect("stat attachment blob")
        .expect("attachment blob exists");
    assert_eq!(stat.id, first.id);
    assert_eq!(stat.size_octets, bytes.len() as i64);
    assert_eq!(stat.content_sha256, sha256_hex(bytes));
    assert_eq!(stat.media_type, "application/octet-stream");

    assert!(blob_store
        .verify_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            first.id,
        )
        .await
        .expect("verify attachment blob"));
    sqlx::query(
        r#"
        UPDATE blobs
        SET blob_bytes = $1
        WHERE tenant_id = $2 AND id = $3
        "#,
    )
    .bind(b"tampered".as_slice())
    .bind(tenant_id)
    .bind(first.id)
    .execute(storage.pool())
    .await
    .expect("tamper attachment blob bytes");
    assert!(!blob_store
        .verify_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            first.id,
        )
        .await
        .expect("verify tampered attachment blob"));
    assert!(blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant,
            other_domain_id,
            DurableBlobKind::Attachment,
            first.id,
        )
        .await
        .expect("read wrong-domain blob")
        .is_none());
    sqlx::query(
        r#"
        DELETE FROM blob_placements
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(first.id)
    .execute(storage.pool())
    .await
    .expect("delete active placement");
    assert_eq!(
        active_placement_count(&storage, tenant_id, first.id).await,
        0
    );
    for error in [
        blob_store
            .read_durable_blob(
                storage.pool(),
                &tenant,
                domain_id,
                DurableBlobKind::Attachment,
                first.id,
            )
            .await
            .expect_err("read without active placement must fail")
            .to_string(),
        blob_store
            .stat_durable_blob(
                storage.pool(),
                &tenant,
                domain_id,
                DurableBlobKind::Attachment,
                first.id,
            )
            .await
            .expect_err("stat without active placement must fail")
            .to_string(),
        blob_store
            .verify_durable_blob(
                storage.pool(),
                &tenant,
                domain_id,
                DurableBlobKind::Attachment,
                first.id,
            )
            .await
            .expect_err("verify without active placement must fail")
            .to_string(),
    ] {
        assert!(
            error.contains("active storage placement"),
            "unexpected storage error: {error}"
        );
    }
}

#[tokio::test]
async fn s3_compatible_backend_put_read_stat_and_verify_round_trip() {
    let Some(config) = s3_test_config() else {
        eprintln!(
            "skipping S3-compatible integration test; set LPE_S3_TEST_ENDPOINT_URL, LPE_S3_TEST_BUCKET, LPE_S3_TEST_SIGNING_REGION or LPE_S3_TEST_REGION, LPE_S3_TEST_ACCESS_KEY_ID, and LPE_S3_TEST_SECRET_ACCESS_KEY"
        );
        return;
    };
    let Some(storage) = test_storage().await else {
        eprintln!("skipping S3-compatible integration test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };

    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let tenant = tenant_id;
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let s3_pool_id = configure_s3_platform_pool(&storage, config).await;

    let blob_store = PostgresBlobStore;
    let bytes = b"s3-compatible-put-read-stat-verify";
    let mut tx = storage.pool().begin().await.expect("begin s3 tx");
    let stored = blob_store
        .put_durable_blob_in_tx(
            &mut tx,
            PutBlobRequest {
                tenant_id: &tenant,
                domain_id,
                kind: DurableBlobKind::Attachment,
                media_type: "application/octet-stream",
                bytes,
                magika_status: "valid",
                extraction_status: "unsupported",
                validated: true,
            },
        )
        .await
        .expect("store s3-compatible blob");
    tx.commit().await.expect("commit s3 blob");

    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, stored.id).await,
        s3_pool_id
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, stored.id).await,
        None
    );

    let read = blob_store
        .read_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            stored.id,
        )
        .await
        .expect("read s3-compatible blob")
        .expect("s3-compatible blob exists");
    assert_eq!(read.bytes, bytes);

    let stat = blob_store
        .stat_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            stored.id,
        )
        .await
        .expect("stat s3-compatible blob")
        .expect("s3-compatible blob exists");
    assert_eq!(stat.size_octets, bytes.len() as i64);
    assert_eq!(stat.content_sha256, sha256_hex(bytes));

    assert!(blob_store
        .verify_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            stored.id,
        )
        .await
        .expect("verify s3-compatible blob"));
}

#[tokio::test]
async fn s3_compatible_migration_paths_copy_verify_and_switch() {
    let Some(first_config) = s3_test_config() else {
        eprintln!(
            "skipping S3-compatible integration test; set LPE_S3_TEST_ENDPOINT_URL, LPE_S3_TEST_BUCKET, LPE_S3_TEST_SIGNING_REGION or LPE_S3_TEST_REGION, LPE_S3_TEST_ACCESS_KEY_ID, and LPE_S3_TEST_SECRET_ACCESS_KEY"
        );
        return;
    };
    let Some(second_config) = s3_test_config() else {
        eprintln!(
            "skipping S3-compatible integration test; set LPE_S3_TEST_ENDPOINT_URL, LPE_S3_TEST_BUCKET, LPE_S3_TEST_SIGNING_REGION or LPE_S3_TEST_REGION, LPE_S3_TEST_ACCESS_KEY_ID, and LPE_S3_TEST_SECRET_ACCESS_KEY"
        );
        return;
    };
    let Some(storage) = test_storage().await else {
        eprintln!("skipping S3-compatible integration test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let tenant = tenant_id;
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    let blob_store = PostgresBlobStore;

    let first_s3_pool_id = insert_s3_storage_pool(&storage, first_config).await;
    let db_blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"db-to-s3-migration",
    )
    .await;
    let db_to_s3_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            db_blob.id,
            first_s3_pool_id,
        )
        .await
        .expect("create db-to-s3 migration job");
    let db_to_s3_verified = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("copy db-to-s3 migration")
        .expect("db-to-s3 job claimed");
    assert_eq!(db_to_s3_verified.status, "verified");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, db_blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );

    sqlx::query(
        r#"
        UPDATE blob_migration_jobs
        SET status = 'pending',
            target_placement_id = NULL,
            verified_at = NULL,
            next_attempt_at = NOW(),
            updated_at = NOW()
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(db_to_s3_job.id)
    .execute(storage.pool())
    .await
    .expect("simulate interrupted s3 target metadata");
    let db_to_s3_repeated = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("repeat db-to-s3 migration")
        .expect("db-to-s3 job reclaimed");
    assert_eq!(
        db_to_s3_repeated.target_placement_id,
        db_to_s3_verified.target_placement_id
    );

    let db_to_s3_switched = blob_store
        .switch_verified_blob_migration_job(storage.pool(), db_to_s3_job.id)
        .await
        .expect("switch db-to-s3 migration")
        .expect("db-to-s3 switched");
    assert_eq!(db_to_s3_switched.status, "switched");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, db_blob.id).await,
        first_s3_pool_id
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, db_blob.id).await,
        None
    );
    assert_eq!(
        placement_status_by_id(&storage, db_to_s3_switched.source_placement_id).await,
        "retiring"
    );
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        db_blob.id,
        b"db-to-s3-migration",
    )
    .await;

    sqlx::query(
        r#"
        UPDATE storage_policy_assignments
        SET storage_pool_id = $1, updated_at = NOW()
        WHERE scope_kind = 'platform'
        "#,
    )
    .bind(first_s3_pool_id)
    .execute(storage.pool())
    .await
    .expect("set platform policy to first s3 pool");
    let s3_blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"s3-to-db-migration",
    )
    .await;
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, s3_blob.id).await,
        first_s3_pool_id
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, s3_blob.id).await,
        None
    );
    let s3_to_db_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            s3_blob.id,
            POSTGRES_PRIMARY_STORAGE_POOL_ID,
        )
        .await
        .expect("create s3-to-db migration job");
    let s3_to_db_verified = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("copy s3-to-db migration")
        .expect("s3-to-db job claimed");
    assert_eq!(s3_to_db_verified.status, "verified");
    let s3_to_db_switched = blob_store
        .switch_verified_blob_migration_job(storage.pool(), s3_to_db_job.id)
        .await
        .expect("switch s3-to-db migration")
        .expect("s3-to-db switched");
    assert_eq!(s3_to_db_switched.status, "switched");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, s3_blob.id).await,
        POSTGRES_PRIMARY_STORAGE_POOL_ID
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, s3_blob.id).await,
        Some(b"s3-to-db-migration".len() as i64)
    );
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        s3_blob.id,
        b"s3-to-db-migration",
    )
    .await;

    let second_s3_pool_id = insert_s3_storage_pool(&storage, second_config).await;
    let s3_to_s3_blob = put_test_blob(
        &storage,
        tenant_id,
        domain_id,
        DurableBlobKind::Attachment,
        b"s3-to-s3-migration",
    )
    .await;
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, s3_to_s3_blob.id).await,
        first_s3_pool_id
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, s3_to_s3_blob.id).await,
        None
    );
    let s3_to_s3_job = blob_store
        .create_blob_migration_job(
            storage.pool(),
            &tenant,
            domain_id,
            "attachment",
            s3_to_s3_blob.id,
            second_s3_pool_id,
        )
        .await
        .expect("create s3-to-s3 migration job");
    let s3_to_s3_verified = blob_store
        .copy_and_verify_one_blob_migration_job(storage.pool())
        .await
        .expect("copy s3-to-s3 migration")
        .expect("s3-to-s3 job claimed");
    assert_eq!(s3_to_s3_verified.status, "verified");
    let s3_to_s3_switched = blob_store
        .switch_verified_blob_migration_job(storage.pool(), s3_to_s3_job.id)
        .await
        .expect("switch s3-to-s3 migration")
        .expect("s3-to-s3 switched");
    assert_eq!(s3_to_s3_switched.status, "switched");
    assert_eq!(
        active_storage_pool_id(&storage, tenant_id, s3_to_s3_blob.id).await,
        second_s3_pool_id
    );
    assert_eq!(
        database_blob_bytes_len(&storage, tenant_id, s3_to_s3_blob.id).await,
        None
    );
    assert_active_blob_read(
        &storage,
        tenant_id,
        domain_id,
        s3_to_s3_blob.id,
        b"s3-to-s3-migration",
    )
    .await;
    assert!(blob_store
        .verify_durable_blob(
            storage.pool(),
            &tenant,
            domain_id,
            DurableBlobKind::Attachment,
            s3_to_s3_blob.id,
        )
        .await
        .expect("verify s3-to-s3 active blob"));
}

#[tokio::test]
async fn attachment_content_fetch_reads_through_blob_store_boundary() {
    let Some(storage) = test_storage().await else {
        eprintln!("skipping database test; set LPE_STORAGE_TEST_DATABASE_URL");
        return;
    };
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let mailbox_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let raw_blob_id = Uuid::new_v4();
    let mailbox_message_id = Uuid::new_v4();
    let tenant = tenant_id;
    insert_tenant_domain(&storage, tenant_id, domain_id).await;
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'user@example.test', 'User')
        "#,
    )
    .bind(account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await
    .expect("insert account");
    sqlx::query(
        r#"
        INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, uid_validity)
        VALUES ($1, $2, $3, 'inbox', 'INBOX', 1)
        "#,
    )
    .bind(mailbox_id)
    .bind(tenant_id)
    .bind(account_id)
    .execute(storage.pool())
    .await
    .expect("insert mailbox");
    sqlx::query(
        r#"
        INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256, media_type, size_octets, blob_bytes
        )
        VALUES ($1, $2, $3, 'raw_message', $4, 'message/rfc822', 4, $5)
        "#,
    )
    .bind(raw_blob_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(sha256_hex(b"raw!"))
    .bind(b"raw!".as_slice())
    .execute(storage.pool())
    .await
    .expect("insert raw message blob");
    assert_eq!(
        active_placement_count(&storage, tenant_id, raw_blob_id).await,
        0
    );
    sqlx::query(
        r#"
        INSERT INTO messages (
            id, tenant_id, domain_id, blob_id, message_hash, normalized_subject, received_at, size_octets
        )
        VALUES ($1, $2, $3, $4, $5, 'subject', NOW(), 4)
        "#,
    )
    .bind(message_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(raw_blob_id)
    .bind(sha256_hex(b"raw!"))
    .execute(storage.pool())
    .await
    .expect("insert message");
    sqlx::query(
        r#"
        INSERT INTO mailbox_messages (
            id, tenant_id, account_id, mailbox_id, message_id, imap_uid, received_at
        )
        VALUES ($1, $2, $3, $4, $5, 1, NOW())
        "#,
    )
    .bind(mailbox_message_id)
    .bind(tenant_id)
    .bind(account_id)
    .bind(mailbox_id)
    .bind(message_id)
    .execute(storage.pool())
    .await
    .expect("insert mailbox message");

    let mut tx = storage.pool().begin().await.expect("begin attachment tx");
    let attachment_ids = storage
        .ingest_message_attachments_in_tx(
            &mut tx,
            &tenant,
            account_id,
            message_id,
            &[AttachmentUploadInput {
                file_name: "note.txt".to_string(),
                media_type: "text/plain".to_string(),
                disposition: Some("inline".to_string()),
                content_id: Some("<cid-1>".to_string()),
                blob_bytes: b"attachment body".to_vec(),
            }],
        )
        .await
        .expect("ingest attachment");
    tx.commit().await.expect("commit attachment tx");
    let attachment_id = attachment_ids[0];

    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let by_reference = storage
        .fetch_activesync_attachment_content(account_id, &file_reference)
        .await
        .expect("fetch attachment by reference")
        .expect("attachment content exists");
    assert_eq!(by_reference.file_name, "note.txt");
    assert_eq!(by_reference.media_type, "text/plain");
    assert_eq!(by_reference.blob_bytes, b"attachment body");

    let by_cid = storage
        .fetch_message_attachment_content_by_cid(account_id, message_id, "cid-1")
        .await
        .expect("fetch attachment by cid")
        .expect("cid attachment content exists");
    assert_eq!(by_cid.file_reference, file_reference);
    assert_eq!(by_cid.blob_bytes, b"attachment body");

    let attachment_blob_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT blob_id
        FROM attachments
        WHERE tenant_id = $1 AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(attachment_id)
    .fetch_one(storage.pool())
    .await
    .expect("load attachment blob id");
    let target_pool_id = insert_secondary_storage_pool(&storage).await;
    let verified = create_verified_migration_job(
        &storage,
        tenant_id,
        domain_id,
        attachment_blob_id,
        target_pool_id,
    )
    .await;
    PostgresBlobStore
        .switch_verified_blob_migration_job(storage.pool(), verified.id)
        .await
        .expect("switch verified attachment migration");
    expire_retiring_placement(&storage, tenant_id, verified.source_placement_id).await;
    let cleanup = PostgresBlobStore
        .cleanup_one_old_placement(storage.pool(), verified.source_placement_id)
        .await
        .expect("cleanup old attachment placement");
    assert!(cleanup.cleaned);
    assert_eq!(cleanup.status, "deleted");
    assert_eq!(
        placement_status_by_id(&storage, verified.source_placement_id).await,
        "deleted"
    );
    assert_eq!(
        active_placement_count(&storage, tenant_id, attachment_blob_id).await,
        1
    );

    let attachments = storage
        .fetch_activesync_message_attachments(account_id, message_id)
        .await
        .expect("fetch attachment list after old placement cleanup");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].file_reference, file_reference);

    let after_cleanup_by_reference = storage
        .fetch_activesync_attachment_content(account_id, &file_reference)
        .await
        .expect("fetch attachment by reference after cleanup")
        .expect("attachment content exists after cleanup");
    assert_eq!(after_cleanup_by_reference.blob_bytes, b"attachment body");

    let after_cleanup_by_cid = storage
        .fetch_message_attachment_content_by_cid(account_id, message_id, "cid-1")
        .await
        .expect("fetch attachment by cid after cleanup")
        .expect("cid attachment content exists after cleanup");
    assert_eq!(after_cleanup_by_cid.blob_bytes, b"attachment body");

    let jmap_emails = storage
        .fetch_jmap_emails(account_id, &[message_id])
        .await
        .expect("fetch JMAP email after cleanup");
    assert_eq!(jmap_emails.len(), 1);
    assert_eq!(jmap_emails[0].id, message_id);
    assert!(jmap_emails[0].has_attachments);

    let imap_emails = storage
        .fetch_imap_emails(account_id, mailbox_id)
        .await
        .expect("fetch IMAP email after cleanup");
    assert_eq!(imap_emails.len(), 1);
    assert_eq!(imap_emails[0].id, message_id);
    assert!(imap_emails[0]
        .mime_parts
        .iter()
        .any(|part| part.file_name.as_deref() == Some("note.txt")));
}
