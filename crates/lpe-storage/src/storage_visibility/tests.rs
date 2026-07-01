use super::*;
use crate::{
    sha256_hex,
    storage_backend::{s3_put_object, select_storage_backend, StorageBackendSelection},
    Storage,
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;

const SCHEMA: &str = include_str!("../../sql/schema.sql");
const PRIMARY_POOL_ID: Uuid = Uuid::from_u128(1);

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

async fn insert_tenant_domain(storage: &Storage, slug: &str) -> (Uuid, Uuid) {
    let tenant_id = Uuid::new_v4();
    let domain_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, slug, display_name) VALUES ($1, $2, $3)")
        .bind(tenant_id)
        .bind(slug)
        .bind(format!("Tenant {slug}"))
        .execute(storage.pool())
        .await
        .expect("insert tenant");
    sqlx::query("INSERT INTO domains (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(domain_id)
        .bind(tenant_id)
        .bind(format!("{slug}.test"))
        .execute(storage.pool())
        .await
        .expect("insert domain");
    (tenant_id, domain_id)
}

async fn insert_blob(storage: &Storage, tenant_id: Uuid, domain_id: Uuid) -> Uuid {
    let blob_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256, media_type,
            size_octets, blob_bytes, magika_status, extraction_status, validated_at
        )
        VALUES (
            $1, $2, $3, 'attachment',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'text/plain', 4, '\x74657374'::bytea, 'valid', 'not_requested', NOW()
        )
        "#,
    )
    .bind(blob_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await
    .expect("insert blob");
    blob_id
}

async fn insert_placement(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_id: Uuid,
    status: &str,
) -> Uuid {
    let placement_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO blob_placements (
            id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
            placement_status, verified_content_sha256, verified_size_octets, verified_at,
            rollback_until, cleanup_error, next_cleanup_attempt_at
        )
        VALUES (
            $1, $2, $3, $4, 'attachment', $5, $6,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            4, NOW(),
            CASE WHEN $6 = 'retiring' THEN NOW() + INTERVAL '1 hour' ELSE NULL END,
            CASE WHEN $6 = 'cleanup_failed' THEN 'cleanup failed in test' ELSE NULL END,
            CASE WHEN $6 = 'cleanup_failed' THEN NOW() - INTERVAL '1 minute' ELSE NULL END
        )
        "#,
    )
    .bind(placement_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(blob_id)
    .bind(PRIMARY_POOL_ID)
    .bind(status)
    .execute(storage.pool())
    .await
    .expect("insert placement");
    placement_id
}

async fn insert_external_blob_with_active_placement(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
) {
    let pool_id = Uuid::new_v4();
    let blob_id = Uuid::new_v4();
    let placement_id = Uuid::new_v4();
    let content_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    sqlx::query(
        r#"
        INSERT INTO storage_pools (id, name, pool_kind, status, config_json)
        VALUES ($1, $2, 's3_compatible', 'active', '{}'::jsonb)
        "#,
    )
    .bind(pool_id)
    .bind(format!("external-vis-{}", pool_id.simple()))
    .execute(storage.pool())
    .await
    .expect("insert external pool");
    sqlx::query(
        r#"
        INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256, media_type,
            size_octets, blob_bytes, magika_status, extraction_status, validated_at
        )
        VALUES ($1, $2, $3, 'attachment', $4, 'text/plain', 5, NULL, 'valid', 'not_requested', NOW())
        "#,
    )
    .bind(blob_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(content_hash)
    .execute(storage.pool())
    .await
    .expect("insert externally placed blob metadata");
    sqlx::query(
        r#"
        INSERT INTO blob_placements (
            id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
            placement_status, verified_content_sha256, verified_size_octets, verified_at
        )
        VALUES ($1, $2, $3, $4, 'attachment', $5, 'active', $6, 5, NOW())
        "#,
    )
    .bind(placement_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(blob_id)
    .bind(pool_id)
    .bind(content_hash)
    .execute(storage.pool())
    .await
    .expect("insert active external placement");
}

async fn insert_failed_migration(
    storage: &Storage,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_id: Uuid,
    source_placement_id: Uuid,
) {
    let target_pool_id = Uuid::new_v4();
    sqlx::query("INSERT INTO storage_pools (id, name, pool_kind) VALUES ($1, $2, 'postgres')")
        .bind(target_pool_id)
        .bind(format!("postgres-{}", tenant_id.simple()))
        .execute(storage.pool())
        .await
        .expect("insert target pool");
    sqlx::query(
        r#"
        INSERT INTO blob_migration_jobs (
            id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
            source_storage_pool_id, target_storage_pool_id, status, attempts, last_error
        )
        VALUES ($1, $2, $3, $4, 'attachment', $5, $6, $7, 'failed', 2, 'checksum mismatch')
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(domain_id)
    .bind(blob_id)
    .bind(source_placement_id)
    .bind(PRIMARY_POOL_ID)
    .bind(target_pool_id)
    .execute(storage.pool())
    .await
    .expect("insert failed migration");
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
        .unwrap_or_else(|_| format!("lpe-storage-health-tests/{}", Uuid::new_v4()));
    Some(json!({
        "endpointUrl": endpoint_url,
        "bucket": bucket,
        "signingRegion": signing_region,
        "addressingStyle": addressing_style,
        "objectPrefix": object_prefix,
        "credentialsRef": "env:LPE_S3_TEST"
    }))
}

#[test]
fn pool_health_marks_failed_placements_degraded() {
    let health = pool_health_summary(
        PoolHealthRow {
            id: PRIMARY_POOL_ID,
            name: "postgres-primary".to_string(),
            pool_kind: "postgres".to_string(),
            status: "active".to_string(),
            config_json: serde_json::json!({}),
            policy_references: 1,
            active_placements: 8,
            retiring_placements: 1,
            failed_placements: 1,
            cleanup_failed_placements: 0,
        },
        PoolBackendHealth {
            state: "ok".to_string(),
            detail: None,
        },
    );

    assert_eq!(health.health, "degraded");
    assert_eq!(health.readiness, "required");
    assert_eq!(health.backend_state, "ok");
    assert_eq!(health.pool.name, "postgres-primary");
}

#[test]
fn s3_backend_health_errors_map_to_provider_neutral_states() {
    let cases = [
        (
            StorageBackendError::NotFound("missing".to_string()),
            "missing_object",
        ),
        (
            StorageBackendError::ChecksumMismatch("bad checksum".to_string()),
            "checksum_mismatch",
        ),
        (
            StorageBackendError::AuthFailed("auth".to_string()),
            "auth_failed",
        ),
        (
            StorageBackendError::Timeout("timeout".to_string()),
            "timeout",
        ),
        (
            StorageBackendError::UnreachableEndpoint("unreachable".to_string()),
            "unreachable_endpoint",
        ),
    ];

    for (error, state) in cases {
        let error: Error = error.into();
        let health = pool_backend_health_from_error(&error);
        assert_eq!(health.state, state);
        assert!(health.detail.is_some());
    }
}

#[test]
fn cleanup_blockers_are_reported_without_internal_ids() {
    let blockers = cleanup_blocker_labels(CleanupBlockerState {
        placement_status: "retiring".to_string(),
        rollback_window_active: true,
        active_replacement_missing: true,
        blob_retention_active: true,
        blob_legal_hold_active: false,
        message_retention_active: false,
        message_legal_hold_active: true,
    });

    assert_eq!(
        blockers,
        vec![
            "rollback_window_active",
            "active_replacement_missing",
            "blob_retention_active",
            "message_legal_hold_active",
        ]
    );
}

#[test]
fn storage_metadata_diagnostics_marks_missing_active_as_critical() {
    let diagnostics = storage_metadata_diagnostics(1, true, 0, 0, 2);

    assert_eq!(diagnostics.status, "degraded");
    assert!(diagnostics.critical);
    assert_eq!(diagnostics.missing_active_placements, 2);
    assert!(diagnostics.detail.contains("missing_active_placements=2"));
}

#[test]
fn storage_metadata_diagnostics_accepts_consistent_metadata() {
    let diagnostics = storage_metadata_diagnostics(1, true, 0, 0, 0);

    assert_eq!(diagnostics.status, "ok");
    assert!(!diagnostics.critical);
    assert_eq!(diagnostics.active_pools, 1);
}

#[tokio::test]
async fn storage_health_reports_degraded_and_tenant_scoped_counts() {
    let Some(storage) = test_storage().await else {
        return;
    };
    let (tenant_a, domain_a) = insert_tenant_domain(&storage, "vis-a").await;
    let (tenant_b, domain_b) = insert_tenant_domain(&storage, "vis-b").await;
    let blob_a = insert_blob(&storage, tenant_a, domain_a).await;
    let placement_a = insert_placement(&storage, tenant_a, domain_a, blob_a, "active").await;
    insert_failed_migration(&storage, tenant_a, domain_a, blob_a, placement_a).await;
    let blob_b = insert_blob(&storage, tenant_b, domain_b).await;
    insert_placement(&storage, tenant_b, domain_b, blob_b, "active").await;

    let platform = storage
        .fetch_platform_storage_health()
        .await
        .expect("platform health");
    assert_eq!(platform.status, "degraded");
    assert_eq!(platform.placements.active, 2);
    assert_eq!(platform.migrations.failed, 1);

    let tenant = storage
        .fetch_tenant_storage_health(tenant_b)
        .await
        .expect("tenant health");
    assert_eq!(tenant.placements.active, 1);
    assert_eq!(tenant.migrations.failed, 0);
}

#[tokio::test]
async fn s3_compatible_pool_health_checks_active_object_placement() {
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

    let pool_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO storage_pools (id, name, pool_kind, status, config_json)
        VALUES ($1, 's3-health', 's3_compatible', 'active', $2)
        "#,
    )
    .bind(pool_id)
    .bind(&config)
    .execute(storage.pool())
    .await
    .expect("insert s3 health pool");
    let (tenant_id, domain_id) = insert_tenant_domain(&storage, "vis-s3-health").await;
    let blob_id = Uuid::new_v4();
    let bytes = b"s3-health-check";
    let content_sha256 = sha256_hex(bytes);
    sqlx::query(
        r#"
        INSERT INTO blobs (
            id, tenant_id, domain_id, blob_kind, content_sha256, media_type,
            size_octets, blob_bytes, magika_status, extraction_status, validated_at
        )
        VALUES ($1, $2, $3, 'attachment', $4, 'text/plain', $5, ''::bytea, 'valid', 'not_requested', NOW())
        "#,
    )
    .bind(blob_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(&content_sha256)
    .bind(bytes.len() as i64)
    .execute(storage.pool())
    .await
    .expect("insert s3 health blob");
    let placement_id = Uuid::new_v4();
    let StorageBackendSelection::S3Compatible(parsed_config) =
        select_storage_backend("s3_compatible", &config).expect("parse s3 config")
    else {
        panic!("expected s3-compatible backend");
    };
    s3_put_object(
        &parsed_config,
        placement_id,
        bytes,
        &content_sha256,
        bytes.len() as i64,
    )
    .await
    .expect("put s3 health object");
    sqlx::query(
        r#"
        INSERT INTO blob_placements (
            id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
            placement_status, verified_content_sha256, verified_size_octets, verified_at
        )
        VALUES ($1, $2, $3, $4, 'attachment', $5, 'active', $6, $7, NOW())
        "#,
    )
    .bind(placement_id)
    .bind(tenant_id)
    .bind(domain_id)
    .bind(blob_id)
    .bind(pool_id)
    .bind(&content_sha256)
    .bind(bytes.len() as i64)
    .execute(storage.pool())
    .await
    .expect("insert s3 health placement");

    let health = storage
        .fetch_platform_storage_health()
        .await
        .expect("fetch storage health");
    let s3_pool = health
        .pools
        .iter()
        .find(|pool| pool.pool.id == pool_id)
        .expect("s3 pool in health");
    assert_eq!(s3_pool.health, "ok");
    assert_eq!(s3_pool.readiness, "required");
    assert_eq!(s3_pool.backend_state, "ok");
    assert!(s3_pool.backend_detail.is_none());
}

#[tokio::test]
async fn cleanup_visibility_reports_blockers_without_blob_or_placement_ids() {
    let Some(storage) = test_storage().await else {
        return;
    };
    let (tenant_id, domain_id) = insert_tenant_domain(&storage, "vis-cleanup").await;
    let blob_id = insert_blob(&storage, tenant_id, domain_id).await;
    insert_placement(&storage, tenant_id, domain_id, blob_id, "retiring").await;

    let cleanup = storage
        .fetch_tenant_storage_cleanup(tenant_id)
        .await
        .expect("cleanup visibility");
    assert_eq!(cleanup.summary.retiring, 1);
    assert_eq!(cleanup.summary.blocked_by_rollback, 1);
    assert_eq!(cleanup.items.len(), 1);
    assert!(cleanup.items[0]
        .blockers
        .iter()
        .any(|blocker| blocker == "rollback_window_active"));
}

#[tokio::test]
async fn storage_metadata_diagnostics_reports_consistent_seed_metadata() {
    let Some(storage) = test_storage().await else {
        return;
    };

    let diagnostics = storage
        .fetch_storage_metadata_diagnostics()
        .await
        .expect("metadata diagnostics");
    assert_eq!(diagnostics.status, "ok");
    assert!(!diagnostics.critical);
    assert_eq!(diagnostics.active_pools, 1);
}

#[tokio::test]
async fn storage_metadata_diagnostics_accepts_external_active_blob_without_db_bytes() {
    let Some(storage) = test_storage().await else {
        return;
    };
    let (tenant_id, domain_id) = insert_tenant_domain(&storage, "vis-external-null").await;
    insert_external_blob_with_active_placement(&storage, tenant_id, domain_id).await;

    let diagnostics = storage
        .fetch_storage_metadata_diagnostics()
        .await
        .expect("metadata diagnostics");

    assert_eq!(diagnostics.status, "ok");
    assert!(!diagnostics.critical);
    assert_eq!(diagnostics.missing_active_placements, 0);
}

#[test]
fn long_errors_are_summarized() {
    let summary = summarize_error(Some("x".repeat(300))).expect("summary");
    assert!(summary.len() < 250);
    assert!(summary.ends_with("..."));
}
