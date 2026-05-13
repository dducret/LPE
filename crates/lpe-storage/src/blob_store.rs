use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::{
    sha256_hex,
    storage_backend::{
        s3_put_object, s3_read_object, s3_stat_object, select_storage_backend,
        StorageBackendSelection,
    },
};

const POSTGRES_PRIMARY_STORAGE_POOL_ID: Uuid = Uuid::from_u128(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DurableBlobKind {
    Attachment,
    MimePart,
}

impl DurableBlobKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::MimePart => "mime_part",
        }
    }
}

#[derive(Debug)]
pub(crate) struct PutBlobRequest<'a> {
    pub(crate) tenant_id: &'a Uuid,
    pub(crate) domain_id: Uuid,
    pub(crate) kind: DurableBlobKind,
    pub(crate) media_type: &'a str,
    pub(crate) bytes: &'a [u8],
    pub(crate) magika_status: &'a str,
    pub(crate) extraction_status: &'a str,
    pub(crate) validated: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct StoredBlobRef {
    pub(crate) id: Uuid,
    pub(crate) domain_id: Uuid,
    pub(crate) content_sha256: String,
    pub(crate) size_octets: i64,
    pub(crate) created: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct StoredBlobBytes {
    pub(crate) id: Uuid,
    pub(crate) media_type: String,
    pub(crate) size_octets: i64,
    pub(crate) content_sha256: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct StoredBlobStat {
    pub(crate) id: Uuid,
    pub(crate) media_type: String,
    pub(crate) size_octets: i64,
    pub(crate) content_sha256: String,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlobMigrationJob {
    pub(crate) id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) domain_id: Uuid,
    pub(crate) blob_id: Uuid,
    pub(crate) blob_kind: String,
    pub(crate) source_placement_id: Uuid,
    pub(crate) source_storage_pool_id: Uuid,
    pub(crate) target_storage_pool_id: Uuid,
    pub(crate) target_placement_id: Option<Uuid>,
    pub(crate) status: String,
    pub(crate) attempts: i32,
}

#[derive(Debug)]
struct WriteStoragePool {
    id: Uuid,
    backend: StorageBackendSelection,
}

#[derive(Debug)]
struct ActiveBlobPlacement {
    placement_id: Uuid,
    backend: StorageBackendSelection,
    id: Uuid,
    media_type: String,
    size_octets: i64,
    content_sha256: String,
    blob_bytes: Option<Vec<u8>>,
}

#[derive(Debug)]
struct MigrationTargetPlacement {
    placement_id: Uuid,
    backend: StorageBackendSelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct PlacementCleanupEligibility {
    pub(crate) placement_id: Uuid,
    pub(crate) blockers: Vec<String>,
}

impl PlacementCleanupEligibility {
    #[allow(dead_code)]
    pub(crate) fn is_eligible(&self) -> bool {
        self.blockers.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct PlacementCleanupResult {
    pub(crate) placement_id: Uuid,
    pub(crate) cleaned: bool,
    pub(crate) status: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct PostgresBlobStore;

impl PostgresBlobStore {
    pub(crate) async fn put_durable_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: PutBlobRequest<'_>,
    ) -> Result<StoredBlobRef> {
        let content_sha256 = sha256_hex(request.bytes);
        let write_pool = self
            .effective_write_storage_pool_in_tx(tx, &request)
            .await?;
        if let Some(row) = sqlx::query(
            r#"
            SELECT id, size_octets
            FROM blobs
            WHERE tenant_id = $1
              AND domain_id = $2
              AND blob_kind = $3
              AND content_sha256 = $4
            LIMIT 1
            "#,
        )
        .bind(request.tenant_id)
        .bind(request.domain_id)
        .bind(request.kind.as_str())
        .bind(&content_sha256)
        .fetch_optional(&mut **tx)
        .await?
        {
            let blob_id = row.try_get("id")?;
            let size_octets = row.try_get("size_octets")?;
            self.ensure_backend_placement_in_tx(
                tx,
                &request,
                blob_id,
                &content_sha256,
                size_octets,
                &write_pool,
            )
            .await?;
            return Ok(StoredBlobRef {
                id: blob_id,
                domain_id: request.domain_id,
                content_sha256,
                size_octets,
                created: false,
            });
        }

        let blob_id = Uuid::new_v4();
        let database_bytes = match &write_pool.backend {
            StorageBackendSelection::Postgres => Some(request.bytes),
            StorageBackendSelection::S3Compatible(_) => None,
        };
        sqlx::query(
            r#"
            INSERT INTO blobs (
                id, tenant_id, domain_id, blob_kind, content_sha256,
                media_type, size_octets, blob_bytes, magika_status,
                extraction_status, validated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CASE WHEN $11 THEN NOW() ELSE NULL END)
            "#,
        )
        .bind(blob_id)
        .bind(request.tenant_id)
        .bind(request.domain_id)
        .bind(request.kind.as_str())
        .bind(&content_sha256)
        .bind(request.media_type)
        .bind(request.bytes.len() as i64)
        .bind(database_bytes)
        .bind(request.magika_status)
        .bind(request.extraction_status)
        .bind(request.validated)
        .execute(&mut **tx)
        .await?;

        self.ensure_backend_placement_in_tx(
            tx,
            &request,
            blob_id,
            &content_sha256,
            request.bytes.len() as i64,
            &write_pool,
        )
        .await?;

        Ok(StoredBlobRef {
            id: blob_id,
            domain_id: request.domain_id,
            content_sha256,
            size_octets: request.bytes.len() as i64,
            created: true,
        })
    }

    async fn effective_write_storage_pool_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: &PutBlobRequest<'_>,
    ) -> Result<WriteStoragePool> {
        let tenant_id = *request.tenant_id;
        let row = sqlx::query(
            r#"
            SELECT sp.id, sp.pool_kind, sp.config_json
            FROM storage_policy_assignments spa
            JOIN storage_pools sp
              ON sp.id = spa.storage_pool_id
             AND sp.status = 'active'
            WHERE spa.scope_kind = 'platform'
               OR (spa.scope_kind = 'tenant' AND spa.tenant_id = $1)
               OR (spa.scope_kind = 'domain' AND spa.tenant_id = $1 AND spa.domain_id = $2)
            ORDER BY CASE spa.scope_kind
                WHEN 'domain' THEN 1
                WHEN 'tenant' THEN 2
                WHEN 'platform' THEN 3
                ELSE 4
            END
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(request.domain_id)
        .fetch_optional(&mut **tx)
        .await?;

        let Some(row) = row else {
            return Ok(WriteStoragePool {
                id: POSTGRES_PRIMARY_STORAGE_POOL_ID,
                backend: StorageBackendSelection::Postgres,
            });
        };
        let pool_kind: String = row.try_get("pool_kind")?;
        let config_json: serde_json::Value = row.try_get("config_json")?;
        Ok(WriteStoragePool {
            id: row.try_get("id")?,
            backend: select_storage_backend(&pool_kind, &config_json)?,
        })
    }

    async fn ensure_backend_placement_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: &PutBlobRequest<'_>,
        blob_id: Uuid,
        content_sha256: &str,
        size_octets: i64,
        write_pool: &WriteStoragePool,
    ) -> Result<()> {
        if self
            .active_blob_placement_exists_in_tx(tx, request, blob_id)
            .await?
        {
            return Ok(());
        }

        let placement_id = Uuid::new_v4();
        if let StorageBackendSelection::S3Compatible(config) = &write_pool.backend {
            let stat = s3_put_object(
                config,
                placement_id,
                request.bytes,
                content_sha256,
                size_octets,
            )
            .await?;
            if stat.content_sha256 != content_sha256 {
                return Err(anyhow!(
                    "storage backend upload checksum verification failed"
                ));
            }
            if stat.size_octets != size_octets {
                return Err(anyhow!("storage backend upload size verification failed"));
            }
        }

        sqlx::query(
            r#"
            INSERT INTO blob_placements (
                id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
                placement_status, verified_content_sha256, verified_size_octets, verified_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8, NOW())
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(placement_id)
        .bind(request.tenant_id)
        .bind(request.domain_id)
        .bind(blob_id)
        .bind(request.kind.as_str())
        .bind(write_pool.id)
        .bind(content_sha256)
        .bind(size_octets)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn active_blob_placement_exists_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: &PutBlobRequest<'_>,
        blob_id: Uuid,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blob_placements
                WHERE tenant_id = $1
                  AND domain_id = $2
                  AND blob_id = $3
                  AND blob_kind = $4
                  AND placement_status = 'active'
            )
            "#,
        )
        .bind(request.tenant_id)
        .bind(request.domain_id)
        .bind(blob_id)
        .bind(request.kind.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub(crate) async fn create_blob_migration_job(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        blob_kind: &str,
        blob_id: Uuid,
        target_storage_pool_id: Uuid,
    ) -> Result<BlobMigrationJob> {
        let blob_kind = normalize_migration_blob_kind(blob_kind)?;
        if let Some(existing) = self
            .existing_open_migration_job(
                pool,
                tenant_id,
                domain_id,
                blob_id,
                target_storage_pool_id,
            )
            .await?
        {
            return Ok(existing);
        }

        let source = sqlx::query(
            r#"
            SELECT bp.id, bp.storage_pool_id
            FROM blobs b
            JOIN blob_placements bp
              ON bp.tenant_id = b.tenant_id
             AND bp.domain_id = b.domain_id
             AND bp.blob_id = b.id
             AND bp.blob_kind = b.blob_kind
             AND bp.placement_status = 'active'
            JOIN storage_pools sp
              ON sp.id = bp.storage_pool_id
             AND sp.status = 'active'
            WHERE b.tenant_id = $1
              AND b.domain_id = $2
              AND b.blob_kind = $3
              AND b.id = $4
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_kind)
        .bind(blob_id)
        .fetch_optional(pool)
        .await?;

        let Some(source) = source else {
            return Err(anyhow!(
                "durable blob {blob_id} has no active source storage placement"
            ));
        };
        let source_placement_id: Uuid = source.try_get("id")?;
        let source_storage_pool_id: Uuid = source.try_get("storage_pool_id")?;
        if source_storage_pool_id == target_storage_pool_id {
            return Err(anyhow!(
                "source and target storage pools must differ for blob migration"
            ));
        }

        let target_pool = sqlx::query(
            r#"
            SELECT pool_kind, config_json
            FROM storage_pools
            WHERE id = $1
              AND status = 'active'
            "#,
        )
        .bind(target_storage_pool_id)
        .fetch_optional(pool)
        .await?;

        let Some(target_pool) = target_pool else {
            return Err(anyhow!(
                "target storage pool {target_storage_pool_id} is not active or is unsupported"
            ));
        };
        let target_pool_kind: String = target_pool.try_get("pool_kind")?;
        let target_config_json: serde_json::Value = target_pool.try_get("config_json")?;
        select_storage_backend(&target_pool_kind, &target_config_json)?;

        let job_id = Uuid::new_v4();
        let inserted = sqlx::query(
            r#"
            INSERT INTO blob_migration_jobs (
                id, tenant_id, domain_id, blob_id, blob_kind,
                source_placement_id, source_storage_pool_id, target_storage_pool_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(blob_kind)
        .bind(source_placement_id)
        .bind(source_storage_pool_id)
        .bind(target_storage_pool_id)
        .fetch_one(pool)
        .await;

        match inserted {
            Ok(row) => blob_migration_job_from_row(row),
            Err(error) if is_constraint_error(&error, "blob_migration_jobs_open_target_idx") => {
                self.existing_open_migration_job(
                    pool,
                    tenant_id,
                    domain_id,
                    blob_id,
                    target_storage_pool_id,
                )
                .await?
                .ok_or_else(|| anyhow!("duplicate migration job was not readable after conflict"))
            }
            Err(error) => Err(error.into()),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn load_pending_blob_migration_jobs(
        &self,
        pool: &PgPool,
        limit: i64,
    ) -> Result<Vec<BlobMigrationJob>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            FROM blob_migration_jobs
            WHERE status = 'pending'
              AND next_attempt_at <= NOW()
            ORDER BY next_attempt_at ASC, created_at ASC, id ASC
            LIMIT $1
            "#,
        )
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;

        rows.into_iter().map(blob_migration_job_from_row).collect()
    }

    #[allow(dead_code)]
    pub(crate) async fn copy_and_verify_one_blob_migration_job(
        &self,
        pool: &PgPool,
    ) -> Result<Option<BlobMigrationJob>> {
        let Some(job) = self.claim_blob_migration_job(pool).await? else {
            return Ok(None);
        };
        let kind = durable_blob_kind_from_str(&job.blob_kind)?;
        let source = match self.load_migration_source_placement(pool, &job, kind).await {
            Ok(Some(source)) => source,
            Ok(None) => {
                let failed = self
                    .record_blob_migration_failure(pool, job.id, "source placement is missing")
                    .await?;
                return Ok(Some(failed));
            }
            Err(error) => {
                let failed = self
                    .record_blob_migration_failure(pool, job.id, &error.to_string())
                    .await?;
                return Ok(Some(failed));
            }
        };
        let blob = match self.read_placement_bytes(&source).await {
            Ok(blob) => blob,
            Err(error) => {
                let failed = self
                    .record_blob_migration_failure(pool, job.id, &error.to_string())
                    .await?;
                return Ok(Some(failed));
            }
        };

        let actual_hash = format!("{:x}", Sha256::digest(&blob.bytes));
        if actual_hash != blob.content_sha256 || blob.bytes.len() as i64 != blob.size_octets {
            let failed = self
                .record_blob_migration_failure(
                    pool,
                    job.id,
                    "source blob checksum or size verification failed",
                )
                .await?;
            return Ok(Some(failed));
        }

        let target_placement_id = self
            .ensure_copying_target_placement(pool, &job, &blob)
            .await;
        let target = match target_placement_id {
            Ok(target) => target,
            Err(error) => {
                let failed = self
                    .record_blob_migration_failure(pool, job.id, &error.to_string())
                    .await?;
                return Ok(Some(failed));
            }
        };
        if let Err(error) = self
            .write_migration_target_placement(pool, &job, &target, &blob)
            .await
        {
            let failed = self
                .record_blob_migration_failure(pool, job.id, &error.to_string())
                .await?;
            return Ok(Some(failed));
        }

        let mut tx = pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE blob_placements
            SET placement_status = 'verified',
                verified_content_sha256 = $2,
                verified_size_octets = $3,
                verified_at = NOW(),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND id = $4
              AND placement_status IN ('copying', 'verified')
            "#,
        )
        .bind(&job.tenant_id)
        .bind(&blob.content_sha256)
        .bind(blob.size_octets)
        .bind(target.placement_id)
        .execute(&mut *tx)
        .await?;

        let row = sqlx::query(
            r#"
            UPDATE blob_migration_jobs
            SET status = 'verified',
                target_placement_id = $2,
                verified_at = NOW(),
                lease_expires_at = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND id = $3
              AND status IN ('running', 'verified')
            RETURNING
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            "#,
        )
        .bind(&job.tenant_id)
        .bind(target.placement_id)
        .bind(job.id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        blob_migration_job_from_row(row).map(Some)
    }

    #[allow(dead_code)]
    pub(crate) async fn switch_verified_blob_migration_job(
        &self,
        pool: &PgPool,
        job_id: Uuid,
    ) -> Result<Option<BlobMigrationJob>> {
        let mut tx = pool.begin().await?;
        let Some(job_row) = sqlx::query(
            r#"
            SELECT
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            FROM blob_migration_jobs
            WHERE id = $1
              AND status IN ('verified', 'switched')
            FOR UPDATE
            "#,
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await?
        else {
            tx.commit().await?;
            return Ok(None);
        };
        let job = blob_migration_job_from_row(job_row)?;
        if job.status == "switched" {
            tx.commit().await?;
            return Ok(Some(job));
        }
        let target_placement_id = job
            .target_placement_id
            .ok_or_else(|| anyhow!("verified migration job has no target placement"))?;

        let target_verified = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blob_placements bp
                JOIN storage_pools sp
                  ON sp.id = bp.storage_pool_id
                 AND sp.status = 'active'
                WHERE bp.tenant_id = $1
                  AND bp.domain_id = $2
                  AND bp.id = $3
                  AND bp.blob_id = $4
                  AND bp.blob_kind = $5
                  AND bp.storage_pool_id = $6
                  AND bp.placement_status = 'verified'
            )
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(target_placement_id)
        .bind(job.blob_id)
        .bind(&job.blob_kind)
        .bind(job.target_storage_pool_id)
        .fetch_one(&mut *tx)
        .await?;
        if !target_verified {
            return Err(anyhow!(
                "verified migration job target placement is not verified"
            ));
        }

        let source_retired = sqlx::query(
            r#"
            UPDATE blob_placements
            SET placement_status = 'retiring',
                rollback_until = NOW() + INTERVAL '1 hour',
                updated_at = NOW()
            WHERE tenant_id = $1
              AND domain_id = $2
              AND id = $3
              AND blob_id = $4
              AND blob_kind = $5
              AND storage_pool_id = $6
              AND placement_status = 'active'
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(job.source_placement_id)
        .bind(job.blob_id)
        .bind(&job.blob_kind)
        .bind(job.source_storage_pool_id)
        .execute(&mut *tx)
        .await?;
        if source_retired.rows_affected() != 1 {
            return Err(anyhow!(
                "verified migration job source placement is not active"
            ));
        }

        let target_activated = sqlx::query(
            r#"
            UPDATE blob_placements
            SET placement_status = 'active',
                rollback_until = NULL,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND domain_id = $2
              AND id = $3
              AND blob_id = $4
              AND blob_kind = $5
              AND storage_pool_id = $6
              AND placement_status = 'verified'
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(target_placement_id)
        .bind(job.blob_id)
        .bind(&job.blob_kind)
        .bind(job.target_storage_pool_id)
        .execute(&mut *tx)
        .await?;
        if target_activated.rows_affected() != 1 {
            return Err(anyhow!(
                "verified migration job target placement could not be activated"
            ));
        }

        let target_pool = sqlx::query(
            r#"
            SELECT pool_kind, config_json
            FROM storage_pools
            WHERE id = $1
            "#,
        )
        .bind(job.target_storage_pool_id)
        .fetch_one(&mut *tx)
        .await?;
        let target_pool_kind: String = target_pool.try_get("pool_kind")?;
        let target_config_json: serde_json::Value = target_pool.try_get("config_json")?;
        if matches!(
            select_storage_backend(&target_pool_kind, &target_config_json)?,
            StorageBackendSelection::S3Compatible(_)
        ) {
            sqlx::query(
                r#"
                UPDATE blobs
                SET blob_bytes = NULL,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND domain_id = $2
                  AND id = $3
                  AND blob_kind = $4
                "#,
            )
            .bind(&job.tenant_id)
            .bind(job.domain_id)
            .bind(job.blob_id)
            .bind(&job.blob_kind)
            .execute(&mut *tx)
            .await?;
        }

        let switched = sqlx::query(
            r#"
            UPDATE blob_migration_jobs
            SET status = 'switched',
                switched_at = NOW(),
                rollback_until = (
                    SELECT rollback_until
                    FROM blob_placements
                    WHERE tenant_id = $1 AND id = $2
                ),
                lease_expires_at = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND id = $3
              AND status = 'verified'
            RETURNING
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.source_placement_id)
        .bind(job.id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        blob_migration_job_from_row(switched).map(Some)
    }

    #[allow(dead_code)]
    async fn claim_blob_migration_job(&self, pool: &PgPool) -> Result<Option<BlobMigrationJob>> {
        sqlx::query(
            r#"
            UPDATE blob_migration_jobs
            SET status = 'running',
                attempts = attempts + 1,
                started_at = NOW(),
                lease_expires_at = NOW() + INTERVAL '5 minutes',
                updated_at = NOW()
            WHERE id = (
                SELECT id
                FROM blob_migration_jobs
                WHERE status IN ('pending', 'failed')
                  AND next_attempt_at <= NOW()
                ORDER BY next_attempt_at ASC, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            "#,
        )
        .fetch_optional(pool)
        .await?
        .map(blob_migration_job_from_row)
        .transpose()
    }

    async fn ensure_copying_target_placement(
        &self,
        pool: &PgPool,
        job: &BlobMigrationJob,
        blob: &StoredBlobBytes,
    ) -> Result<MigrationTargetPlacement> {
        let pool_row = sqlx::query(
            r#"
            SELECT pool_kind, config_json
            FROM storage_pools
            WHERE id = $1
              AND status = 'active'
            "#,
        )
        .bind(job.target_storage_pool_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "target storage pool {} is not active or is unsupported",
                job.target_storage_pool_id
            )
        })?;
        let pool_kind: String = pool_row.try_get("pool_kind")?;
        let config_json: serde_json::Value = pool_row.try_get("config_json")?;
        let backend = select_storage_backend(&pool_kind, &config_json)?;

        if let Some(placement_id) = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM blob_placements
            WHERE tenant_id = $1
              AND domain_id = $2
              AND blob_id = $3
              AND blob_kind = $4
              AND storage_pool_id = $5
              AND placement_status IN ('copying', 'verified')
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(job.blob_id)
        .bind(&job.blob_kind)
        .bind(job.target_storage_pool_id)
        .fetch_optional(pool)
        .await?
        {
            return Ok(MigrationTargetPlacement {
                placement_id,
                backend,
            });
        }

        let placement_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO blob_placements (
                id, tenant_id, domain_id, blob_id, blob_kind, storage_pool_id,
                placement_status, verified_content_sha256, verified_size_octets
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'copying', $7, $8)
            RETURNING id
            "#,
        )
        .bind(placement_id)
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(job.blob_id)
        .bind(&job.blob_kind)
        .bind(job.target_storage_pool_id)
        .bind(&blob.content_sha256)
        .bind(blob.size_octets)
        .fetch_one(pool)
        .await?;
        Ok(MigrationTargetPlacement {
            placement_id,
            backend,
        })
    }

    #[allow(dead_code)]
    async fn record_blob_migration_failure(
        &self,
        pool: &PgPool,
        job_id: Uuid,
        error: &str,
    ) -> Result<BlobMigrationJob> {
        let row = sqlx::query(
            r#"
            UPDATE blob_migration_jobs
            SET status = 'failed',
                next_attempt_at = NOW() + INTERVAL '1 minute',
                last_error = $2,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            "#,
        )
        .bind(job_id)
        .bind(error)
        .fetch_one(pool)
        .await?;
        blob_migration_job_from_row(row)
    }

    #[allow(dead_code)]
    async fn existing_open_migration_job(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        blob_id: Uuid,
        target_storage_pool_id: Uuid,
    ) -> Result<Option<BlobMigrationJob>> {
        sqlx::query(
            r#"
            SELECT
                id, tenant_id, domain_id, blob_id, blob_kind, source_placement_id,
                source_storage_pool_id, target_storage_pool_id, target_placement_id,
                status, attempts
            FROM blob_migration_jobs
            WHERE tenant_id = $1
              AND domain_id = $2
              AND blob_id = $3
              AND target_storage_pool_id = $4
              AND status IN ('pending', 'running', 'verified')
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(target_storage_pool_id)
        .fetch_optional(pool)
        .await?
        .map(blob_migration_job_from_row)
        .transpose()
    }

    #[allow(dead_code)]
    pub(crate) async fn old_placement_cleanup_eligibility(
        &self,
        pool: &PgPool,
        placement_id: Uuid,
    ) -> Result<PlacementCleanupEligibility> {
        let Some(row) = sqlx::query(
            r#"
            SELECT
                bp.tenant_id,
                bp.domain_id,
                bp.blob_id,
                bp.blob_kind,
                bp.placement_status,
                bp.rollback_until IS NULL OR bp.rollback_until > NOW() AS rollback_window_active,
                b.retained_until IS NOT NULL AND b.retained_until > NOW() AS blob_retention_active,
                b.legal_hold AS blob_legal_hold_active
            FROM blob_placements bp
            JOIN blobs b
              ON b.tenant_id = bp.tenant_id
             AND b.domain_id = bp.domain_id
             AND b.id = bp.blob_id
             AND b.blob_kind = bp.blob_kind
            WHERE bp.id = $1
            LIMIT 1
            "#,
        )
        .bind(placement_id)
        .fetch_optional(pool)
        .await?
        else {
            return Ok(PlacementCleanupEligibility {
                placement_id,
                blockers: vec!["placement_not_found".to_string()],
            });
        };

        let tenant_id: Uuid = row.try_get("tenant_id")?;
        let domain_id: Uuid = row.try_get("domain_id")?;
        let blob_id: Uuid = row.try_get("blob_id")?;
        let blob_kind: String = row.try_get("blob_kind")?;
        let placement_status: String = row.try_get("placement_status")?;
        let mut blockers = Vec::new();

        if !matches!(placement_status.as_str(), "retiring" | "cleanup_failed") {
            blockers.push("placement_not_retiring".to_string());
        }
        if row.try_get::<bool, _>("rollback_window_active")? {
            blockers.push("rollback_window_active".to_string());
        }

        let active_replacement_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blob_placements bp
                JOIN blobs b
                  ON b.tenant_id = bp.tenant_id
                 AND b.domain_id = bp.domain_id
                 AND b.id = bp.blob_id
                 AND b.blob_kind = bp.blob_kind
                 AND b.content_sha256 = bp.verified_content_sha256
                 AND b.size_octets = bp.verified_size_octets
                JOIN storage_pools sp
                  ON sp.id = bp.storage_pool_id
                 AND sp.pool_kind = 'postgres'
                 AND sp.status = 'active'
                WHERE bp.tenant_id = $1
                  AND bp.domain_id = $2
                  AND bp.blob_id = $3
                  AND bp.blob_kind = $4
                  AND bp.id <> $5
                  AND bp.placement_status = 'active'
            )
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(&blob_kind)
        .bind(placement_id)
        .fetch_one(pool)
        .await?;
        if !active_replacement_exists {
            blockers.push("active_replacement_missing".to_string());
            blockers.extend(
                self.live_reference_cleanup_blockers(pool, tenant_id, domain_id, blob_id)
                    .await?,
            );
        }

        if row.try_get::<bool, _>("blob_retention_active")? {
            blockers.push("blob_retention_active".to_string());
        }
        if row.try_get::<bool, _>("blob_legal_hold_active")? {
            blockers.push("blob_legal_hold_active".to_string());
        }
        blockers.extend(
            self.message_lifecycle_cleanup_blockers(pool, tenant_id, domain_id, blob_id)
                .await?,
        );

        blockers.sort();
        blockers.dedup();
        Ok(PlacementCleanupEligibility {
            placement_id,
            blockers,
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn cleanup_old_retiring_placements(
        &self,
        pool: &PgPool,
        limit: i64,
    ) -> Result<Vec<PlacementCleanupResult>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let placement_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM blob_placements
            WHERE placement_status IN ('retiring', 'cleanup_failed')
              AND (next_cleanup_attempt_at IS NULL OR next_cleanup_attempt_at <= NOW())
            ORDER BY rollback_until ASC NULLS LAST, updated_at ASC, id ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(pool)
        .await?;

        let mut results = Vec::with_capacity(placement_ids.len());
        for placement_id in placement_ids {
            results.push(self.cleanup_one_old_placement(pool, placement_id).await?);
        }
        Ok(results)
    }

    #[allow(dead_code)]
    pub(crate) async fn cleanup_one_old_placement(
        &self,
        pool: &PgPool,
        placement_id: Uuid,
    ) -> Result<PlacementCleanupResult> {
        self.cleanup_one_old_placement_inner(pool, placement_id, None)
            .await
    }

    async fn cleanup_one_old_placement_inner(
        &self,
        pool: &PgPool,
        placement_id: Uuid,
        forced_error: Option<&str>,
    ) -> Result<PlacementCleanupResult> {
        if let Some(status) = self.placement_status(pool, placement_id).await? {
            if status == "deleted" {
                return Ok(PlacementCleanupResult {
                    placement_id,
                    cleaned: false,
                    status,
                    blockers: Vec::new(),
                    error: None,
                });
            }
        }

        let eligibility = self
            .old_placement_cleanup_eligibility(pool, placement_id)
            .await?;
        if !eligibility.blockers.is_empty() {
            let status = self
                .placement_status(pool, placement_id)
                .await?
                .unwrap_or_else(|| "missing".to_string());
            return Ok(PlacementCleanupResult {
                placement_id,
                cleaned: false,
                status,
                blockers: eligibility.blockers,
                error: None,
            });
        }

        let claimed = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE blob_placements
            SET placement_status = 'cleaning',
                cleanup_attempts = cleanup_attempts + 1,
                cleanup_claimed_at = NOW(),
                cleanup_error = NULL,
                next_cleanup_attempt_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND placement_status IN ('retiring', 'cleanup_failed')
              AND (next_cleanup_attempt_at IS NULL OR next_cleanup_attempt_at <= NOW())
            RETURNING placement_status
            "#,
        )
        .bind(placement_id)
        .fetch_optional(pool)
        .await?;

        if claimed.is_none() {
            let status = self
                .placement_status(pool, placement_id)
                .await?
                .unwrap_or_else(|| "missing".to_string());
            return Ok(PlacementCleanupResult {
                placement_id,
                cleaned: false,
                status,
                blockers: vec!["cleanup_claim_failed".to_string()],
                error: None,
            });
        }

        if let Some(error) = forced_error {
            self.record_placement_cleanup_failure(pool, placement_id, error)
                .await?;
            return Ok(PlacementCleanupResult {
                placement_id,
                cleaned: false,
                status: "cleanup_failed".to_string(),
                blockers: Vec::new(),
                error: Some(error.to_string()),
            });
        }

        let updated = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE blob_placements
            SET placement_status = 'deleted',
                cleaned_at = NOW(),
                cleanup_error = NULL,
                next_cleanup_attempt_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND placement_status = 'cleaning'
            RETURNING placement_status
            "#,
        )
        .bind(placement_id)
        .fetch_optional(pool)
        .await;

        match updated {
            Ok(Some(status)) => Ok(PlacementCleanupResult {
                placement_id,
                cleaned: true,
                status,
                blockers: Vec::new(),
                error: None,
            }),
            Ok(None) => {
                let error = "cleanup placement was not in cleaning state";
                self.record_placement_cleanup_failure(pool, placement_id, error)
                    .await?;
                Ok(PlacementCleanupResult {
                    placement_id,
                    cleaned: false,
                    status: "cleanup_failed".to_string(),
                    blockers: Vec::new(),
                    error: Some(error.to_string()),
                })
            }
            Err(error) => {
                let error = error.to_string();
                self.record_placement_cleanup_failure(pool, placement_id, &error)
                    .await?;
                Ok(PlacementCleanupResult {
                    placement_id,
                    cleaned: false,
                    status: "cleanup_failed".to_string(),
                    blockers: Vec::new(),
                    error: Some(error),
                })
            }
        }
    }

    #[allow(dead_code)]
    async fn placement_status(&self, pool: &PgPool, placement_id: Uuid) -> Result<Option<String>> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT placement_status
            FROM blob_placements
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(placement_id)
        .fetch_optional(pool)
        .await
        .map_err(Into::into)
    }

    #[allow(dead_code)]
    async fn record_placement_cleanup_failure(
        &self,
        pool: &PgPool,
        placement_id: Uuid,
        error: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE blob_placements
            SET placement_status = 'cleanup_failed',
                cleanup_error = $2,
                next_cleanup_attempt_at = NOW() + INTERVAL '1 minute',
                updated_at = NOW()
            WHERE id = $1
              AND placement_status = 'cleaning'
            "#,
        )
        .bind(placement_id)
        .bind(error)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[cfg(test)]
    async fn simulate_old_placement_cleanup_failure(
        &self,
        pool: &PgPool,
        placement_id: Uuid,
        error: &str,
    ) -> Result<PlacementCleanupResult> {
        self.cleanup_one_old_placement_inner(pool, placement_id, Some(error))
            .await
    }

    #[allow(dead_code)]
    async fn live_reference_cleanup_blockers(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
        domain_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Vec<String>> {
        let tenant = tenant_id;
        let mut blockers = Vec::new();
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM messages m
                WHERE m.tenant_id = $1
                  AND m.domain_id = $2
                  AND m.blob_id = $3
                  AND EXISTS (
                      SELECT 1
                      FROM mailbox_messages mm
                      WHERE mm.tenant_id = m.tenant_id
                        AND mm.message_id = m.id
                        AND mm.visibility <> 'expunged'
                  )
            )
            "#,
        )
        .bind(&tenant)
        .bind(domain_id)
        .bind(blob_id)
        .fetch_one(pool)
        .await?
        {
            blockers.push("live_message_reference".to_string());
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mime_parts mp
                WHERE mp.tenant_id = $1
                  AND mp.domain_id = $2
                  AND mp.blob_id = $3
                  AND EXISTS (
                      SELECT 1
                      FROM mailbox_messages mm
                      WHERE mm.tenant_id = mp.tenant_id
                        AND mm.message_id = mp.message_id
                        AND mm.visibility <> 'expunged'
                  )
            )
            "#,
        )
        .bind(&tenant)
        .bind(domain_id)
        .bind(blob_id)
        .fetch_one(pool)
        .await?
        {
            blockers.push("live_mime_part_reference".to_string());
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM attachments a
                WHERE a.tenant_id = $1
                  AND a.domain_id = $2
                  AND a.blob_id = $3
                  AND EXISTS (
                      SELECT 1
                      FROM mailbox_messages mm
                      WHERE mm.tenant_id = a.tenant_id
                        AND mm.message_id = a.message_id
                        AND mm.visibility <> 'expunged'
                  )
            )
            "#,
        )
        .bind(&tenant)
        .bind(domain_id)
        .bind(blob_id)
        .fetch_one(pool)
        .await?
        {
            blockers.push("live_attachment_reference".to_string());
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM attachment_extraction_jobs
                WHERE tenant_id = $1 AND blob_id = $2
            )
            "#,
        )
        .bind(&tenant)
        .bind(blob_id)
        .fetch_one(pool)
        .await?
        {
            blockers.push("live_extraction_job_reference".to_string());
        }
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM attachment_texts
                WHERE tenant_id = $1 AND blob_id = $2
            )
            "#,
        )
        .bind(&tenant)
        .bind(blob_id)
        .fetch_one(pool)
        .await?
        {
            blockers.push("live_attachment_text_reference".to_string());
        }
        Ok(blockers)
    }

    #[allow(dead_code)]
    async fn message_lifecycle_cleanup_blockers(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
        domain_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Vec<String>> {
        let tenant = tenant_id;
        let row = sqlx::query(
            r#"
            SELECT
                EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE m.tenant_id = $1
                      AND m.domain_id = $2
                      AND m.retained_until IS NOT NULL
                      AND m.retained_until > NOW()
                      AND (
                          m.blob_id = $3
                          OR EXISTS (
                              SELECT 1
                              FROM mime_parts mp
                              WHERE mp.tenant_id = m.tenant_id
                                AND mp.message_id = m.id
                                AND mp.blob_id = $3
                          )
                          OR EXISTS (
                              SELECT 1
                              FROM attachments a
                              WHERE a.tenant_id = m.tenant_id
                                AND a.message_id = m.id
                                AND a.blob_id = $3
                          )
                      )
                ) AS message_retention_active,
                EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE m.tenant_id = $1
                      AND m.domain_id = $2
                      AND m.legal_hold = TRUE
                      AND (
                          m.blob_id = $3
                          OR EXISTS (
                              SELECT 1
                              FROM mime_parts mp
                              WHERE mp.tenant_id = m.tenant_id
                                AND mp.message_id = m.id
                                AND mp.blob_id = $3
                          )
                          OR EXISTS (
                              SELECT 1
                              FROM attachments a
                              WHERE a.tenant_id = m.tenant_id
                                AND a.message_id = m.id
                                AND a.blob_id = $3
                          )
                      )
                ) AS message_legal_hold_active
            "#,
        )
        .bind(&tenant)
        .bind(domain_id)
        .bind(blob_id)
        .fetch_one(pool)
        .await?;
        let mut blockers = Vec::new();
        if row.try_get::<bool, _>("message_retention_active")? {
            blockers.push("message_retention_active".to_string());
        }
        if row.try_get::<bool, _>("message_legal_hold_active")? {
            blockers.push("message_legal_hold_active".to_string());
        }
        Ok(blockers)
    }

    pub(crate) async fn read_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<Option<StoredBlobBytes>> {
        let Some(placement) = self
            .load_active_blob_placement(pool, tenant_id, domain_id, kind, blob_id)
            .await?
        else {
            self.error_if_durable_blob_lacks_active_placement(
                pool, tenant_id, domain_id, kind, blob_id,
            )
            .await?;
            return Ok(None);
        };

        let bytes = match &placement.backend {
            StorageBackendSelection::Postgres => placement
                .blob_bytes
                .clone()
                .ok_or_else(|| anyhow!("database storage placement has no database blob bytes"))?,
            StorageBackendSelection::S3Compatible(config) => {
                let bytes = s3_read_object(config, placement.placement_id).await?;
                let actual_hash = format!("{:x}", Sha256::digest(&bytes));
                if actual_hash != placement.content_sha256 {
                    return Err(anyhow!("storage backend read checksum verification failed"));
                }
                if bytes.len() as i64 != placement.size_octets {
                    return Err(anyhow!("storage backend read size verification failed"));
                }
                bytes
            }
        };

        Ok(Some(StoredBlobBytes {
            id: placement.id,
            media_type: placement.media_type,
            size_octets: placement.size_octets,
            content_sha256: placement.content_sha256,
            bytes,
        }))
    }

    pub(crate) async fn stat_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<Option<StoredBlobStat>> {
        let Some(placement) = self
            .load_active_blob_placement(pool, tenant_id, domain_id, kind, blob_id)
            .await?
        else {
            self.error_if_durable_blob_lacks_active_placement(
                pool, tenant_id, domain_id, kind, blob_id,
            )
            .await?;
            return Ok(None);
        };

        if let StorageBackendSelection::S3Compatible(config) = &placement.backend {
            let stat = s3_stat_object(config, placement.placement_id).await?;
            if stat.content_sha256 != placement.content_sha256 {
                return Err(anyhow!("storage backend stat checksum verification failed"));
            }
            if stat.size_octets != placement.size_octets {
                return Err(anyhow!("storage backend stat size verification failed"));
            }
        }

        Ok(Some(StoredBlobStat {
            id: placement.id,
            media_type: placement.media_type,
            size_octets: placement.size_octets,
            content_sha256: placement.content_sha256,
        }))
    }

    async fn load_migration_source_placement(
        &self,
        pool: &PgPool,
        job: &BlobMigrationJob,
        kind: DurableBlobKind,
    ) -> Result<Option<ActiveBlobPlacement>> {
        let row = sqlx::query(
            r#"
            SELECT
                b.id,
                b.media_type,
                b.size_octets,
                b.content_sha256,
                b.blob_bytes,
                bp.id AS placement_id,
                sp.pool_kind,
                sp.config_json
            FROM blobs b
            JOIN blob_placements bp
              ON bp.tenant_id = b.tenant_id
             AND bp.domain_id = b.domain_id
             AND bp.blob_id = b.id
             AND bp.blob_kind = b.blob_kind
             AND bp.verified_content_sha256 = b.content_sha256
             AND bp.verified_size_octets = b.size_octets
             AND bp.placement_status = 'active'
            JOIN storage_pools sp
              ON sp.id = bp.storage_pool_id
             AND sp.status = 'active'
            WHERE b.tenant_id = $1
              AND b.domain_id = $2
              AND b.blob_kind = $3
              AND b.id = $4
              AND bp.id = $5
              AND bp.storage_pool_id = $6
            LIMIT 1
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.domain_id)
        .bind(kind.as_str())
        .bind(job.blob_id)
        .bind(job.source_placement_id)
        .bind(job.source_storage_pool_id)
        .fetch_optional(pool)
        .await?;

        row.map(|row| {
            let pool_kind: String = row.try_get("pool_kind")?;
            let config_json: serde_json::Value = row.try_get("config_json")?;
            Ok(ActiveBlobPlacement {
                placement_id: row.try_get("placement_id")?,
                backend: select_storage_backend(&pool_kind, &config_json)?,
                id: row.try_get("id")?,
                media_type: row.try_get("media_type")?,
                size_octets: row.try_get("size_octets")?,
                content_sha256: row.try_get("content_sha256")?,
                blob_bytes: row.try_get("blob_bytes")?,
            })
        })
        .transpose()
    }

    async fn read_placement_bytes(
        &self,
        placement: &ActiveBlobPlacement,
    ) -> Result<StoredBlobBytes> {
        let bytes = match &placement.backend {
            StorageBackendSelection::Postgres => placement
                .blob_bytes
                .clone()
                .ok_or_else(|| anyhow!("database storage placement has no database blob bytes"))?,
            StorageBackendSelection::S3Compatible(config) => {
                s3_read_object(config, placement.placement_id).await?
            }
        };
        Ok(StoredBlobBytes {
            id: placement.id,
            media_type: placement.media_type.clone(),
            size_octets: placement.size_octets,
            content_sha256: placement.content_sha256.clone(),
            bytes,
        })
    }

    async fn write_migration_target_placement(
        &self,
        pool: &PgPool,
        job: &BlobMigrationJob,
        target: &MigrationTargetPlacement,
        blob: &StoredBlobBytes,
    ) -> Result<()> {
        match &target.backend {
            StorageBackendSelection::Postgres => {
                let updated = sqlx::query(
                    r#"
                    UPDATE blobs
                    SET blob_bytes = $5
                    WHERE tenant_id = $1
                      AND domain_id = $2
                      AND blob_kind = $3
                      AND id = $4
                      AND content_sha256 = $6
                      AND size_octets = $7
                    "#,
                )
                .bind(&job.tenant_id)
                .bind(job.domain_id)
                .bind(&job.blob_kind)
                .bind(job.blob_id)
                .bind(&blob.bytes)
                .bind(&blob.content_sha256)
                .bind(blob.size_octets)
                .execute(pool)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(anyhow!("target database blob row could not be updated"));
                }

                let stored_bytes = sqlx::query_scalar::<_, Option<Vec<u8>>>(
                    r#"
                    SELECT blob_bytes
                    FROM blobs
                    WHERE tenant_id = $1
                      AND domain_id = $2
                      AND blob_kind = $3
                      AND id = $4
                    "#,
                )
                .bind(&job.tenant_id)
                .bind(job.domain_id)
                .bind(&job.blob_kind)
                .bind(job.blob_id)
                .fetch_one(pool)
                .await?
                .ok_or_else(|| anyhow!("target database blob row has no database bytes"))?;
                let stored_hash = format!("{:x}", Sha256::digest(&stored_bytes));
                if stored_hash != blob.content_sha256
                    || stored_bytes.len() as i64 != blob.size_octets
                {
                    return Err(anyhow!(
                        "target database placement checksum or size verification failed"
                    ));
                }
            }
            StorageBackendSelection::S3Compatible(config) => {
                let stat = s3_put_object(
                    config,
                    target.placement_id,
                    &blob.bytes,
                    &blob.content_sha256,
                    blob.size_octets,
                )
                .await?;
                if stat.content_sha256 != blob.content_sha256
                    || stat.size_octets != blob.size_octets
                {
                    return Err(anyhow!(
                        "target storage placement checksum or size verification failed"
                    ));
                }
            }
        }
        Ok(())
    }

    async fn load_active_blob_placement(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<Option<ActiveBlobPlacement>> {
        let row = sqlx::query(
            r#"
            SELECT
                b.id,
                b.media_type,
                b.size_octets,
                b.content_sha256,
                b.blob_bytes,
                bp.id AS placement_id,
                sp.pool_kind,
                sp.config_json
            FROM blobs b
            JOIN blob_placements bp
              ON bp.tenant_id = b.tenant_id
             AND bp.domain_id = b.domain_id
             AND bp.blob_id = b.id
             AND bp.blob_kind = b.blob_kind
             AND bp.verified_content_sha256 = b.content_sha256
             AND bp.verified_size_octets = b.size_octets
             AND bp.placement_status = 'active'
            JOIN storage_pools sp
              ON sp.id = bp.storage_pool_id
             AND sp.status = 'active'
            WHERE b.tenant_id = $1
              AND b.domain_id = $2
              AND b.blob_kind = $3
              AND b.id = $4
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(kind.as_str())
        .bind(blob_id)
        .fetch_optional(pool)
        .await?;

        row.map(|row| {
            let pool_kind: String = row.try_get("pool_kind")?;
            let config_json: serde_json::Value = row.try_get("config_json")?;
            Ok(ActiveBlobPlacement {
                placement_id: row.try_get("placement_id")?,
                backend: select_storage_backend(&pool_kind, &config_json)?,
                id: row.try_get("id")?,
                media_type: row.try_get("media_type")?,
                size_octets: row.try_get("size_octets")?,
                content_sha256: row.try_get("content_sha256")?,
                blob_bytes: row.try_get("blob_bytes")?,
            })
        })
        .transpose()
    }

    async fn error_if_durable_blob_lacks_active_placement(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<()> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blobs
                WHERE tenant_id = $1
                  AND domain_id = $2
                  AND blob_kind = $3
                  AND id = $4
            )
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .bind(kind.as_str())
        .bind(blob_id)
        .fetch_one(pool)
        .await?;

        if exists {
            return Err(anyhow!(
                "durable blob {blob_id} has no active storage placement"
            ));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn verify_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &Uuid,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<bool> {
        let Some(placement) = self
            .load_active_blob_placement(pool, tenant_id, domain_id, kind, blob_id)
            .await?
        else {
            self.error_if_durable_blob_lacks_active_placement(
                pool, tenant_id, domain_id, kind, blob_id,
            )
            .await?;
            return Ok(false);
        };
        match &placement.backend {
            StorageBackendSelection::Postgres => {
                let Some(blob_bytes) = placement.blob_bytes.as_ref() else {
                    return Ok(false);
                };
                let actual_hash = format!("{:x}", Sha256::digest(blob_bytes));
                Ok(actual_hash == placement.content_sha256
                    && blob_bytes.len() as i64 == placement.size_octets)
            }
            StorageBackendSelection::S3Compatible(config) => {
                let stat = s3_stat_object(config, placement.placement_id).await?;
                Ok(stat.content_sha256 == placement.content_sha256
                    && stat.size_octets == placement.size_octets)
            }
        }
    }
}

#[allow(dead_code)]
fn normalize_migration_blob_kind(blob_kind: &str) -> Result<&'static str> {
    match blob_kind.trim() {
        "attachment" => Ok("attachment"),
        "mime_part" => Ok("mime_part"),
        _ => Err(anyhow!(
            "blob migration jobs only support durable attachment and MIME-part blobs"
        )),
    }
}

#[allow(dead_code)]
fn durable_blob_kind_from_str(blob_kind: &str) -> Result<DurableBlobKind> {
    match normalize_migration_blob_kind(blob_kind)? {
        "attachment" => Ok(DurableBlobKind::Attachment),
        "mime_part" => Ok(DurableBlobKind::MimePart),
        _ => unreachable!("normalize_migration_blob_kind returned unsupported kind"),
    }
}

#[allow(dead_code)]
fn blob_migration_job_from_row(row: sqlx::postgres::PgRow) -> Result<BlobMigrationJob> {
    Ok(BlobMigrationJob {
        id: row.try_get("id")?,
        tenant_id: row.try_get::<Uuid, _>("tenant_id")?,
        domain_id: row.try_get("domain_id")?,
        blob_id: row.try_get("blob_id")?,
        blob_kind: row.try_get("blob_kind")?,
        source_placement_id: row.try_get("source_placement_id")?,
        source_storage_pool_id: row.try_get("source_storage_pool_id")?,
        target_storage_pool_id: row.try_get("target_storage_pool_id")?,
        target_placement_id: row.try_get("target_placement_id")?,
        status: row.try_get("status")?,
        attempts: row.try_get("attempts")?,
    })
}

#[allow(dead_code)]
fn is_constraint_error(error: &sqlx::Error, constraint: &str) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.constraint() == Some(constraint)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttachmentUploadInput, Storage};
    use serde_json::{json, Value};
    use sqlx::postgres::PgPoolOptions;

    const SCHEMA: &str = include_str!("../sql/schema.sql");

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
            VALUES ($1, $2, $3, 'inbox', 'Inbox', 1)
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

    async fn database_blob_bytes_len(
        storage: &Storage,
        tenant_id: Uuid,
        blob_id: Uuid,
    ) -> Option<i64> {
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
            VALUES ($1, $2, $3, 'inbox', 'Inbox', 1)
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
}
