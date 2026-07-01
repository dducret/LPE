use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::{
    sha256_hex,
    storage_backend::{s3_put_object, select_storage_backend, StorageBackendSelection},
};

mod io;
mod types;

use types::{
    blob_migration_job_from_row, durable_blob_kind_from_str, is_constraint_error,
    normalize_migration_blob_kind, ActiveBlobPlacement, MigrationTargetPlacement, WriteStoragePool,
};
pub(crate) use types::{
    BlobMigrationJob, DurableBlobKind, PlacementCleanupEligibility, PlacementCleanupResult,
    PostgresBlobStore, PutBlobRequest, StoredBlobBytes, StoredBlobRef, StoredBlobStat,
};

const POSTGRES_PRIMARY_STORAGE_POOL_ID: Uuid = Uuid::from_u128(1);

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
}

#[cfg(test)]
mod tests;
