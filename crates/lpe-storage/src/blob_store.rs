use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::sha256_hex;

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
    pub(crate) tenant_id: &'a str,
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
    pub(crate) tenant_id: String,
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

#[derive(Debug, Default)]
pub(crate) struct PostgresBlobStore;

impl PostgresBlobStore {
    pub(crate) async fn put_durable_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: PutBlobRequest<'_>,
    ) -> Result<StoredBlobRef> {
        let content_sha256 = sha256_hex(request.bytes);
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
            self.ensure_database_placement_in_tx(
                tx,
                &request,
                blob_id,
                &content_sha256,
                size_octets,
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
        .bind(request.bytes)
        .bind(request.magika_status)
        .bind(request.extraction_status)
        .bind(request.validated)
        .execute(&mut **tx)
        .await?;

        self.ensure_database_placement_in_tx(
            tx,
            &request,
            blob_id,
            &content_sha256,
            request.bytes.len() as i64,
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

    async fn ensure_database_placement_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        request: &PutBlobRequest<'_>,
        blob_id: Uuid,
        content_sha256: &str,
        size_octets: i64,
    ) -> Result<()> {
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
        .bind(Uuid::new_v4())
        .bind(request.tenant_id)
        .bind(request.domain_id)
        .bind(blob_id)
        .bind(request.kind.as_str())
        .bind(POSTGRES_PRIMARY_STORAGE_POOL_ID)
        .bind(content_sha256)
        .bind(size_octets)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn create_blob_migration_job(
        &self,
        pool: &PgPool,
        tenant_id: &str,
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
             AND sp.pool_kind = 'postgres'
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
                "durable blob {blob_id} has no active database source placement"
            ));
        };
        let source_placement_id: Uuid = source.try_get("id")?;
        let source_storage_pool_id: Uuid = source.try_get("storage_pool_id")?;
        if source_storage_pool_id == target_storage_pool_id {
            return Err(anyhow!(
                "source and target storage pools must differ for blob migration"
            ));
        }

        let target_pool_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM storage_pools
                WHERE id = $1
                  AND pool_kind = 'postgres'
                  AND status = 'active'
            )
            "#,
        )
        .bind(target_storage_pool_id)
        .fetch_one(pool)
        .await?;
        if !target_pool_exists {
            return Err(anyhow!(
                "target storage pool {target_storage_pool_id} is not an active database pool"
            ));
        }

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
        let blob = match self
            .read_durable_blob(pool, &job.tenant_id, job.domain_id, kind, job.blob_id)
            .await
        {
            Ok(Some(blob)) => blob,
            Ok(None) => {
                let failed = self
                    .record_blob_migration_failure(pool, job.id, "source blob is missing")
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

        let mut tx = pool.begin().await?;
        let target_placement_id = self
            .ensure_copying_target_placement_in_tx(&mut tx, &job, &blob)
            .await?;
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
        .bind(target_placement_id)
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
        .bind(target_placement_id)
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
                 AND sp.pool_kind = 'postgres'
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

    async fn ensure_copying_target_placement_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        job: &BlobMigrationJob,
        blob: &StoredBlobBytes,
    ) -> Result<Uuid> {
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
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(placement_id);
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
        .fetch_one(&mut **tx)
        .await?;
        Ok(placement_id)
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
        tenant_id: &str,
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

    pub(crate) async fn read_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<Option<StoredBlobBytes>> {
        let row = sqlx::query(
            r#"
            SELECT b.id, b.media_type, b.size_octets, b.content_sha256, b.blob_bytes
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
             AND sp.pool_kind = 'postgres'
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

        if let Some(row) = row {
            return Ok(Some(StoredBlobBytes {
                id: row.try_get("id")?,
                media_type: row.try_get("media_type")?,
                size_octets: row.try_get("size_octets")?,
                content_sha256: row.try_get("content_sha256")?,
                bytes: row.try_get("blob_bytes")?,
            }));
        }

        self.error_if_durable_blob_lacks_active_placement(
            pool, tenant_id, domain_id, kind, blob_id,
        )
        .await?;
        Ok(None)
    }

    pub(crate) async fn stat_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<Option<StoredBlobStat>> {
        let row = sqlx::query(
            r#"
            SELECT b.id, b.media_type, b.size_octets, b.content_sha256
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
             AND sp.pool_kind = 'postgres'
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

        if let Some(row) = row {
            return Ok(Some(StoredBlobStat {
                id: row.try_get("id")?,
                media_type: row.try_get("media_type")?,
                size_octets: row.try_get("size_octets")?,
                content_sha256: row.try_get("content_sha256")?,
            }));
        }

        self.error_if_durable_blob_lacks_active_placement(
            pool, tenant_id, domain_id, kind, blob_id,
        )
        .await?;
        Ok(None)
    }

    async fn error_if_durable_blob_lacks_active_placement(
        &self,
        pool: &PgPool,
        tenant_id: &str,
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
                "durable blob {blob_id} has no active database placement"
            ));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn verify_durable_blob(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        domain_id: Uuid,
        kind: DurableBlobKind,
        blob_id: Uuid,
    ) -> Result<bool> {
        let Some(blob) = self
            .read_durable_blob(pool, tenant_id, domain_id, kind, blob_id)
            .await?
        else {
            return Ok(false);
        };
        let actual_hash = format!("{:x}", Sha256::digest(&blob.bytes));
        Ok(actual_hash == blob.content_sha256 && blob.bytes.len() as i64 == blob.size_octets)
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
        tenant_id: row.try_get::<Uuid, _>("tenant_id")?.to_string(),
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
        let tenant = tenant_id.to_string();
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
                &tenant_id.to_string(),
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
        let tenant = tenant_id.to_string();

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
        let tenant = tenant_id.to_string();

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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
                domain_id,
                "attachment",
                blob.id,
                target_pool_id,
            )
            .await
            .expect_err("missing source placement must fail")
            .to_string();
        assert!(
            error.contains("no active database source placement"),
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
                &tenant_id.to_string(),
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
        let tenant = tenant_id.to_string();

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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
        let verified = create_verified_migration_job(
            &storage,
            tenant_id,
            domain_id,
            blob.id,
            target_pool_id,
        )
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
        let verified = create_verified_migration_job(
            &storage,
            tenant_id,
            domain_id,
            blob.id,
            target_pool_id,
        )
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
                &tenant_id.to_string(),
                domain_id,
                DurableBlobKind::Attachment,
                blob.id,
            )
            .await
            .expect("read before copy")
            .expect("blob before copy");
        assert_eq!(before.bytes, b"switch-read-path");

        let verified = create_verified_migration_job(
            &storage,
            tenant_id,
            domain_id,
            blob.id,
            target_pool_id,
        )
        .await;
        let during = blob_store
            .read_durable_blob(
                storage.pool(),
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
                &tenant_id.to_string(),
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
        let verified = create_verified_migration_job(
            &storage,
            tenant_id,
            domain_id,
            blob.id,
            target_pool_id,
        )
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
                &tenant_id.to_string(),
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
        let tenant = tenant_id.to_string();
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
                error.contains("active database placement"),
                "unexpected storage error: {error}"
            );
        }
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
        let tenant = tenant_id.to_string();
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
    }
}
