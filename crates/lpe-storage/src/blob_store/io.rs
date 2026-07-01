use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::storage_backend::{
    s3_put_object, s3_read_object, s3_stat_object, select_storage_backend, StorageBackendSelection,
};

use super::{
    ActiveBlobPlacement, BlobMigrationJob, DurableBlobKind, MigrationTargetPlacement,
    PostgresBlobStore, StoredBlobBytes, StoredBlobStat,
};

impl PostgresBlobStore {
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

    pub(super) async fn load_migration_source_placement(
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

    pub(super) async fn read_placement_bytes(
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

    pub(super) async fn write_migration_target_placement(
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
