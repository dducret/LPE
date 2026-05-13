use anyhow::{anyhow, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    Storage, StorageCleanupCounts, StorageCleanupPlacementSummary,
    StorageCleanupVisibilityResponse, StorageHealthResponse, StorageMigrationCounts,
    StorageMigrationJobSummary, StorageMigrationVisibilityResponse, StoragePlacementCounts,
    StoragePoolHealth, StoragePoolReference,
};

#[derive(Debug, Clone)]
struct PoolHealthRow {
    id: Uuid,
    name: String,
    pool_kind: String,
    status: String,
    active_placements: u64,
    retiring_placements: u64,
    failed_placements: u64,
    cleanup_failed_placements: u64,
}

#[derive(Debug, Clone)]
struct CleanupRow {
    placement_id: Uuid,
    tenant_id: Uuid,
    domain_id: Uuid,
    blob_kind: String,
    pool: StoragePoolReference,
    status: String,
    cleanup_attempts: u32,
    rollback_until: Option<String>,
    next_cleanup_attempt_at: Option<String>,
    cleaned_at: Option<String>,
    cleanup_error_summary: Option<String>,
}

impl Storage {
    pub async fn fetch_platform_storage_health(&self) -> Result<StorageHealthResponse> {
        self.fetch_storage_health(None).await
    }

    pub async fn fetch_tenant_storage_health(
        &self,
        tenant_id: Uuid,
    ) -> Result<StorageHealthResponse> {
        self.ensure_visibility_tenant_exists(tenant_id).await?;
        self.fetch_storage_health(Some(tenant_id)).await
    }

    pub async fn fetch_platform_storage_migrations(
        &self,
    ) -> Result<StorageMigrationVisibilityResponse> {
        self.fetch_storage_migrations(None).await
    }

    pub async fn fetch_tenant_storage_migrations(
        &self,
        tenant_id: Uuid,
    ) -> Result<StorageMigrationVisibilityResponse> {
        self.ensure_visibility_tenant_exists(tenant_id).await?;
        self.fetch_storage_migrations(Some(tenant_id)).await
    }

    pub async fn fetch_platform_storage_cleanup(&self) -> Result<StorageCleanupVisibilityResponse> {
        self.fetch_storage_cleanup(None).await
    }

    pub async fn fetch_tenant_storage_cleanup(
        &self,
        tenant_id: Uuid,
    ) -> Result<StorageCleanupVisibilityResponse> {
        self.ensure_visibility_tenant_exists(tenant_id).await?;
        self.fetch_storage_cleanup(Some(tenant_id)).await
    }

    async fn fetch_storage_health(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StorageHealthResponse> {
        let pools = self.load_pool_health_rows(tenant_filter).await?;
        let placements = self.load_placement_counts(tenant_filter).await?;
        let migrations = self.load_migration_counts(tenant_filter).await?;
        let cleanup = self.load_cleanup_counts(tenant_filter).await?;
        let degraded = placements.missing_active > 0
            || placements.degraded > 0
            || migrations.failed > 0
            || migrations.expired_leases > 0
            || cleanup.cleanup_failed > 0
            || cleanup.blocked_by_missing_active_replacement > 0;

        Ok(StorageHealthResponse {
            status: if degraded { "degraded" } else { "ok" }.to_string(),
            pools: pools.into_iter().map(pool_health_summary).collect(),
            placements,
            migrations,
            cleanup,
        })
    }

    async fn fetch_storage_migrations(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StorageMigrationVisibilityResponse> {
        let summary = self.load_migration_counts(tenant_filter).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                j.id,
                j.tenant_id,
                j.domain_id,
                j.blob_kind,
                source.id AS source_pool_id,
                source.name AS source_pool_name,
                source.pool_kind AS source_pool_kind,
                source.status AS source_pool_status,
                target.id AS target_pool_id,
                target.name AS target_pool_name,
                target.pool_kind AS target_pool_kind,
                target.status AS target_pool_status,
                j.status,
                j.attempts,
                to_char(j.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS next_attempt_at,
                j.last_error,
                CASE WHEN j.started_at IS NULL THEN NULL ELSE to_char(j.started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS started_at,
                CASE WHEN j.verified_at IS NULL THEN NULL ELSE to_char(j.verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS verified_at,
                CASE WHEN j.switched_at IS NULL THEN NULL ELSE to_char(j.switched_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS switched_at,
                CASE WHEN j.rollback_until IS NULL THEN NULL ELSE to_char(j.rollback_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS rollback_until
            FROM blob_migration_jobs j
            JOIN storage_pools source ON source.id = j.source_storage_pool_id
            JOIN storage_pools target ON target.id = j.target_storage_pool_id
            WHERE $1::uuid IS NULL OR j.tenant_id = $1
            ORDER BY j.updated_at DESC, j.created_at DESC, j.id ASC
            LIMIT 100
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        Ok(StorageMigrationVisibilityResponse {
            summary,
            items: rows
                .into_iter()
                .map(|row| {
                    Ok(StorageMigrationJobSummary {
                        id: row.try_get("id")?,
                        tenant_id: row.try_get("tenant_id")?,
                        domain_id: row.try_get("domain_id")?,
                        blob_kind: row.try_get("blob_kind")?,
                        source_pool: pool_reference_from_columns(&row, "source")?,
                        target_pool: pool_reference_from_columns(&row, "target")?,
                        status: row.try_get("status")?,
                        attempts: row.try_get::<i32, _>("attempts")?.max(0) as u32,
                        next_attempt_at: row.try_get("next_attempt_at")?,
                        last_error_summary: summarize_error(row.try_get("last_error")?),
                        started_at: row.try_get("started_at")?,
                        verified_at: row.try_get("verified_at")?,
                        switched_at: row.try_get("switched_at")?,
                        rollback_until: row.try_get("rollback_until")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    async fn fetch_storage_cleanup(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StorageCleanupVisibilityResponse> {
        let summary = self.load_cleanup_counts(tenant_filter).await?;
        let rows = self.load_cleanup_rows(tenant_filter).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let blockers = self.cleanup_blockers_for_row(&row).await?;
            items.push(StorageCleanupPlacementSummary {
                tenant_id: row.tenant_id,
                domain_id: row.domain_id,
                blob_kind: row.blob_kind,
                pool: row.pool,
                status: row.status,
                cleanup_attempts: row.cleanup_attempts,
                rollback_until: row.rollback_until,
                next_cleanup_attempt_at: row.next_cleanup_attempt_at,
                cleaned_at: row.cleaned_at,
                cleanup_error_summary: row.cleanup_error_summary,
                blockers,
            });
        }
        Ok(StorageCleanupVisibilityResponse { summary, items })
    }

    async fn load_pool_health_rows(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<Vec<PoolHealthRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                sp.id,
                sp.name,
                sp.pool_kind,
                sp.status,
                COUNT(bp.id) FILTER (WHERE bp.placement_status = 'active') AS active_placements,
                COUNT(bp.id) FILTER (WHERE bp.placement_status = 'retiring') AS retiring_placements,
                COUNT(bp.id) FILTER (WHERE bp.placement_status = 'failed') AS failed_placements,
                COUNT(bp.id) FILTER (WHERE bp.placement_status = 'cleanup_failed') AS cleanup_failed_placements
            FROM storage_pools sp
            LEFT JOIN blob_placements bp
              ON bp.storage_pool_id = sp.id
             AND ($1::uuid IS NULL OR bp.tenant_id = $1)
            GROUP BY sp.id, sp.name, sp.pool_kind, sp.status, sp.created_at
            ORDER BY sp.created_at ASC, sp.name ASC
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PoolHealthRow {
                    id: row.try_get("id")?,
                    name: row.try_get("name")?,
                    pool_kind: row.try_get("pool_kind")?,
                    status: row.try_get("status")?,
                    active_placements: row.try_get::<i64, _>("active_placements")?.max(0) as u64,
                    retiring_placements: row.try_get::<i64, _>("retiring_placements")?.max(0)
                        as u64,
                    failed_placements: row.try_get::<i64, _>("failed_placements")?.max(0) as u64,
                    cleanup_failed_placements: row
                        .try_get::<i64, _>("cleanup_failed_placements")?
                        .max(0) as u64,
                })
            })
            .collect()
    }

    async fn load_placement_counts(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StoragePlacementCounts> {
        let mut counts = StoragePlacementCounts::default();
        let rows = sqlx::query(
            r#"
            SELECT placement_status, COUNT(*) AS count
            FROM blob_placements
            WHERE $1::uuid IS NULL OR tenant_id = $1
            GROUP BY placement_status
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let count = row.try_get::<i64, _>("count")?.max(0) as u64;
            match row.try_get::<String, _>("placement_status")?.as_str() {
                "active" => counts.active = count,
                "copying" => counts.copying = count,
                "verified" => counts.verified = count,
                "retiring" => counts.retiring = count,
                "failed" => counts.failed = count,
                "cleaning" => counts.cleaning = count,
                "cleanup_failed" => counts.cleanup_failed = count,
                "deleted" => counts.deleted = count,
                _ => {}
            }
        }

        counts.missing_active = self.count_missing_active_placements(tenant_filter).await?;
        counts.degraded = counts.failed + counts.cleanup_failed + counts.missing_active;
        Ok(counts)
    }

    async fn count_missing_active_placements(&self, tenant_filter: Option<Uuid>) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM blobs b
            WHERE b.blob_kind IN ('attachment', 'mime_part')
              AND ($1::uuid IS NULL OR b.tenant_id = $1)
              AND NOT EXISTS (
                  SELECT 1
                  FROM blob_placements bp
                  JOIN storage_pools sp
                    ON sp.id = bp.storage_pool_id
                   AND sp.pool_kind = 'postgres'
                   AND sp.status = 'active'
                  WHERE bp.tenant_id = b.tenant_id
                    AND bp.domain_id = b.domain_id
                    AND bp.blob_id = b.id
                    AND bp.blob_kind = b.blob_kind
                    AND bp.placement_status = 'active'
              )
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?;
        Ok(count.max(0) as u64)
    }

    async fn load_migration_counts(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StorageMigrationCounts> {
        let mut counts = StorageMigrationCounts::default();
        let rows = sqlx::query(
            r#"
            SELECT status, COUNT(*) AS count
            FROM blob_migration_jobs
            WHERE $1::uuid IS NULL OR tenant_id = $1
            GROUP BY status
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let count = row.try_get::<i64, _>("count")?.max(0) as u64;
            match row.try_get::<String, _>("status")?.as_str() {
                "pending" => counts.pending = count,
                "running" => counts.running = count,
                "verified" => counts.verified = count,
                "switched" => counts.switched = count,
                "failed" => counts.failed = count,
                "cancelled" => counts.cancelled = count,
                _ => {}
            }
        }

        counts.expired_leases = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM blob_migration_jobs
            WHERE status = 'running'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= NOW()
              AND ($1::uuid IS NULL OR tenant_id = $1)
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?
        .max(0) as u64;
        Ok(counts)
    }

    async fn load_cleanup_counts(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<StorageCleanupCounts> {
        let mut counts = StorageCleanupCounts::default();
        let rows = sqlx::query(
            r#"
            SELECT placement_status, COUNT(*) AS count
            FROM blob_placements
            WHERE placement_status IN ('retiring', 'cleaning', 'cleanup_failed', 'deleted')
              AND ($1::uuid IS NULL OR tenant_id = $1)
            GROUP BY placement_status
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let count = row.try_get::<i64, _>("count")?.max(0) as u64;
            match row.try_get::<String, _>("placement_status")?.as_str() {
                "retiring" => counts.retiring = count,
                "cleaning" => counts.cleaning = count,
                "cleanup_failed" => counts.cleanup_failed = count,
                "deleted" => counts.deleted = count,
                _ => {}
            }
        }

        counts.due = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM blob_placements
            WHERE placement_status IN ('retiring', 'cleanup_failed')
              AND (next_cleanup_attempt_at IS NULL OR next_cleanup_attempt_at <= NOW())
              AND ($1::uuid IS NULL OR tenant_id = $1)
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?
        .max(0) as u64;

        counts.blocked_by_rollback = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM blob_placements
            WHERE placement_status IN ('retiring', 'cleanup_failed')
              AND (rollback_until IS NULL OR rollback_until > NOW())
              AND ($1::uuid IS NULL OR tenant_id = $1)
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?
        .max(0) as u64;

        counts.blocked_by_missing_active_replacement = self
            .count_cleanup_missing_active_replacement(tenant_filter)
            .await?;
        counts.blocked_by_retention_or_legal_hold = self
            .count_cleanup_retention_or_legal_hold(tenant_filter)
            .await?;
        Ok(counts)
    }

    async fn count_cleanup_missing_active_replacement(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM blob_placements old
            WHERE old.placement_status IN ('retiring', 'cleanup_failed')
              AND ($1::uuid IS NULL OR old.tenant_id = $1)
              AND NOT EXISTS (
                  SELECT 1
                  FROM blob_placements active
                  JOIN storage_pools sp
                    ON sp.id = active.storage_pool_id
                   AND sp.pool_kind = 'postgres'
                   AND sp.status = 'active'
                  WHERE active.tenant_id = old.tenant_id
                    AND active.domain_id = old.domain_id
                    AND active.blob_id = old.blob_id
                    AND active.blob_kind = old.blob_kind
                    AND active.id <> old.id
                    AND active.placement_status = 'active'
              )
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?;
        Ok(count.max(0) as u64)
    }

    async fn count_cleanup_retention_or_legal_hold(
        &self,
        tenant_filter: Option<Uuid>,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(DISTINCT old.id)
            FROM blob_placements old
            JOIN blobs b
              ON b.tenant_id = old.tenant_id
             AND b.domain_id = old.domain_id
             AND b.id = old.blob_id
             AND b.blob_kind = old.blob_kind
            WHERE old.placement_status IN ('retiring', 'cleanup_failed')
              AND ($1::uuid IS NULL OR old.tenant_id = $1)
              AND (
                  (b.retained_until IS NOT NULL AND b.retained_until > NOW())
                  OR b.legal_hold = TRUE
                  OR EXISTS (
                      SELECT 1
                      FROM messages m
                      WHERE m.tenant_id = old.tenant_id
                        AND m.domain_id = old.domain_id
                        AND ((m.retained_until IS NOT NULL AND m.retained_until > NOW()) OR m.legal_hold = TRUE)
                        AND (
                            m.blob_id = old.blob_id
                            OR EXISTS (
                                SELECT 1 FROM mime_parts mp
                                WHERE mp.tenant_id = m.tenant_id
                                  AND mp.message_id = m.id
                                  AND mp.blob_id = old.blob_id
                            )
                            OR EXISTS (
                                SELECT 1 FROM attachments a
                                WHERE a.tenant_id = m.tenant_id
                                  AND a.message_id = m.id
                                  AND a.blob_id = old.blob_id
                            )
                        )
                  )
              )
            "#,
        )
        .bind(tenant_filter)
        .fetch_one(&self.pool)
        .await?;
        Ok(count.max(0) as u64)
    }

    async fn load_cleanup_rows(&self, tenant_filter: Option<Uuid>) -> Result<Vec<CleanupRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                bp.id,
                bp.tenant_id,
                bp.domain_id,
                bp.blob_kind,
                sp.id AS pool_id,
                sp.name AS pool_name,
                sp.pool_kind AS pool_kind,
                sp.status AS pool_status,
                bp.placement_status,
                bp.cleanup_attempts,
                CASE WHEN bp.rollback_until IS NULL THEN NULL ELSE to_char(bp.rollback_until AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS rollback_until,
                CASE WHEN bp.next_cleanup_attempt_at IS NULL THEN NULL ELSE to_char(bp.next_cleanup_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS next_cleanup_attempt_at,
                CASE WHEN bp.cleaned_at IS NULL THEN NULL ELSE to_char(bp.cleaned_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS cleaned_at,
                bp.cleanup_error
            FROM blob_placements bp
            JOIN storage_pools sp ON sp.id = bp.storage_pool_id
            WHERE bp.placement_status IN ('retiring', 'cleaning', 'cleanup_failed', 'deleted')
              AND ($1::uuid IS NULL OR bp.tenant_id = $1)
            ORDER BY bp.updated_at DESC, bp.created_at DESC, bp.id ASC
            LIMIT 100
            "#,
        )
        .bind(tenant_filter)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(CleanupRow {
                    placement_id: row.try_get("id")?,
                    tenant_id: row.try_get("tenant_id")?,
                    domain_id: row.try_get("domain_id")?,
                    blob_kind: row.try_get("blob_kind")?,
                    pool: pool_reference_from_columns(&row, "pool")?,
                    status: row.try_get("placement_status")?,
                    cleanup_attempts: row.try_get::<i32, _>("cleanup_attempts")?.max(0) as u32,
                    rollback_until: row.try_get("rollback_until")?,
                    next_cleanup_attempt_at: row.try_get("next_cleanup_attempt_at")?,
                    cleaned_at: row.try_get("cleaned_at")?,
                    cleanup_error_summary: summarize_error(row.try_get("cleanup_error")?),
                })
            })
            .collect()
    }

    async fn cleanup_blockers_for_row(&self, row: &CleanupRow) -> Result<Vec<String>> {
        if row.status == "deleted" || row.status == "cleaning" {
            return Ok(Vec::new());
        }

        let details = sqlx::query(
            r#"
            SELECT
                bp.placement_status,
                bp.rollback_until IS NULL OR bp.rollback_until > NOW() AS rollback_window_active,
                NOT EXISTS (
                    SELECT 1
                    FROM blob_placements active
                    JOIN storage_pools sp
                      ON sp.id = active.storage_pool_id
                     AND sp.pool_kind = 'postgres'
                     AND sp.status = 'active'
                    WHERE active.tenant_id = bp.tenant_id
                      AND active.domain_id = bp.domain_id
                      AND active.blob_id = bp.blob_id
                      AND active.blob_kind = bp.blob_kind
                      AND active.id <> bp.id
                      AND active.placement_status = 'active'
                ) AS active_replacement_missing,
                b.retained_until IS NOT NULL AND b.retained_until > NOW() AS blob_retention_active,
                b.legal_hold AS blob_legal_hold_active,
                EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE m.tenant_id = bp.tenant_id
                      AND m.domain_id = bp.domain_id
                      AND m.retained_until IS NOT NULL
                      AND m.retained_until > NOW()
                      AND (
                          m.blob_id = bp.blob_id
                          OR EXISTS (
                              SELECT 1 FROM mime_parts mp
                              WHERE mp.tenant_id = m.tenant_id
                                AND mp.message_id = m.id
                                AND mp.blob_id = bp.blob_id
                          )
                          OR EXISTS (
                              SELECT 1 FROM attachments a
                              WHERE a.tenant_id = m.tenant_id
                                AND a.message_id = m.id
                                AND a.blob_id = bp.blob_id
                          )
                      )
                ) AS message_retention_active,
                EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE m.tenant_id = bp.tenant_id
                      AND m.domain_id = bp.domain_id
                      AND m.legal_hold = TRUE
                      AND (
                          m.blob_id = bp.blob_id
                          OR EXISTS (
                              SELECT 1 FROM mime_parts mp
                              WHERE mp.tenant_id = m.tenant_id
                                AND mp.message_id = m.id
                                AND mp.blob_id = bp.blob_id
                          )
                          OR EXISTS (
                              SELECT 1 FROM attachments a
                              WHERE a.tenant_id = m.tenant_id
                                AND a.message_id = m.id
                                AND a.blob_id = bp.blob_id
                          )
                      )
                ) AS message_legal_hold_active
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
        .bind(row.placement_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("placement not found"))?;

        Ok(cleanup_blocker_labels(CleanupBlockerState {
            placement_status: details.try_get("placement_status")?,
            rollback_window_active: details.try_get("rollback_window_active")?,
            active_replacement_missing: details.try_get("active_replacement_missing")?,
            blob_retention_active: details.try_get("blob_retention_active")?,
            blob_legal_hold_active: details.try_get("blob_legal_hold_active")?,
            message_retention_active: details.try_get("message_retention_active")?,
            message_legal_hold_active: details.try_get("message_legal_hold_active")?,
        }))
    }

    async fn ensure_visibility_tenant_exists(&self, tenant_id: Uuid) -> Result<()> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM tenants
                WHERE id = $1
            )
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await?;
        if exists {
            Ok(())
        } else {
            Err(anyhow!("tenant not found"))
        }
    }
}

fn pool_health_summary(row: PoolHealthRow) -> StoragePoolHealth {
    let health = if row.status == "disabled" {
        "disabled"
    } else if row.failed_placements > 0 || row.cleanup_failed_placements > 0 {
        "degraded"
    } else {
        "ok"
    };
    StoragePoolHealth {
        pool: StoragePoolReference {
            id: row.id,
            name: row.name,
            pool_kind: row.pool_kind,
            status: row.status,
        },
        health: health.to_string(),
        active_placements: row.active_placements,
        retiring_placements: row.retiring_placements,
        failed_placements: row.failed_placements,
        cleanup_failed_placements: row.cleanup_failed_placements,
    }
}

fn pool_reference_from_columns(
    row: &sqlx::postgres::PgRow,
    prefix: &str,
) -> Result<StoragePoolReference> {
    Ok(StoragePoolReference {
        id: row.try_get(format!("{prefix}_pool_id").as_str())?,
        name: row.try_get(format!("{prefix}_pool_name").as_str())?,
        pool_kind: row.try_get(format!("{prefix}_pool_kind").as_str())?,
        status: row.try_get(format!("{prefix}_pool_status").as_str())?,
    })
}

fn summarize_error(error: Option<String>) -> Option<String> {
    error
        .map(|error| {
            let trimmed = error.trim();
            if trimmed.len() <= 240 {
                trimmed.to_string()
            } else {
                format!("{}...", &trimmed[..240])
            }
        })
        .filter(|error| !error.is_empty())
}

struct CleanupBlockerState {
    placement_status: String,
    rollback_window_active: bool,
    active_replacement_missing: bool,
    blob_retention_active: bool,
    blob_legal_hold_active: bool,
    message_retention_active: bool,
    message_legal_hold_active: bool,
}

fn cleanup_blocker_labels(state: CleanupBlockerState) -> Vec<String> {
    let mut blockers = Vec::new();
    if !matches!(
        state.placement_status.as_str(),
        "retiring" | "cleanup_failed"
    ) {
        blockers.push("placement_not_retiring".to_string());
    }
    if state.rollback_window_active {
        blockers.push("rollback_window_active".to_string());
    }
    if state.active_replacement_missing {
        blockers.push("active_replacement_missing".to_string());
    }
    if state.blob_retention_active {
        blockers.push("blob_retention_active".to_string());
    }
    if state.blob_legal_hold_active {
        blockers.push("blob_legal_hold_active".to_string());
    }
    if state.message_retention_active {
        blockers.push("message_retention_active".to_string());
    }
    if state.message_legal_hold_active {
        blockers.push("message_legal_hold_active".to_string());
    }
    blockers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Storage;
    use sqlx::postgres::PgPoolOptions;

    const SCHEMA: &str = include_str!("../sql/schema.sql");
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

    #[test]
    fn pool_health_marks_failed_placements_degraded() {
        let health = pool_health_summary(PoolHealthRow {
            id: PRIMARY_POOL_ID,
            name: "postgres-primary".to_string(),
            pool_kind: "postgres".to_string(),
            status: "active".to_string(),
            active_placements: 8,
            retiring_placements: 1,
            failed_placements: 1,
            cleanup_failed_placements: 0,
        });

        assert_eq!(health.health, "degraded");
        assert_eq!(health.pool.name, "postgres-primary");
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

    #[test]
    fn long_errors_are_summarized() {
        let summary = summarize_error(Some("x".repeat(300))).expect("summary");
        assert!(summary.len() < 250);
        assert!(summary.ends_with("..."));
    }
}
