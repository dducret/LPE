use anyhow::{anyhow, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::storage_backend::StorageBackendSelection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DurableBlobKind {
    Attachment,
    MimePart,
}

impl DurableBlobKind {
    pub(super) fn as_str(self) -> &'static str {
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
pub(super) struct WriteStoragePool {
    pub(super) id: Uuid,
    pub(super) backend: StorageBackendSelection,
}

#[derive(Debug)]
pub(super) struct ActiveBlobPlacement {
    pub(super) placement_id: Uuid,
    pub(super) backend: StorageBackendSelection,
    pub(super) id: Uuid,
    pub(super) media_type: String,
    pub(super) size_octets: i64,
    pub(super) content_sha256: String,
    pub(super) blob_bytes: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(super) struct MigrationTargetPlacement {
    pub(super) placement_id: Uuid,
    pub(super) backend: StorageBackendSelection,
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

#[allow(dead_code)]
pub(super) fn normalize_migration_blob_kind(blob_kind: &str) -> Result<&'static str> {
    match blob_kind.trim() {
        "attachment" => Ok("attachment"),
        "mime_part" => Ok("mime_part"),
        _ => Err(anyhow!(
            "blob migration jobs only support durable attachment and MIME-part blobs"
        )),
    }
}

#[allow(dead_code)]
pub(super) fn durable_blob_kind_from_str(blob_kind: &str) -> Result<DurableBlobKind> {
    match normalize_migration_blob_kind(blob_kind)? {
        "attachment" => Ok(DurableBlobKind::Attachment),
        "mime_part" => Ok(DurableBlobKind::MimePart),
        _ => unreachable!("normalize_migration_blob_kind returned unsupported kind"),
    }
}

#[allow(dead_code)]
pub(super) fn blob_migration_job_from_row(row: sqlx::postgres::PgRow) -> Result<BlobMigrationJob> {
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
pub(super) fn is_constraint_error(error: &sqlx::Error, constraint: &str) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.constraint() == Some(constraint)
    )
}
