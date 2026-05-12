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
