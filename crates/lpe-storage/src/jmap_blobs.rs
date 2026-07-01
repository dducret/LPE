use anyhow::{anyhow, Result};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use crate::{AccountQuotaRow, JmapUploadBlobRow, Storage};

#[derive(Debug, Clone, Serialize)]
pub struct JmapQuota {
    pub id: String,
    pub name: String,
    pub used: u64,
    pub hard_limit: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapUploadBlob {
    pub id: Uuid,
    pub account_id: Uuid,
    pub media_type: String,
    pub octet_size: u64,
    pub blob_bytes: Vec<u8>,
}

impl Storage {
    pub async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, AccountQuotaRow>(
            r#"
            SELECT
                a.quota_mb,
                COALESCE((
                    SELECT SUM(logical_messages.size_octets)::BIGINT
                    FROM (
                        SELECT DISTINCT m.id, m.size_octets
                        FROM mailbox_messages mm
                        JOIN messages m
                          ON m.tenant_id = mm.tenant_id
                         AND m.id = mm.message_id
                        WHERE mm.tenant_id = a.tenant_id
                          AND mm.account_id = a.id
                          AND mm.visibility <> 'expunged'
                    ) logical_messages
                ), 0)::BIGINT AS quota_used_octets
            FROM accounts a
            WHERE a.tenant_id = $1 AND a.id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(JmapQuota {
            id: "mail".to_string(),
            name: "Mail".to_string(),
            used: row.quota_used_octets.max(0) as u64,
            hard_limit: (row.quota_mb.max(0) as u64) * 1024 * 1024,
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn fetch_mailbox_logical_quota_used_octets(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<u64> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let used = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE(SUM(m.size_octets), 0)::BIGINT
            FROM mailbox_messages mm
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.mailbox_id = $3
              AND mm.visibility <> 'expunged'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(used.max(0) as u64)
    }

    #[allow(dead_code)]
    pub(crate) async fn fetch_domain_logical_quota_used_octets(
        &self,
        tenant_id: &Uuid,
        domain_id: Uuid,
    ) -> Result<u64> {
        let used = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COALESCE((
                SELECT SUM(logical_messages.size_octets)::BIGINT
                FROM (
                    SELECT DISTINCT m.id, m.size_octets
                    FROM messages m
                    JOIN mailbox_messages mm
                      ON mm.tenant_id = m.tenant_id
                     AND mm.message_id = m.id
                     AND mm.visibility <> 'expunged'
                    WHERE m.tenant_id = $1
                      AND m.domain_id = $2
                ) logical_messages
            ), 0)::BIGINT
            "#,
        )
        .bind(tenant_id)
        .bind(domain_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(used.max(0) as u64)
    }

    pub async fn fetch_jmap_message_blob(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT b.media_type, b.size_octets, b.blob_bytes
            FROM messages m
            JOIN blobs b
              ON b.tenant_id = m.tenant_id
             AND b.domain_id = m.domain_id
             AND b.id = m.blob_id
             AND b.blob_kind = 'raw_message'
            WHERE m.tenant_id = $1
              AND m.id = $2
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = m.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = m.id
                    AND mm.visibility = 'visible'
              )
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let raw_bytes: Vec<u8> = row.try_get("blob_bytes").unwrap_or_default();
            let blob_bytes = strip_protected_bcc_headers(&raw_bytes);
            JmapUploadBlob {
                id: message_id,
                account_id,
                media_type: row
                    .try_get("media_type")
                    .unwrap_or_else(|_| "message/rfc822".to_string()),
                octet_size: blob_bytes.len() as u64,
                blob_bytes,
            }
        }))
    }

    pub async fn fetch_jmap_message_blob_with_protected_headers(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT b.media_type, b.size_octets, b.blob_bytes
            FROM messages m
            JOIN blobs b
              ON b.tenant_id = m.tenant_id
             AND b.domain_id = m.domain_id
             AND b.id = m.blob_id
             AND b.blob_kind = 'raw_message'
            WHERE m.tenant_id = $1
              AND m.id = $2
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = m.tenant_id
                    AND mm.account_id = $3
                    AND mm.message_id = m.id
                    AND mm.visibility = 'visible'
              )
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: message_id,
            account_id,
            media_type: row
                .try_get("media_type")
                .unwrap_or_else(|_| "message/rfc822".to_string()),
            octet_size: row
                .try_get::<i64, _>("size_octets")
                .unwrap_or_default()
                .max(0) as u64,
            blob_bytes: row.try_get("blob_bytes").unwrap_or_default(),
        }))
    }

    pub async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO jmap_upload_blobs (
                id, tenant_id, account_id, media_type, size_octets,
                content_sha256, blob_bytes, magika_status, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'valid', NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(media_type.trim())
        .bind(blob_bytes.len() as i64)
        .bind(crate::sha256_hex(blob_bytes))
        .bind(blob_bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(JmapUploadBlob {
            id,
            account_id,
            media_type: media_type.trim().to_string(),
            octet_size: blob_bytes.len() as u64,
            blob_bytes: blob_bytes.to_vec(),
        })
    }

    pub async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, JmapUploadBlobRow>(
            r#"
            SELECT id, account_id, media_type, size_octets AS octet_size, blob_bytes
            FROM jmap_upload_blobs
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
              AND consumed_at IS NULL
              AND expires_at > NOW()
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(blob_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: row.id,
            account_id: row.account_id,
            media_type: row.media_type,
            octet_size: row.octet_size.max(0) as u64,
            blob_bytes: row.blob_bytes,
        }))
    }
}

fn strip_protected_bcc_headers(raw: &[u8]) -> Vec<u8> {
    let (header_end, separator_len) =
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            (position, 4)
        } else if let Some(position) = raw.windows(2).position(|window| window == b"\n\n") {
            (position, 2)
        } else {
            return raw.to_vec();
        };

    let header = &raw[..header_end];
    let mut stripped = Vec::with_capacity(raw.len());
    let mut offset = 0;
    let mut skipping_bcc = false;
    while offset < header.len() {
        let relative_line_end = header[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| offset + position + 1)
            .unwrap_or(header.len());
        let line = &header[offset..relative_line_end];
        let line_without_eol = line
            .strip_suffix(b"\r\n")
            .or_else(|| line.strip_suffix(b"\n"))
            .unwrap_or(line);
        let is_continuation = matches!(line_without_eol.first(), Some(b' ' | b'\t'));

        if !is_continuation {
            skipping_bcc = header_name_is_bcc(line_without_eol);
        }
        if !skipping_bcc {
            stripped.extend_from_slice(line);
        }
        offset = relative_line_end;
    }

    stripped.extend_from_slice(&raw[header_end..header_end + separator_len]);
    stripped.extend_from_slice(&raw[header_end + separator_len..]);
    stripped
}

fn header_name_is_bcc(line: &[u8]) -> bool {
    let Some(colon) = line.iter().position(|byte| *byte == b':') else {
        return false;
    };
    line[..colon].trim_ascii().eq_ignore_ascii_case(b"bcc")
}

#[cfg(test)]
mod tests {
    use super::strip_protected_bcc_headers;

    #[test]
    fn protected_bcc_headers_are_stripped_from_default_message_blobs() {
        let raw = b"From: a@example.test\r\nBcc: hidden@example.test\r\n\tfolded@example.test\r\nTo: b@example.test\r\n\r\nbody";
        let stripped = strip_protected_bcc_headers(raw);
        let rendered = String::from_utf8(stripped).unwrap();

        assert!(!rendered.contains("Bcc:"));
        assert!(!rendered.contains("hidden@example.test"));
        assert!(!rendered.contains("folded@example.test"));
        assert!(rendered.contains("From: a@example.test"));
        assert!(rendered.contains("To: b@example.test"));
        assert!(rendered.ends_with("\r\n\r\nbody"));
    }
}
