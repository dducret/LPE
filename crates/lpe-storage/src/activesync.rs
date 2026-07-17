use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    ActiveSyncDeviceRow, ActiveSyncSyncStateRow, Storage,
};

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncDeviceState {
    pub account_id: Uuid,
    pub device_id: String,
    pub device_type: String,
    pub policy_key: Option<String>,
    pub pending_policy_key: Option<String>,
    pub provision_status: String,
    pub wipe_status: String,
    pub account_wipe_status: String,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncSyncState {
    pub sync_key: String,
    pub snapshot_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncItemState {
    pub id: Uuid,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachment {
    pub id: Uuid,
    pub message_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub disposition: Option<String>,
    pub content_id: Option<String>,
    pub size_octets: u64,
    pub file_reference: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachmentContent {
    pub file_reference: String,
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

impl Storage {
    pub async fn store_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
        snapshot_json: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = activesync_collection_kind(collection_id);
        let state_json = serde_json::from_str::<Value>(snapshot_json.trim())?;
        let last_change_sequence = self
            .fetch_canonical_change_cursor(account_id)
            .await?
            .unwrap_or(0);
        sqlx::query(
            r#"
            INSERT INTO activesync_sync_cursors (
                id, tenant_id, account_id, device_id, collection_kind, collection_key,
                sync_key, last_change_sequence, state_json, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() + INTERVAL '30 days')
            ON CONFLICT (tenant_id, account_id, device_id, collection_kind, collection_key)
            DO UPDATE SET
                sync_key = EXCLUDED.sync_key,
                last_change_sequence = EXCLUDED.last_change_sequence,
                state_json = EXCLUDED.state_json,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .bind(last_change_sequence)
        .bind(state_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_device(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<Option<ActiveSyncDeviceState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, ActiveSyncDeviceRow>(
            r#"
            SELECT account_id, device_id, device_type, policy_key, pending_policy_key,
                   provision_status, wipe_status, account_wipe_status, last_seen_at::text AS last_seen_at
            FROM activesync_devices
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(active_sync_device_state_from_row))
    }

    pub async fn store_activesync_device_pending_policy(
        &self,
        account_id: Uuid,
        device_id: &str,
        device_type: &str,
        pending_policy_key: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let device_type = normalized_device_type(device_type);
        sqlx::query(
            r#"
            INSERT INTO activesync_devices (
                id, tenant_id, account_id, device_id, device_type, pending_policy_key,
                provision_status, last_seen_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
            ON CONFLICT (tenant_id, account_id, device_id)
            DO UPDATE SET
                device_type = EXCLUDED.device_type,
                pending_policy_key = EXCLUDED.pending_policy_key,
                provision_status = 'pending',
                last_seen_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(device_type)
        .bind(pending_policy_key.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn acknowledge_activesync_device_policy(
        &self,
        account_id: Uuid,
        device_id: &str,
        device_type: &str,
        policy_key: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let device_type = normalized_device_type(device_type);
        sqlx::query(
            r#"
            INSERT INTO activesync_devices (
                id, tenant_id, account_id, device_id, device_type, policy_key,
                provision_status, last_seen_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'active', NOW())
            ON CONFLICT (tenant_id, account_id, device_id)
            DO UPDATE SET
                device_type = EXCLUDED.device_type,
                policy_key = EXCLUDED.policy_key,
                pending_policy_key = NULL,
                provision_status = 'active',
                last_seen_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(device_type)
        .bind(policy_key.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn touch_activesync_device(&self, account_id: Uuid, device_id: &str) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            UPDATE activesync_devices
            SET last_seen_at = NOW(), updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn cleanup_expired_activesync_sync_cursors(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            DELETE FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND expires_at <= NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = activesync_collection_kind(collection_id);
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, state_json::text AS snapshot_json
            FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_kind = $4
              AND collection_key = $5
              AND sync_key = $6
              AND expires_at > NOW()
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }

    pub async fn fetch_activesync_email_states(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.normalized_subject,
                    COALESCE(left(b.body_text, 160), ''),
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN NOT mm.is_seen THEN '1' ELSE '0' END,
                    CASE WHEN mm.is_flagged THEN '1' ELSE '0' END,
                    COALESCE(fr.display_name, ''),
                    COALESCE(fr.address, ''),
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END)
                ) AS fingerprint
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.mailbox_id = $3
             AND mm.visibility <> 'expunged'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC, m.id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_email_states_by_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.normalized_subject,
                    COALESCE(left(b.body_text, 160), ''),
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN NOT mm.is_seen THEN '1' ELSE '0' END,
                    CASE WHEN mm.is_flagged THEN '1' ELSE '0' END,
                    COALESCE(fr.display_name, ''),
                    COALESCE(fr.address, ''),
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END)
                ) AS fingerprint
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.mailbox_id = $3
             AND mm.visibility <> 'expunged'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.role = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.id = ANY($4)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2
            ORDER BY display_name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND lifecycle_state = 'active'
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
              AND lifecycle_state = 'active'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_message_attachments(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<ActiveSyncAttachment>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT a.id, a.message_id, a.file_name, a.disposition, a.content_id,
                   a.domain_id, a.blob_id, a.size_octets
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.account_id = a.account_id
             AND mm.message_id = a.message_id
             AND mm.visibility = 'visible'
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
            ORDER BY a.file_name ASC, a.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_all(&self.pool)
        .await?;

        let blob_store = PostgresBlobStore;
        let mut attachments = Vec::with_capacity(rows.len());
        for row in rows {
            let attachment_id: Uuid = row.try_get("id")?;
            let message_id: Uuid = row.try_get("message_id")?;
            let domain_id: Uuid = row.try_get("domain_id")?;
            let blob_id: Uuid = row.try_get("blob_id")?;
            let Some(blob) = blob_store
                .stat_durable_blob(
                    &self.pool,
                    &tenant_id,
                    domain_id,
                    DurableBlobKind::Attachment,
                    blob_id,
                )
                .await?
            else {
                continue;
            };
            attachments.push(ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: row.try_get("file_name")?,
                media_type: blob.media_type,
                disposition: row.try_get("disposition")?,
                content_id: row.try_get("content_id")?,
                size_octets: row
                    .try_get::<i64, _>("size_octets")
                    .unwrap_or(blob.size_octets)
                    .max(0) as u64,
                file_reference: format!("attachment:{message_id}:{attachment_id}"),
            });
        }
        Ok(attachments)
    }

    pub async fn fetch_activesync_attachment_content(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let Some((message_id, attachment_id)) =
            crate::parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let row = sqlx::query(
            r#"
            SELECT a.file_name, a.domain_id, a.blob_id
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.account_id = a.account_id
             AND mm.message_id = a.message_id
             AND mm.visibility = 'visible'
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND a.account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(
                &self.pool,
                &tenant_id,
                row.try_get("domain_id")?,
                DurableBlobKind::Attachment,
                row.try_get("blob_id")?,
            )
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(ActiveSyncAttachmentContent {
            file_reference: file_reference.trim().to_string(),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: blob.media_type,
            blob_bytes: blob.bytes,
        }))
    }

    pub async fn fetch_message_attachment_content_by_cid(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        content_id: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let content_id = content_id.trim().trim_matches(['<', '>']);
        if content_id.is_empty() {
            return Ok(None);
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT a.id, a.file_name, a.domain_id, a.blob_id
            FROM attachments a
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
              AND lower(btrim(btrim(a.content_id), '<>')) = lower($4)
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY a.ordinal ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(content_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Some(blob) = PostgresBlobStore
            .read_durable_blob(
                &self.pool,
                &tenant_id,
                row.try_get("domain_id")?,
                DurableBlobKind::Attachment,
                row.try_get("blob_id")?,
            )
            .await?
        else {
            return Ok(None);
        };
        let attachment_id = row.try_get("id").unwrap_or(message_id);
        Ok(Some(ActiveSyncAttachmentContent {
            file_reference: format!("attachment:{message_id}:{attachment_id}"),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: blob.media_type,
            blob_bytes: blob.bytes,
        }))
    }
}

pub(crate) fn activesync_collection_kind(collection_id: &str) -> &'static str {
    match collection_id.trim() {
        "__folders__" => "folders",
        "contacts" => "contacts",
        "calendar" => "calendar",
        "tasks" => "tasks",
        _ => "mail",
    }
}

fn active_sync_device_state_from_row(row: ActiveSyncDeviceRow) -> ActiveSyncDeviceState {
    ActiveSyncDeviceState {
        account_id: row.account_id,
        device_id: row.device_id,
        device_type: row.device_type,
        policy_key: row.policy_key,
        pending_policy_key: row.pending_policy_key,
        provision_status: row.provision_status,
        wipe_status: row.wipe_status,
        account_wipe_status: row.account_wipe_status,
        last_seen_at: row.last_seen_at,
    }
}

fn normalized_device_type(device_type: &str) -> String {
    let trimmed = device_type.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::activesync_collection_kind;

    #[test]
    fn activesync_collection_kind_maps_builtin_and_mail_collections() {
        assert_eq!(activesync_collection_kind("__folders__"), "folders");
        assert_eq!(activesync_collection_kind("contacts"), "contacts");
        assert_eq!(activesync_collection_kind("calendar"), "calendar");
        assert_eq!(
            activesync_collection_kind("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            "mail"
        );
    }
}
