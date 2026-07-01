use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::Storage;

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapThreadQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapStoredQueryState {
    pub id: Uuid,
    pub account_id: Uuid,
    pub method_name: String,
    pub filter_hash: String,
    pub sort_hash: String,
    pub last_change_sequence: i64,
    pub snapshot_ids: Vec<String>,
}

impl Storage {
    pub async fn save_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
        last_change_sequence: i64,
        snapshot_ids: &[String],
    ) -> Result<Uuid> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let state_id = Uuid::new_v4();
        let snapshot_json = serde_json::to_value(snapshot_ids)?;
        sqlx::query(
            r#"
            INSERT INTO jmap_query_states (
                id, tenant_id, account_id, method_name, filter_hash, sort_hash,
                last_change_sequence, snapshot_ids, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + INTERVAL '1 hour')
            "#,
        )
        .bind(state_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(method_name)
        .bind(jmap_query_hash(filter.as_ref())?)
        .bind(jmap_query_hash(sort.as_ref())?)
        .bind(last_change_sequence.max(0))
        .bind(snapshot_json)
        .execute(&self.pool)
        .await?;

        Ok(state_id)
    }

    pub async fn fetch_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        state_id: Uuid,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
    ) -> Result<Option<JmapStoredQueryState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let Some(row) = sqlx::query(
            r#"
            SELECT id, account_id, method_name, filter_hash, sort_hash,
                   last_change_sequence, snapshot_ids
            FROM jmap_query_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND method_name = $3
              AND id = $4
              AND filter_hash = $5
              AND sort_hash = $6
              AND expires_at > NOW()
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(method_name)
        .bind(state_id)
        .bind(jmap_query_hash(filter.as_ref())?)
        .bind(jmap_query_hash(sort.as_ref())?)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        let snapshot_json: Value = row.try_get("snapshot_ids")?;
        Ok(Some(JmapStoredQueryState {
            id: row.try_get("id")?,
            account_id: row.try_get("account_id")?,
            method_name: row.try_get("method_name")?,
            filter_hash: row.try_get("filter_hash")?,
            sort_hash: row.try_get("sort_hash")?,
            last_change_sequence: row.try_get("last_change_sequence")?,
            snapshot_ids: serde_json::from_value(snapshot_json)?,
        }))
    }

    pub async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_messages AS (
                SELECT
                    s.message_id AS id,
                    MAX(mm.received_at) AS latest_received_at
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY s.message_id
            )
            SELECT id
            FROM matched_messages
            ORDER BY latest_received_at DESC, id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT s.message_id
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY s.message_id
            ) matched_messages
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapEmailQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT m.id
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.visibility = 'visible'
            ORDER BY m.id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(Into::into))
            .collect()
    }

    pub async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT thread_id
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
            WHERE m.tenant_id = $1
              AND mm.account_id = $2
              AND mm.visibility = 'visible'
            ORDER BY thread_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("thread_id").map_err(Into::into))
            .collect()
    }

    pub async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_threads AS (
                SELECT
                    m.thread_id,
                    MAX(mm.received_at) AS latest_received_at
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                JOIN messages m ON m.tenant_id = s.tenant_id AND m.id = s.message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            )
            SELECT thread_id
            FROM matched_threads
            ORDER BY latest_received_at DESC, thread_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("thread_id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT m.thread_id
                FROM mail_search_documents s
                JOIN mailbox_messages mm
                  ON mm.tenant_id = s.tenant_id
                 AND mm.account_id = s.account_id
                 AND mm.id = s.mailbox_message_id
                JOIN messages m ON m.tenant_id = s.tenant_id AND m.id = s.message_id
                WHERE s.account_id = $1
                  AND mm.visibility = 'visible'
                  AND ($2::uuid IS NULL OR mm.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR s.search_vector @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            ) matched_threads
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapThreadQuery {
            ids,
            total: total.max(0) as u64,
        })
    }
}

fn jmap_query_hash<T: Serialize>(value: Option<&T>) -> Result<String> {
    let bytes = serde_json::to_vec(&value)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}
