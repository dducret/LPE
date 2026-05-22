use anyhow::{bail, Result};
use serde_json::json;
use sqlx::{FromRow, Row};
use uuid::Uuid;

use crate::{AuditEntryInput, CanonicalChangeCategory, JmapEmailFollowupUpdate, Storage};

pub const CONVERSATION_ACTION_VERSION: i32 = 0x003C_CCCC;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationAction {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub subject: String,
    pub categories_json: String,
    pub move_folder_entry_id: Option<Vec<u8>>,
    pub move_store_entry_id: Option<Vec<u8>>,
    pub move_target_mailbox_id: Option<Uuid>,
    pub max_delivery_time: Option<String>,
    pub last_applied_time: Option<String>,
    pub version: i32,
    pub processed: i32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertConversationActionInput {
    pub account_id: Uuid,
    pub conversation_id: Uuid,
    pub subject: String,
    pub categories_json: String,
    pub move_folder_entry_id: Option<Vec<u8>>,
    pub move_store_entry_id: Option<Vec<u8>>,
    pub move_target_mailbox_id: Option<Uuid>,
    pub max_delivery_time: Option<String>,
    pub last_applied_time: Option<String>,
    pub version: Option<i32>,
    pub processed: Option<i32>,
}

#[derive(Debug, FromRow)]
struct ConversationActionRow {
    id: Uuid,
    conversation_id: Uuid,
    subject: String,
    categories_json: String,
    move_folder_entry_id: Option<Vec<u8>>,
    move_store_entry_id: Option<Vec<u8>>,
    move_target_mailbox_id: Option<Uuid>,
    max_delivery_time: Option<String>,
    last_applied_time: Option<String>,
    version: i32,
    processed: i32,
    created_at: String,
    updated_at: String,
}

impl Storage {
    pub async fn fetch_conversation_actions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ConversationAction>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ConversationActionRow>(
            r#"
            SELECT
                id,
                conversation_id,
                subject,
                categories_json::text AS categories_json,
                move_folder_entry_id,
                move_store_entry_id,
                move_target_mailbox_id,
                to_char(max_delivery_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS max_delivery_time,
                to_char(last_applied_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_applied_time,
                version,
                processed,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM conversation_actions
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY updated_at DESC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_conversation_action).collect())
    }

    pub async fn fetch_conversation_actions_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ConversationAction>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ConversationActionRow>(
            r#"
            SELECT
                id,
                conversation_id,
                subject,
                categories_json::text AS categories_json,
                move_folder_entry_id,
                move_store_entry_id,
                move_target_mailbox_id,
                to_char(max_delivery_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS max_delivery_time,
                to_char(last_applied_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_applied_time,
                version,
                processed,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM conversation_actions
            WHERE tenant_id = $1 AND account_id = $2 AND id = ANY($3)
            ORDER BY updated_at DESC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_conversation_action).collect())
    }

    pub async fn upsert_conversation_action(
        &self,
        input: UpsertConversationActionInput,
    ) -> Result<ConversationAction> {
        if serde_json::from_str::<Vec<String>>(&input.categories_json).is_err() {
            bail!("conversation action categories must be a JSON string array");
        }
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let existed = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM conversation_actions
            WHERE tenant_id = $1 AND account_id = $2 AND conversation_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.conversation_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        let row = sqlx::query_as::<_, ConversationActionRow>(
            r#"
            INSERT INTO conversation_actions (
                id, tenant_id, account_id, conversation_id, subject, categories_json,
                move_folder_entry_id, move_store_entry_id, move_target_mailbox_id, max_delivery_time,
                last_applied_time, version, processed
            )
            VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7, $8,
                $9, $10::timestamptz, $11::timestamptz, $12, $13
            )
            ON CONFLICT (tenant_id, account_id, conversation_id) DO UPDATE SET
                subject = EXCLUDED.subject,
                categories_json = EXCLUDED.categories_json,
                move_folder_entry_id = EXCLUDED.move_folder_entry_id,
                move_store_entry_id = EXCLUDED.move_store_entry_id,
                move_target_mailbox_id = EXCLUDED.move_target_mailbox_id,
                max_delivery_time = EXCLUDED.max_delivery_time,
                last_applied_time = EXCLUDED.last_applied_time,
                version = EXCLUDED.version,
                processed = EXCLUDED.processed,
                updated_at = NOW()
            RETURNING
                id,
                conversation_id,
                subject,
                categories_json::text AS categories_json,
                move_folder_entry_id,
                move_store_entry_id,
                move_target_mailbox_id,
                to_char(max_delivery_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS max_delivery_time,
                to_char(last_applied_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_applied_time,
                version,
                processed,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(input.conversation_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.conversation_id)
        .bind(input.subject.trim())
        .bind(input.categories_json.trim())
        .bind(input.move_folder_entry_id)
        .bind(input.move_store_entry_id)
        .bind(input.move_target_mailbox_id)
        .bind(input.max_delivery_time)
        .bind(input.last_applied_time)
        .bind(input.version.unwrap_or(CONVERSATION_ACTION_VERSION))
        .bind(input.processed.unwrap_or_default())
        .fetch_one(&mut *tx)
        .await?;
        let action = map_conversation_action(row);
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::ConversationActions.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "conversation_action",
            action.id,
            if existed { "updated" } else { "created" },
            modseq,
            &[input.account_id],
            json!({ "conversationId": action.conversation_id }),
        )
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::ConversationActions,
            &[input.account_id],
            &[input.account_id],
        )
        .await?;
        tx.commit().await?;

        Ok(action)
    }

    pub async fn delete_conversation_action(
        &self,
        account_id: Uuid,
        conversation_action_id: Uuid,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM conversation_actions
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            RETURNING conversation_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(conversation_action_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = deleted else {
            bail!("conversation action not found");
        };
        let conversation_id: Uuid = row.try_get("conversation_id")?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                CanonicalChangeCategory::ConversationActions.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "conversation_action",
            conversation_action_id,
            "destroyed",
            modseq,
            &[account_id],
            json!({ "conversationId": conversation_id }),
        )
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::ConversationActions,
            &[account_id],
            &[account_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn apply_conversation_actions_to_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        actor: &str,
    ) -> Result<()> {
        let Some(email) = self
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
        else {
            bail!("message not found");
        };
        let actions = self.fetch_conversation_actions(account_id).await?;
        for action in actions
            .iter()
            .filter(|action| action.conversation_id == email.thread_id)
            .filter(|action| {
                action
                    .max_delivery_time
                    .as_deref()
                    .map(|max_delivery| email.received_at.as_str() > max_delivery)
                    .unwrap_or(true)
            })
        {
            let categories = serde_json::from_str::<Vec<String>>(&action.categories_json)
                .unwrap_or_default()
                .into_iter()
                .map(|category| category.trim().to_string())
                .filter(|category| !category.is_empty())
                .collect::<Vec<_>>();
            if !categories.is_empty() && email.categories != categories {
                self.update_jmap_email_followup_flags(
                    account_id,
                    email.id,
                    JmapEmailFollowupUpdate {
                        categories: Some(categories),
                        ..Default::default()
                    },
                    AuditEntryInput {
                        actor: actor.to_string(),
                        action: "conversation-action-categorize".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await?;
            }
            let Some(target_mailbox_id) = action.move_target_mailbox_id else {
                continue;
            };
            if email.mailbox_id == target_mailbox_id {
                continue;
            }
            self.move_jmap_email_from_mailbox(
                account_id,
                email.mailbox_id,
                email.id,
                target_mailbox_id,
                AuditEntryInput {
                    actor: actor.to_string(),
                    action: "conversation-action-move".to_string(),
                    subject: format!("message:{}->{}", email.id, target_mailbox_id),
                },
            )
            .await?;
        }
        Ok(())
    }
}

fn map_conversation_action(row: ConversationActionRow) -> ConversationAction {
    ConversationAction {
        id: row.id,
        conversation_id: row.conversation_id,
        subject: row.subject,
        categories_json: row.categories_json,
        move_folder_entry_id: row.move_folder_entry_id,
        move_store_entry_id: row.move_store_entry_id,
        move_target_mailbox_id: row.move_target_mailbox_id,
        max_delivery_time: row.max_delivery_time,
        last_applied_time: row.last_applied_time,
        version: row.version,
        processed: row.processed,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}
