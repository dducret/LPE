use anyhow::{bail, Result};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::{CanonicalChangeCategory, ClientNoteRow, ClientReminderRow, JournalEntryRow, Storage};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientNote {
    pub id: Uuid,
    pub title: String,
    pub body_text: String,
    pub color: String,
    pub categories_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UpsertClientNoteInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub title: String,
    pub body_text: String,
    pub color: String,
    pub categories_json: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub id: Uuid,
    pub subject: String,
    pub body_text: String,
    pub entry_type: String,
    pub message_class: String,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub occurred_at: Option<String>,
    pub companies_json: String,
    pub contacts_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UpsertJournalEntryInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub subject: String,
    pub body_text: String,
    pub entry_type: String,
    pub message_class: String,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub occurred_at: Option<String>,
    pub companies_json: String,
    pub contacts_json: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientReminder {
    pub source_type: String,
    pub source_id: Uuid,
    pub title: String,
    pub due_at: Option<String>,
    pub reminder_at: String,
    pub dismissed_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Default)]
pub struct ReminderQuery {
    pub include_inactive: bool,
}

fn map_note(row: ClientNoteRow) -> ClientNote {
    ClientNote {
        id: row.id,
        title: row.title,
        body_text: row.body_text,
        color: row.color,
        categories_json: row.categories_json,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_journal_entry(row: JournalEntryRow) -> JournalEntry {
    JournalEntry {
        id: row.id,
        subject: row.subject,
        body_text: row.body_text,
        entry_type: row.entry_type,
        message_class: row.message_class,
        starts_at: row.starts_at,
        ends_at: row.ends_at,
        occurred_at: row.occurred_at,
        companies_json: row.companies_json,
        contacts_json: row.contacts_json,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_reminder(row: ClientReminderRow) -> ClientReminder {
    ClientReminder {
        source_type: row.source_type,
        source_id: row.source_id,
        title: row.title,
        due_at: row.due_at,
        reminder_at: row.reminder_at,
        dismissed_at: row.dismissed_at,
        completed_at: row.completed_at,
        status: row.status,
    }
}

impl Storage {
    pub async fn fetch_client_notes(&self, account_id: Uuid) -> Result<Vec<ClientNote>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientNoteRow>(
            r#"
            SELECT
                id,
                title,
                body_text,
                color,
                categories_json::text AS categories_json,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM notes
            WHERE tenant_id = $1 AND owner_account_id = $2
            ORDER BY updated_at DESC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_note).collect())
    }

    pub async fn fetch_client_notes_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientNote>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientNoteRow>(
            r#"
            SELECT
                id,
                title,
                body_text,
                color,
                categories_json::text AS categories_json,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM notes
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
            ORDER BY updated_at DESC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_note).collect())
    }

    pub async fn upsert_client_note(&self, input: UpsertClientNoteInput) -> Result<ClientNote> {
        let title = input.title.trim();
        let body_text = input.body_text.trim();
        if title.is_empty() && body_text.is_empty() {
            bail!("note title or body is required");
        }
        let note_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let categories_json = if input.categories_json.trim().is_empty() {
            "[]"
        } else {
            input.categories_json.trim()
        };
        let mut tx = self.pool.begin().await?;
        if input.id.is_some() {
            let owned_by_account = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT owner_account_id
                FROM notes
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(note_id)
            .fetch_optional(&mut *tx)
            .await?;
            if owned_by_account.is_some_and(|owner_account_id| owner_account_id != input.account_id)
            {
                bail!("note not found");
            }
        }
        let existed = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM notes
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(note_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        let row = sqlx::query_as::<_, ClientNoteRow>(
            r#"
            INSERT INTO notes (
                id, tenant_id, owner_account_id, title, body_text, color, categories_json
            )
            VALUES ($1, $2, $3, $4, $5, lower($6), $7::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                body_text = EXCLUDED.body_text,
                color = EXCLUDED.color,
                categories_json = EXCLUDED.categories_json,
                updated_at = NOW()
            WHERE notes.tenant_id = EXCLUDED.tenant_id
              AND notes.owner_account_id = EXCLUDED.owner_account_id
            RETURNING
                id,
                title,
                body_text,
                color,
                categories_json::text AS categories_json,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(note_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(title)
        .bind(body_text)
        .bind(input.color.trim())
        .bind(categories_json)
        .fetch_one(&mut *tx)
        .await?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Notes.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "note",
            note_id,
            if existed { "updated" } else { "created" },
            modseq,
            &[input.account_id],
            json!({}),
        )
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Notes,
            &[input.account_id],
            &[input.account_id],
        )
        .await?;
        tx.commit().await?;

        Ok(map_note(row))
    }

    pub async fn delete_client_note(&self, account_id: Uuid, note_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM notes
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(note_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        if !exists {
            bail!("note not found");
        }
        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Notes,
            account_id,
            None,
            "note",
            note_id,
            None,
            &[account_id],
        )
        .await?;
        sqlx::query(
            r#"
            DELETE FROM notes
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(note_id)
        .execute(&mut *tx)
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Notes,
            &[account_id],
            &[account_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_journal_entries(&self, account_id: Uuid) -> Result<Vec<JournalEntry>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let sql = journal_select_sql("WHERE tenant_id = $1 AND owner_account_id = $2");
        let rows = sqlx::query_as::<_, JournalEntryRow>(&sql)
            .bind(&tenant_id)
            .bind(account_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(map_journal_entry).collect())
    }

    pub async fn fetch_journal_entries_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JournalEntry>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let sql =
            journal_select_sql("WHERE tenant_id = $1 AND owner_account_id = $2 AND id = ANY($3)");
        let rows = sqlx::query_as::<_, JournalEntryRow>(&sql)
            .bind(&tenant_id)
            .bind(account_id)
            .bind(ids)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(map_journal_entry).collect())
    }

    pub async fn upsert_journal_entry(
        &self,
        input: UpsertJournalEntryInput,
    ) -> Result<JournalEntry> {
        let subject = input.subject.trim();
        if subject.is_empty() {
            bail!("journal subject is required");
        }
        let entry_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let companies_json = if input.companies_json.trim().is_empty() {
            "[]"
        } else {
            input.companies_json.trim()
        };
        let contacts_json = if input.contacts_json.trim().is_empty() {
            "[]"
        } else {
            input.contacts_json.trim()
        };
        let mut tx = self.pool.begin().await?;
        if input.id.is_some() {
            let owned_by_account = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT owner_account_id
                FROM journal_entries
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(entry_id)
            .fetch_optional(&mut *tx)
            .await?;
            if owned_by_account.is_some_and(|owner_account_id| owner_account_id != input.account_id)
            {
                bail!("journal entry not found");
            }
        }
        let existed = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM journal_entries
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(entry_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        let row = sqlx::query_as::<_, JournalEntryRow>(
            r#"
            INSERT INTO journal_entries (
                id, tenant_id, owner_account_id, subject, body_text, entry_type,
                message_class, starts_at, ends_at, occurred_at, companies_json, contacts_json
            )
            VALUES (
                $1, $2, $3, $4, $5, lower($6), COALESCE(NULLIF($7, ''), 'IPM.Activity'),
                NULLIF($8, '')::timestamptz, NULLIF($9, '')::timestamptz,
                NULLIF($10, '')::timestamptz, $11::jsonb, $12::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                subject = EXCLUDED.subject,
                body_text = EXCLUDED.body_text,
                entry_type = EXCLUDED.entry_type,
                message_class = EXCLUDED.message_class,
                starts_at = EXCLUDED.starts_at,
                ends_at = EXCLUDED.ends_at,
                occurred_at = EXCLUDED.occurred_at,
                companies_json = EXCLUDED.companies_json,
                contacts_json = EXCLUDED.contacts_json,
                updated_at = NOW()
            WHERE journal_entries.tenant_id = EXCLUDED.tenant_id
              AND journal_entries.owner_account_id = EXCLUDED.owner_account_id
            RETURNING
                id,
                subject,
                body_text,
                entry_type,
                message_class,
                CASE WHEN starts_at IS NULL THEN NULL ELSE to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS starts_at,
                CASE WHEN ends_at IS NULL THEN NULL ELSE to_char(ends_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS ends_at,
                CASE WHEN occurred_at IS NULL THEN NULL ELSE to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS occurred_at,
                companies_json::text AS companies_json,
                contacts_json::text AS contacts_json,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(entry_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(subject)
        .bind(input.body_text.trim())
        .bind(input.entry_type.trim())
        .bind(input.message_class.trim())
        .bind(input.starts_at.as_deref().unwrap_or_default().trim())
        .bind(input.ends_at.as_deref().unwrap_or_default().trim())
        .bind(input.occurred_at.as_deref().unwrap_or_default().trim())
        .bind(companies_json)
        .bind(contacts_json)
        .fetch_one(&mut *tx)
        .await?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Journal.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "journal_entry",
            entry_id,
            if existed { "updated" } else { "created" },
            modseq,
            &[input.account_id],
            json!({}),
        )
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Journal,
            &[input.account_id],
            &[input.account_id],
        )
        .await?;
        tx.commit().await?;

        Ok(map_journal_entry(row))
    }

    pub async fn delete_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM journal_entries
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(entry_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        if !exists {
            bail!("journal entry not found");
        }
        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Journal,
            account_id,
            None,
            "journal_entry",
            entry_id,
            None,
            &[account_id],
        )
        .await?;
        sqlx::query(
            r#"
            DELETE FROM journal_entries
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(entry_id)
        .execute(&mut *tx)
        .await?;
        Self::emit_canonical_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Journal,
            &[account_id],
            &[account_id],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn query_client_reminders(
        &self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> Result<Vec<ClientReminder>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientReminderRow>(
            r#"
            SELECT *
            FROM (
                SELECT
                    'calendar'::text AS source_type,
                    id AS source_id,
                    title,
                    to_char(ends_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS due_at,
                    to_char(reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS reminder_at,
                    CASE WHEN reminder_dismissed_at IS NULL THEN NULL ELSE to_char(reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS dismissed_at,
                    NULL::text AS completed_at,
                    CASE
                        WHEN reminder_dismissed_at IS NOT NULL THEN 'dismissed'
                        WHEN status = 'cancelled' THEN 'excluded'
                        WHEN reminder_at <= NOW() THEN 'due'
                        ELSE 'pending'
                    END AS status
                FROM calendar_events
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND reminder_set
                  AND reminder_at IS NOT NULL
                UNION ALL
                SELECT
                    'task'::text AS source_type,
                    id AS source_id,
                    title,
                    CASE WHEN due_at IS NULL THEN NULL ELSE to_char(due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS due_at,
                    to_char(reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS reminder_at,
                    CASE WHEN reminder_dismissed_at IS NULL THEN NULL ELSE to_char(reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS dismissed_at,
                    CASE WHEN completed_at IS NULL THEN NULL ELSE to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS completed_at,
                    CASE
                        WHEN status = 'cancelled' THEN 'excluded'
                        WHEN completed_at IS NOT NULL THEN 'completed'
                        WHEN reminder_dismissed_at IS NOT NULL THEN 'dismissed'
                        WHEN reminder_at <= NOW() THEN 'due'
                        ELSE 'pending'
                    END AS status
                FROM tasks
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND reminder_set
                  AND reminder_at IS NOT NULL
                UNION ALL
                SELECT *
                FROM (
                    SELECT DISTINCT ON (mm.message_id)
                        'mail'::text AS source_type,
                        mm.message_id AS source_id,
                        m.normalized_subject AS title,
                        CASE WHEN mm.followup_due_at IS NULL THEN NULL ELSE to_char(mm.followup_due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS due_at,
                        to_char(mm.reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS reminder_at,
                        CASE WHEN mm.reminder_dismissed_at IS NULL THEN NULL ELSE to_char(mm.reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS dismissed_at,
                        CASE WHEN mm.followup_completed_at IS NULL THEN NULL ELSE to_char(mm.followup_completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS completed_at,
                        CASE
                            WHEN mm.followup_flag_status = 'complete' THEN 'completed'
                            WHEN mm.reminder_dismissed_at IS NOT NULL THEN 'dismissed'
                            WHEN mm.reminder_at <= NOW() THEN 'due'
                            ELSE 'pending'
                        END AS status
                    FROM mailbox_messages mm
                    JOIN messages m
                      ON m.tenant_id = mm.tenant_id
                     AND m.id = mm.message_id
                    JOIN mailboxes mb
                      ON mb.tenant_id = mm.tenant_id
                     AND mb.account_id = mm.account_id
                     AND mb.id = mm.mailbox_id
                    WHERE mm.tenant_id = $1
                      AND mm.account_id = $2
                      AND mm.visibility = 'visible'
                      AND NOT mm.is_deleted
                      AND mm.reminder_set
                      AND mm.reminder_at IS NOT NULL
                      AND mb.role NOT IN ('trash', 'junk', 'drafts', 'outbox', 'sync_issues', 'conflicts', 'local_failures', 'server_failures')
                    ORDER BY mm.message_id, mb.sort_order ASC, mb.display_name ASC, mb.id ASC
                ) mail_reminders
            ) reminders
            WHERE $3 OR status IN ('due', 'pending')
            ORDER BY reminder_at ASC, source_type ASC, source_id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(query.include_inactive)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_reminder).collect())
    }
}

fn journal_select_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            id,
            subject,
            body_text,
            entry_type,
            message_class,
            CASE WHEN starts_at IS NULL THEN NULL ELSE to_char(starts_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS starts_at,
            CASE WHEN ends_at IS NULL THEN NULL ELSE to_char(ends_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS ends_at,
            CASE WHEN occurred_at IS NULL THEN NULL ELSE to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END AS occurred_at,
            companies_json::text AS companies_json,
            contacts_json::text AS contacts_json,
            to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
            to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
        FROM journal_entries
        {where_clause}
        ORDER BY COALESCE(starts_at, occurred_at, updated_at) DESC, id ASC
        "#
    )
}
