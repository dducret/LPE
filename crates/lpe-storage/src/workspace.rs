use anyhow::{bail, Result};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    attachments, normalize_email, CanonicalChangeCategory, ClientAttachment, ClientAttachmentRow,
    ClientContactRow, ClientEventRow, ClientMessageRow, ClientTask, Storage,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientWorkspace {
    pub messages: Vec<ClientMessage>,
    pub events: Vec<ClientEvent>,
    pub contacts: Vec<ClientContact>,
    pub tasks: Vec<ClientTask>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientMessage {
    pub id: Uuid,
    pub folder: String,
    pub from: String,
    pub from_address: String,
    pub to: String,
    pub cc: String,
    pub subject: String,
    pub preview: String,
    pub received_at: String,
    pub time_label: String,
    pub unread: bool,
    pub flagged: bool,
    pub followup_flag_status: String,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub tags: Vec<String>,
    pub attachments: Vec<ClientAttachment>,
    pub body: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientEvent {
    pub id: Uuid,
    pub uid: String,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub all_day: bool,
    pub status: String,
    pub sequence: i32,
    pub recurrence_rule: String,
    pub recurrence_json: String,
    pub recurrence_exceptions_json: String,
    pub title: String,
    pub location: String,
    pub organizer_json: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
    pub body_html: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientContact {
    pub id: Uuid,
    pub address_book_id: String,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone)]
pub struct UpsertClientContactInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone)]
pub struct UpsertClientEventInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub uid: String,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub all_day: bool,
    pub status: String,
    pub sequence: i32,
    pub recurrence_rule: String,
    pub recurrence_json: String,
    pub recurrence_exceptions_json: String,
    pub title: String,
    pub location: String,
    pub organizer_json: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
    pub body_html: String,
}

impl Storage {
    pub async fn fetch_client_workspace(&self, account_id: Uuid) -> Result<ClientWorkspace> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let message_rows = sqlx::query_as::<_, ClientMessageRow>(
            r#"
            SELECT
                m.id,
                mb.role AS mailbox_role,
                COALESCE(NULLIF(fr.display_name, ''), fr.address, '') AS from_name,
                COALESCE(fr.address, '') AS from_address,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.tenant_id = m.tenant_id
                      AND r.message_id = m.id
                      AND r.role = 'to'
                ), '') AS to_recipients,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.tenant_id = m.tenant_id
                      AND r.message_id = m.id
                      AND r.role = 'cc'
                ), '') AS cc_recipients,
                m.normalized_subject AS subject,
                COALESCE(left(b.body_text, 160), '') AS preview,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS received_at,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'HH24:MI') AS time_label,
                NOT mm.is_seen AS unread,
                mm.is_flagged AS flagged,
                mm.followup_flag_status,
                CASE
                    WHEN mm.followup_start_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS followup_start_at,
                CASE
                    WHEN mm.followup_due_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS followup_due_at,
                CASE
                    WHEN mm.followup_completed_at IS NULL THEN NULL
                    ELSE to_char(mm.followup_completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS followup_completed_at,
                mm.reminder_set,
                CASE
                    WHEN mm.reminder_at IS NULL THEN NULL
                    ELSE to_char(mm.reminder_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS reminder_at,
                CASE
                    WHEN mm.reminder_dismissed_at IS NULL THEN NULL
                    ELSE to_char(mm.reminder_dismissed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS reminder_dismissed_at,
                COALESCE(sq.status, CASE WHEN mm.is_draft THEN 'draft' ELSE 'stored' END) AS delivery_status,
                COALESCE(b.body_text, '') AS body_text
            FROM messages m
            JOIN mailbox_messages mm
              ON mm.tenant_id = m.tenant_id
             AND mm.message_id = m.id
             AND mm.account_id = $2
             AND mm.visibility <> 'expunged'
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            LEFT JOIN message_recipients fr
              ON fr.tenant_id = m.tenant_id
             AND fr.message_id = m.id
             AND fr.role = 'from'
            LEFT JOIN message_bodies b
              ON b.tenant_id = m.tenant_id
             AND b.message_id = m.id
             AND b.body_kind = 'text'
            LEFT JOIN submission_queue sq
              ON sq.tenant_id = mm.tenant_id
             AND sq.account_id = mm.account_id
             AND sq.sent_mailbox_message_id = mm.id
            WHERE m.tenant_id = $1
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC
            LIMIT 250
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let attachment_rows = sqlx::query_as::<_, ClientAttachmentRow>(
            r#"
            SELECT
                a.id,
                a.message_id,
                a.file_name AS name,
                COALESCE(mp.content_type, '') AS media_type,
                a.size_octets
            FROM attachments a
            JOIN mailbox_messages mm
              ON mm.tenant_id = a.tenant_id
             AND mm.message_id = a.message_id
             AND mm.account_id = $2
             AND mm.visibility <> 'expunged'
            LEFT JOIN mime_parts mp
              ON mp.tenant_id = a.tenant_id
             AND mp.message_id = a.message_id
             AND mp.id = a.mime_part_id
            WHERE a.tenant_id = $1
            ORDER BY a.file_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let events = self.fetch_client_events(account_id).await?;
        let contacts = self.fetch_client_contacts(account_id).await?;
        let tasks = self.fetch_client_tasks(account_id).await?;

        let messages = message_rows
            .into_iter()
            .map(|row| {
                let attachments = attachment_rows
                    .iter()
                    .filter(|attachment| attachment.message_id == row.id)
                    .map(|attachment| ClientAttachment {
                        id: attachment.id,
                        name: attachment.name.clone(),
                        kind: attachments::attachment_kind(
                            &attachment.media_type,
                            &attachment.name,
                        ),
                        size: format_size(attachment.size_octets),
                    })
                    .collect();

                ClientMessage {
                    id: row.id,
                    folder: client_folder(&row.mailbox_role),
                    from: row.from_name,
                    from_address: row.from_address,
                    to: row.to_recipients,
                    cc: row.cc_recipients,
                    subject: row.subject,
                    preview: row.preview,
                    received_at: row.received_at,
                    time_label: row.time_label,
                    unread: row.unread,
                    flagged: row.flagged,
                    followup_flag_status: row.followup_flag_status,
                    followup_start_at: row.followup_start_at,
                    followup_due_at: row.followup_due_at,
                    followup_completed_at: row.followup_completed_at,
                    reminder_set: row.reminder_set,
                    reminder_at: row.reminder_at,
                    reminder_dismissed_at: row.reminder_dismissed_at,
                    tags: client_message_tags(&row.mailbox_role, &row.delivery_status),
                    attachments,
                    body: body_paragraphs(&row.body_text),
                }
            })
            .collect();

        Ok(ClientWorkspace {
            messages,
            events,
            contacts,
            tasks,
        })
    }

    pub async fn upsert_client_contact(
        &self,
        input: UpsertClientContactInput,
    ) -> Result<ClientContact> {
        self.upsert_client_contact_in_book_role(input, crate::DEFAULT_CONTACT_BOOK_ROLE)
            .await
    }

    pub(crate) async fn upsert_client_contact_in_book_role(
        &self,
        input: UpsertClientContactInput,
        contact_book_role: &str,
    ) -> Result<ClientContact> {
        let name = input.name.trim();
        let email = normalize_email(&input.email);
        if name.is_empty() || email.is_empty() {
            bail!("contact name and email are required");
        }

        let contact_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        let contact_book_id = Self::ensure_contact_book_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            contact_book_role,
        )
        .await?;
        let row = sqlx::query_as::<_, ClientContactRow>(
            r#"
            INSERT INTO contacts (
                id, tenant_id, owner_account_id, contact_book_id, uid,
                display_name, role, emails_json, phones_json, organization_unit, notes
            )
            VALUES (
                $1, $2, $3, $4, $1::text,
                $5, $6,
                jsonb_build_array(jsonb_build_object('email', $7::text, 'label', 'work', 'isDefault', true)),
                CASE
                    WHEN NULLIF($8, '') IS NULL THEN '[]'::jsonb
                    ELSE jsonb_build_array(jsonb_build_object('phone', $8::text, 'label', 'work'))
                END,
                $9,
                $10
            )
            ON CONFLICT (id) DO UPDATE SET
                contact_book_id = EXCLUDED.contact_book_id,
                uid = EXCLUDED.uid,
                display_name = EXCLUDED.display_name,
                role = EXCLUDED.role,
                emails_json = EXCLUDED.emails_json,
                phones_json = EXCLUDED.phones_json,
                organization_unit = EXCLUDED.organization_unit,
                notes = EXCLUDED.notes,
                updated_at = NOW()
            WHERE contacts.tenant_id = EXCLUDED.tenant_id
              AND contacts.owner_account_id = EXCLUDED.owner_account_id
            RETURNING
                id,
                $11::text AS address_book_id,
                display_name AS name,
                role,
                COALESCE(emails_json->0->>'email', '') AS email,
                COALESCE(phones_json->0->>'phone', '') AS phone,
                organization_unit AS team,
                notes
            "#,
        )
        .bind(contact_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(contact_book_id)
        .bind(name)
        .bind(input.role.trim())
        .bind(email)
        .bind(input.phone.trim())
        .bind(input.team.trim())
        .bind(input.notes.trim())
        .bind(client_address_book_id_for_role(contact_book_role))
        .fetch_one(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Contacts.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "contact",
            contact_id,
            "updated",
            modseq,
            &[input.account_id],
            serde_json::json!({
                "collectionId": contact_book_id,
                "objectUid": contact_id.to_string(),
            }),
        )
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_contact(row))
    }

    pub async fn upsert_client_event(&self, input: UpsertClientEventInput) -> Result<ClientEvent> {
        self.upsert_client_event_in_calendar(input, None).await
    }

    pub(crate) async fn upsert_client_event_in_calendar(
        &self,
        input: UpsertClientEventInput,
        calendar_id: Option<Uuid>,
    ) -> Result<ClientEvent> {
        if input.date.trim().is_empty()
            || input.time.trim().is_empty()
            || input.title.trim().is_empty()
        {
            bail!("event date, time, and title are required");
        }

        let event_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        let calendar_id = match calendar_id {
            Some(calendar_id) => calendar_id,
            None => {
                Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, input.account_id).await?
            }
        };
        let row = sqlx::query_as::<_, ClientEventRow>(
            r#"
            INSERT INTO calendar_events (
                id, tenant_id, owner_account_id, calendar_id, uid,
                starts_at, ends_at, time_zone, all_day, status, sequence,
                recurrence_rule, recurrence_json, recurrence_exceptions_json,
                title, location, organizer_json, attendees_json, body_text, body_html,
                source_payload_json
            )
            VALUES (
                $1, $2, $3, $4, COALESCE(NULLIF($5, ''), $1::text),
                (($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')),
                ((($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')) + make_interval(mins => GREATEST($9, 0))),
                $8,
                $10,
                COALESCE(NULLIF($11, ''), 'confirmed'),
                GREATEST($12, 0),
                NULLIF($13, ''),
                CASE
                    WHEN NULLIF($14, '') IS NOT NULL THEN $14::jsonb
                    ELSE '{}'::jsonb
                END,
                CASE
                    WHEN NULLIF($15, '') IS NOT NULL THEN $15::jsonb
                    ELSE '[]'::jsonb
                END,
                $16,
                $17,
                CASE
                    WHEN NULLIF($18, '') IS NOT NULL THEN $18::jsonb
                    ELSE '{}'::jsonb
                END,
                CASE
                    WHEN NULLIF($20, '') IS NOT NULL THEN $20::jsonb
                    WHEN NULLIF($19, '') IS NOT NULL THEN jsonb_build_object('attendees', jsonb_build_array(jsonb_build_object('email', $19::text)))
                    ELSE '{}'::jsonb
                END,
                $21,
                NULLIF($22, ''),
                jsonb_build_object('attendees', $19::text)
            )
            ON CONFLICT (id) DO UPDATE SET
                calendar_id = EXCLUDED.calendar_id,
                uid = EXCLUDED.uid,
                starts_at = EXCLUDED.starts_at,
                ends_at = EXCLUDED.ends_at,
                time_zone = EXCLUDED.time_zone,
                all_day = EXCLUDED.all_day,
                status = EXCLUDED.status,
                sequence = EXCLUDED.sequence,
                recurrence_rule = EXCLUDED.recurrence_rule,
                recurrence_json = EXCLUDED.recurrence_json,
                recurrence_exceptions_json = EXCLUDED.recurrence_exceptions_json,
                title = EXCLUDED.title,
                location = EXCLUDED.location,
                organizer_json = EXCLUDED.organizer_json,
                attendees_json = EXCLUDED.attendees_json,
                body_text = EXCLUDED.body_text,
                body_html = EXCLUDED.body_html,
                source_payload_json = EXCLUDED.source_payload_json,
                updated_at = NOW()
            WHERE calendar_events.tenant_id = EXCLUDED.tenant_id
              AND calendar_events.owner_account_id = EXCLUDED.owner_account_id
            RETURNING
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                all_day,
                status,
                sequence,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                recurrence_json::text AS recurrence_json,
                recurrence_exceptions_json::text AS recurrence_exceptions_json,
                title,
                location,
                organizer_json::text AS organizer_json,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes,
                COALESCE(body_html, '') AS body_html
            "#,
        )
        .bind(event_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(calendar_id)
        .bind(input.uid.trim())
        .bind(input.date.trim())
        .bind(input.time.trim())
        .bind(input.time_zone.trim())
        .bind(input.duration_minutes.max(0))
        .bind(input.all_day)
        .bind(input.status.trim())
        .bind(input.sequence)
        .bind(input.recurrence_rule.trim())
        .bind(input.recurrence_json.trim())
        .bind(input.recurrence_exceptions_json.trim())
        .bind(input.title.trim())
        .bind(input.location.trim())
        .bind(input.organizer_json.trim())
        .bind(input.attendees.trim())
        .bind(input.attendees_json.trim())
        .bind(input.notes.trim())
        .bind(input.body_html.trim())
        .fetch_one(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &[input.account_id],
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": row.uid.clone(),
            }),
        )
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_event(row))
    }

    pub async fn fetch_client_events(&self, account_id: Uuid) -> Result<Vec<ClientEvent>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                all_day,
                status,
                sequence,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                recurrence_json::text AS recurrence_json,
                recurrence_exceptions_json::text AS recurrence_exceptions_json,
                title,
                location,
                organizer_json::text AS organizer_json,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes,
                COALESCE(body_html, '') AS body_html
            FROM calendar_events
            WHERE tenant_id = $1 AND owner_account_id = $2
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }

    pub async fn fetch_client_events_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                uid,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60) AS duration_minutes,
                COALESCE(recurrence_rule, '') AS recurrence_rule,
                title,
                location,
                COALESCE(source_payload_json->>'attendees', '') AS attendees,
                attendees_json::text AS attendees_json,
                body_text AS notes
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = ANY($3)
            ORDER BY starts_at ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }

    pub async fn fetch_client_contacts(&self, account_id: Uuid) -> Result<Vec<ClientContact>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientContactRow>(
            r#"
            SELECT
                contacts.id,
                CASE
                    WHEN b.role = 'suggested_contacts' THEN 'suggested_contacts'
                    WHEN b.role = 'quick_contacts' THEN 'quick_contacts'
                    WHEN b.role = 'im_contact_list' THEN 'im_contact_list'
                    ELSE 'default'
                END AS address_book_id,
                contacts.display_name AS name,
                contacts.role,
                COALESCE(contacts.emails_json->0->>'email', '') AS email,
                COALESCE(contacts.phones_json->0->>'phone', '') AS phone,
                contacts.organization_unit AS team,
                contacts.notes
            FROM contacts
            JOIN contact_books b
              ON b.tenant_id = contacts.tenant_id
             AND b.owner_account_id = contacts.owner_account_id
             AND b.id = contacts.contact_book_id
            WHERE contacts.tenant_id = $1 AND contacts.owner_account_id = $2
            ORDER BY contacts.display_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_contact).collect())
    }

    pub async fn fetch_client_contacts_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientContact>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, ClientContactRow>(
            r#"
            SELECT
                contacts.id,
                CASE
                    WHEN b.role = 'suggested_contacts' THEN 'suggested_contacts'
                    WHEN b.role = 'quick_contacts' THEN 'quick_contacts'
                    WHEN b.role = 'im_contact_list' THEN 'im_contact_list'
                    ELSE 'default'
                END AS address_book_id,
                contacts.display_name AS name,
                contacts.role,
                COALESCE(contacts.emails_json->0->>'email', '') AS email,
                COALESCE(contacts.phones_json->0->>'phone', '') AS phone,
                contacts.organization_unit AS team,
                contacts.notes
            FROM contacts
            JOIN contact_books b
              ON b.tenant_id = contacts.tenant_id
             AND b.owner_account_id = contacts.owner_account_id
             AND b.id = contacts.contact_book_id
            WHERE contacts.tenant_id = $1
              AND contacts.owner_account_id = $2
              AND contacts.id = ANY($3)
            ORDER BY contacts.display_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_contact).collect())
    }
}

fn body_paragraphs(body_text: &str) -> Vec<String> {
    let paragraphs = body_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if paragraphs.is_empty() {
        vec!["".to_string()]
    } else {
        paragraphs
    }
}

fn client_folder(role: &str) -> String {
    match role {
        "drafts" => "drafts",
        "sent" => "sent",
        "archive" => "archive",
        "junk" => "junk",
        "outbox" => "outbox",
        "rss_feeds" => "rss_feeds",
        "conversation_history" => "conversation_history",
        "sync_issues" => "sync_issues",
        "conflicts" => "conflicts",
        "local_failures" => "local_failures",
        "server_failures" => "server_failures",
        _ => "inbox",
    }
    .to_string()
}

fn client_message_tags(role: &str, delivery_status: &str) -> Vec<String> {
    if role == "drafts" || delivery_status == "draft" {
        return vec!["Draft".to_string()];
    }
    if role == "sent" {
        return vec!["Outgoing".to_string()];
    }
    Vec::new()
}

fn format_size(size_octets: i64) -> String {
    let size = size_octets.max(0) as f64;
    if size >= 1_048_576.0 {
        format!("{:.1} MB", size / 1_048_576.0)
    } else if size >= 1024.0 {
        format!("{:.0} KB", size / 1024.0)
    } else {
        format!("{} B", size as i64)
    }
}

fn map_event(row: ClientEventRow) -> ClientEvent {
    ClientEvent {
        id: row.id,
        uid: row.uid,
        date: row.date,
        time: row.time,
        time_zone: row.time_zone,
        duration_minutes: row.duration_minutes,
        all_day: row.all_day,
        status: row.status,
        sequence: row.sequence,
        recurrence_rule: row.recurrence_rule,
        recurrence_json: row.recurrence_json,
        recurrence_exceptions_json: row.recurrence_exceptions_json,
        title: row.title,
        location: row.location,
        organizer_json: row.organizer_json,
        attendees: row.attendees,
        attendees_json: row.attendees_json,
        notes: row.notes,
        body_html: row.body_html,
    }
}

fn map_contact(row: ClientContactRow) -> ClientContact {
    ClientContact {
        id: row.id,
        address_book_id: row.address_book_id,
        name: row.name,
        role: row.role,
        email: row.email,
        phone: row.phone,
        team: row.team,
        notes: row.notes,
    }
}

fn client_address_book_id_for_role(role: &str) -> &'static str {
    match role {
        crate::SUGGESTED_CONTACTS_ROLE => crate::SUGGESTED_CONTACTS_COLLECTION_ID,
        crate::QUICK_CONTACTS_ROLE => crate::QUICK_CONTACTS_COLLECTION_ID,
        crate::IM_CONTACT_LIST_ROLE => crate::IM_CONTACT_LIST_COLLECTION_ID,
        _ => crate::DEFAULT_COLLECTION_ID,
    }
}
