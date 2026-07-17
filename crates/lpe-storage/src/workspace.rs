use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    attachments, normalize_email, CanonicalChangeCategory, ClientAttachment, ClientAttachmentRow,
    ClientContactRow, ClientEventRow, ClientMessageRow, ClientTask, ContactNameFields,
    ContactSourceFields, Storage,
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
    pub structured_name: ContactNameFields,
    pub emails_json: Value,
    pub phones_json: Value,
    pub addresses_json: Value,
    pub urls_json: Value,
    pub organization_name: String,
    pub job_title: String,
    pub raw_vcard: Option<String>,
    pub source: ContactSourceFields,
}

impl Default for ClientContact {
    fn default() -> Self {
        Self {
            id: Uuid::nil(),
            address_book_id: String::new(),
            name: String::new(),
            role: String::new(),
            email: String::new(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
            structured_name: ContactNameFields::default(),
            emails_json: Value::Array(Vec::new()),
            phones_json: Value::Array(Vec::new()),
            addresses_json: Value::Array(Vec::new()),
            urls_json: Value::Array(Vec::new()),
            organization_name: String::new(),
            job_title: String::new(),
            raw_vcard: None,
            source: ContactSourceFields::default(),
        }
    }
}

impl ClientContact {
    pub fn primary_email(&self) -> &str {
        &self.email
    }

    pub fn primary_phone(&self) -> &str {
        &self.phone
    }

    pub fn display_name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClientContactInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
    #[serde(default)]
    pub structured_name: ContactNameFields,
    #[serde(default)]
    pub emails_json: Option<Value>,
    #[serde(default)]
    pub phones_json: Option<Value>,
    #[serde(default)]
    pub addresses_json: Option<Value>,
    #[serde(default)]
    pub urls_json: Option<Value>,
    #[serde(default)]
    pub organization_name: String,
    #[serde(default)]
    pub job_title: String,
    #[serde(default)]
    pub raw_vcard: Option<String>,
    #[serde(default)]
    pub raw_vcard_is_explicit: bool,
    #[serde(default)]
    pub source: ContactSourceFields,
    #[serde(default)]
    pub source_is_explicit: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipientSuggestion {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub source_kind: String,
    pub use_count: i32,
    pub last_used_at: String,
    pub contact_id: Option<Uuid>,
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
        mut input: UpsertClientContactInput,
        contact_book_role: &str,
    ) -> Result<ClientContact> {
        if let Some(contact_id) = input.id {
            if let Some(existing) = self
                .fetch_client_contacts_by_ids(input.account_id, &[contact_id])
                .await?
                .into_iter()
                .next()
            {
                input = merge_contact_update_input(&existing, input);
            }
        }
        let name = input.name.trim();
        let emails_json = contact_emails_json(&input)?;
        let email = contact_primary_email(&emails_json);
        if name.is_empty() || email.is_empty() {
            bail!("contact name and email are required");
        }
        let phones_json = contact_phones_json(&input)?;
        let addresses_json = contact_array_json(input.addresses_json)?;
        let urls_json = contact_array_json(input.urls_json)?;
        let source_payload_json = contact_source_payload_json(input.source.source_payload_json)?;

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
                display_name, name_prefix, given_name, middle_name, family_name, name_suffix,
                nickname, phonetic_given_name, phonetic_family_name, job_title, role,
                organization_name, organization_unit, emails_json, phones_json, addresses_json,
                urls_json, notes, raw_vcard, import_source, source_uid, source_etag,
                source_payload_json
            )
            VALUES (
                $1, $2, $3, $4, $1::text,
                $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26,
                $27
            )
            ON CONFLICT (id) DO UPDATE SET
                contact_book_id = EXCLUDED.contact_book_id,
                uid = EXCLUDED.uid,
                display_name = EXCLUDED.display_name,
                name_prefix = EXCLUDED.name_prefix,
                given_name = EXCLUDED.given_name,
                middle_name = EXCLUDED.middle_name,
                family_name = EXCLUDED.family_name,
                name_suffix = EXCLUDED.name_suffix,
                nickname = EXCLUDED.nickname,
                phonetic_given_name = EXCLUDED.phonetic_given_name,
                phonetic_family_name = EXCLUDED.phonetic_family_name,
                job_title = EXCLUDED.job_title,
                role = EXCLUDED.role,
                organization_name = EXCLUDED.organization_name,
                organization_unit = EXCLUDED.organization_unit,
                emails_json = EXCLUDED.emails_json,
                phones_json = EXCLUDED.phones_json,
                addresses_json = EXCLUDED.addresses_json,
                urls_json = EXCLUDED.urls_json,
                notes = EXCLUDED.notes,
                raw_vcard = EXCLUDED.raw_vcard,
                import_source = EXCLUDED.import_source,
                source_uid = EXCLUDED.source_uid,
                source_etag = EXCLUDED.source_etag,
                source_payload_json = EXCLUDED.source_payload_json,
                updated_at = NOW()
            WHERE contacts.tenant_id = EXCLUDED.tenant_id
              AND contacts.owner_account_id = EXCLUDED.owner_account_id
            RETURNING
                id,
                $28::text AS address_book_id,
                display_name AS name,
                role,
                COALESCE(emails_json->0->>'email', '') AS email,
                COALESCE(phones_json->0->>'phone', '') AS phone,
                organization_unit AS team,
                notes,
                name_prefix,
                given_name,
                middle_name,
                family_name,
                name_suffix,
                nickname,
                phonetic_given_name,
                phonetic_family_name,
                emails_json,
                phones_json,
                addresses_json,
                urls_json,
                organization_name,
                job_title,
                raw_vcard,
                import_source,
                source_uid,
                source_etag,
                source_payload_json
            "#,
        )
        .bind(contact_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(contact_book_id)
        .bind(name)
        .bind(input.structured_name.prefix.trim())
        .bind(input.structured_name.given.trim())
        .bind(input.structured_name.middle.trim())
        .bind(input.structured_name.family.trim())
        .bind(input.structured_name.suffix.trim())
        .bind(input.structured_name.nickname.trim())
        .bind(input.structured_name.phonetic_given.trim())
        .bind(input.structured_name.phonetic_family.trim())
        .bind(input.job_title.trim())
        .bind(input.role.trim())
        .bind(if input.organization_name.trim().is_empty() {
            input.team.trim()
        } else {
            input.organization_name.trim()
        })
        .bind(input.team.trim())
        .bind(emails_json)
        .bind(phones_json)
        .bind(addresses_json)
        .bind(urls_json)
        .bind(input.notes.trim())
        .bind(input.raw_vcard.as_deref())
        .bind(if input.source.import_source.trim().is_empty() {
            "local"
        } else {
            input.source.import_source.trim()
        })
        .bind(input.source.source_uid.as_deref())
        .bind(input.source.source_etag.as_deref())
        .bind(source_payload_json)
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
              AND calendar_events.lifecycle_state = 'active'
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
        self.advance_calendar_event_version_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            event_id,
            modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            event_id,
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
            &affected_principals,
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
              AND lifecycle_state = 'active'
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
                contacts.notes,
                contacts.name_prefix,
                contacts.given_name,
                contacts.middle_name,
                contacts.family_name,
                contacts.name_suffix,
                contacts.nickname,
                contacts.phonetic_given_name,
                contacts.phonetic_family_name,
                contacts.emails_json,
                contacts.phones_json,
                contacts.addresses_json,
                contacts.urls_json,
                contacts.organization_name,
                contacts.job_title,
                contacts.raw_vcard,
                contacts.import_source,
                contacts.source_uid,
                contacts.source_etag,
                contacts.source_payload_json
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
                contacts.notes,
                contacts.name_prefix,
                contacts.given_name,
                contacts.middle_name,
                contacts.family_name,
                contacts.name_suffix,
                contacts.nickname,
                contacts.phonetic_given_name,
                contacts.phonetic_family_name,
                contacts.emails_json,
                contacts.phones_json,
                contacts.addresses_json,
                contacts.urls_json,
                contacts.organization_name,
                contacts.job_title,
                contacts.raw_vcard,
                contacts.import_source,
                contacts.source_uid,
                contacts.source_etag,
                contacts.source_payload_json
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

    pub async fn query_recipient_suggestions(
        &self,
        account_id: Uuid,
        query: Option<&str>,
    ) -> Result<Vec<RecipientSuggestion>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let needle = query.unwrap_or_default().trim().to_lowercase();
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                normalized_email,
                display_name,
                source_kind,
                use_count,
                to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_used_at,
                contact_id
            FROM recipient_suggestions
            WHERE tenant_id = $1
              AND account_id = $2
              AND dismissed_at IS NULL
              AND (
                $3 = ''
                OR normalized_email LIKE '%' || $3 || '%'
                OR lower(display_name) LIKE '%' || $3 || '%'
              )
            ORDER BY use_count DESC, last_used_at DESC, lower(display_name), normalized_email
            LIMIT 50
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(needle)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(RecipientSuggestion {
                    id: row.try_get("id")?,
                    email: row.try_get("normalized_email")?,
                    display_name: row.try_get("display_name")?,
                    source_kind: row.try_get("source_kind")?,
                    use_count: row.try_get("use_count")?,
                    last_used_at: row.try_get("last_used_at")?,
                    contact_id: row.try_get("contact_id")?,
                })
            })
            .collect()
    }

    pub async fn dismiss_recipient_suggestion(
        &self,
        account_id: Uuid,
        suggestion_id: Uuid,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let result = sqlx::query(
            r#"
            UPDATE recipient_suggestions
            SET dismissed_at = NOW(), updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
              AND dismissed_at IS NULL
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(suggestion_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            bail!("recipient suggestion not found");
        }
        Ok(())
    }
}

fn contact_emails_json(input: &UpsertClientContactInput) -> Result<Value> {
    if let Some(value) = input.emails_json.clone() {
        return contact_array_json(Some(value));
    }
    Ok(serde_json::json!([{
        "email": normalize_email(&input.email),
        "label": "work",
        "isDefault": true
    }]))
}

fn contact_phones_json(input: &UpsertClientContactInput) -> Result<Value> {
    if let Some(value) = input.phones_json.clone() {
        return contact_array_json(Some(value));
    }
    if input.phone.trim().is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    Ok(serde_json::json!([{
        "phone": input.phone.trim(),
        "label": "work"
    }]))
}

fn contact_array_json(value: Option<Value>) -> Result<Value> {
    match value {
        Some(array @ Value::Array(_)) => Ok(array),
        Some(_) => bail!("contact JSON field must be an array"),
        None => Ok(Value::Array(Vec::new())),
    }
}

fn contact_source_payload_json(value: Value) -> Result<Value> {
    match value {
        Value::Object(_) => Ok(value),
        _ => bail!("contact source payload must be an object"),
    }
}

fn contact_primary_email(value: &Value) -> String {
    value
        .as_array()
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("email")
                    .or_else(|| item.get("address"))
                    .and_then(Value::as_str)
            })
        })
        .map(normalize_email)
        .unwrap_or_default()
}

fn merge_contact_update_input(
    existing: &ClientContact,
    mut input: UpsertClientContactInput,
) -> UpsertClientContactInput {
    if input.emails_json.is_none() {
        input.emails_json = Some(if normalize_email(&input.email) == existing.email {
            existing.emails_json.clone()
        } else {
            contact_json_with_primary_value(&existing.emails_json, "email", "work", &input.email)
        });
    }
    if input.phones_json.is_none() {
        input.phones_json = Some(if input.phone.trim() == existing.phone {
            existing.phones_json.clone()
        } else {
            contact_json_with_primary_value(&existing.phones_json, "phone", "work", &input.phone)
        });
    }
    if input.addresses_json.is_none() {
        input.addresses_json = Some(existing.addresses_json.clone());
    }
    if input.urls_json.is_none() {
        input.urls_json = Some(existing.urls_json.clone());
    }
    if !input.raw_vcard_is_explicit {
        input.raw_vcard = existing.raw_vcard.clone();
    }
    if !input.source_is_explicit {
        input.source = existing.source.clone();
    }
    input
}

fn contact_json_with_primary_value(existing: &Value, key: &str, label: &str, value: &str) -> Value {
    let value = value.trim();
    if value.is_empty() {
        return Value::Array(Vec::new());
    }

    let mut items = existing.as_array().cloned().unwrap_or_default();
    if let Some(item) = items.iter_mut().find(|item| item.get(key).is_some()) {
        if let Some(object) = item.as_object_mut() {
            object.insert(key.to_string(), Value::String(value.to_string()));
            object.insert("label".to_string(), Value::String(label.to_string()));
            object.insert("isDefault".to_string(), Value::Bool(true));
            return Value::Array(items);
        }
    }

    let mut primary = serde_json::Map::new();
    primary.insert(key.to_string(), Value::String(value.to_string()));
    primary.insert("label".to_string(), Value::String(label.to_string()));
    primary.insert("isDefault".to_string(), Value::Bool(true));
    items.insert(0, Value::Object(primary));
    Value::Array(items)
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
        "trash" => "trash",
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
        structured_name: ContactNameFields {
            prefix: row.name_prefix,
            given: row.given_name,
            middle: row.middle_name,
            family: row.family_name,
            suffix: row.name_suffix,
            nickname: row.nickname,
            phonetic_given: row.phonetic_given_name,
            phonetic_family: row.phonetic_family_name,
        },
        emails_json: row.emails_json,
        phones_json: row.phones_json,
        addresses_json: row.addresses_json,
        urls_json: row.urls_json,
        organization_name: row.organization_name,
        job_title: row.job_title,
        raw_vcard: row.raw_vcard,
        source: ContactSourceFields {
            import_source: row.import_source,
            source_uid: row.source_uid,
            source_etag: row.source_etag,
            source_payload_json: row.source_payload_json,
        },
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

#[cfg(test)]
mod tests {
    use super::{
        client_folder, merge_contact_update_input, ClientContact, ContactSourceFields,
        UpsertClientContactInput, Value,
    };
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn client_folder_preserves_trash_role() {
        assert_eq!(client_folder("trash"), "trash");
        assert_eq!(client_folder("unknown"), "inbox");
    }

    #[test]
    fn contact_update_merges_missing_rich_fields() {
        let existing = ClientContact {
            id: Uuid::from_u128(1),
            name: "Ada Example".to_string(),
            email: "ada@example.test".to_string(),
            phone: "+1 555 0100".to_string(),
            addresses_json: json!([{"full": "1 Example Way"}]),
            urls_json: json!([{"url": "https://example.test"}]),
            raw_vcard: Some("BEGIN:VCARD\nEND:VCARD".to_string()),
            source: ContactSourceFields {
                import_source: "carddav".to_string(),
                source_uid: Some("uid-1".to_string()),
                source_etag: Some("etag-1".to_string()),
                source_payload_json: json!({"href": "/contacts/1.vcf"}),
            },
            ..ClientContact::default()
        };

        let merged = merge_contact_update_input(
            &existing,
            UpsertClientContactInput {
                id: Some(existing.id),
                account_id: Uuid::from_u128(2),
                name: "Ada Updated".to_string(),
                email: existing.email.clone(),
                phone: existing.phone.clone(),
                ..UpsertClientContactInput::default()
            },
        );

        assert_eq!(merged.addresses_json, Some(existing.addresses_json.clone()));
        assert_eq!(merged.urls_json, Some(existing.urls_json.clone()));
        assert_eq!(merged.raw_vcard, existing.raw_vcard);
        assert_eq!(merged.source.import_source, "carddav");
        assert_eq!(
            merged.source.source_payload_json,
            json!({"href": "/contacts/1.vcf"})
        );
    }

    #[test]
    fn contact_update_can_clear_explicit_rich_fields() {
        let existing = ClientContact {
            id: Uuid::from_u128(1),
            email: "ada@example.test".to_string(),
            phone: "+1 555 0100".to_string(),
            addresses_json: json!([{"full": "1 Example Way"}]),
            urls_json: json!([{"url": "https://example.test"}]),
            raw_vcard: Some("BEGIN:VCARD\nEND:VCARD".to_string()),
            ..ClientContact::default()
        };

        let merged = merge_contact_update_input(
            &existing,
            UpsertClientContactInput {
                id: Some(existing.id),
                account_id: Uuid::from_u128(2),
                name: "Ada Example".to_string(),
                email: existing.email.clone(),
                phone: existing.phone.clone(),
                addresses_json: Some(Value::Array(Vec::new())),
                urls_json: Some(Value::Array(Vec::new())),
                raw_vcard: None,
                raw_vcard_is_explicit: true,
                ..UpsertClientContactInput::default()
            },
        );

        assert_eq!(merged.addresses_json, Some(Value::Array(Vec::new())));
        assert_eq!(merged.urls_json, Some(Value::Array(Vec::new())));
        assert_eq!(merged.raw_vcard, None);
    }
}
