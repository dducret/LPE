use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::{
    external_calendar_uid, normalize_calendar_meeting_uid, normalize_email,
    CanonicalChangeCategory, ClientAttachment, ClientContactRow, ClientEventRow, ClientTask,
    CollaborationCollection, ContactNameFields, ContactSourceFields, JmapMailbox, Storage,
};

mod client_workspace;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientWorkspace {
    pub messages: Vec<ClientMessage>,
    pub mailboxes: Vec<ClientMailbox>,
    pub events: Vec<ClientEvent>,
    pub event_collection_ids: BTreeMap<String, String>,
    pub contacts: Vec<ClientContact>,
    pub contact_books: Vec<CollaborationCollection>,
    pub calendar_collections: Vec<CollaborationCollection>,
    pub tasks: Vec<ClientTask>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientMailbox {
    pub id: Uuid,
    pub parent_id: Option<Uuid>,
    pub role: String,
    pub name: String,
    pub sort_order: i32,
    pub total_emails: u32,
    pub unread_emails: u32,
    pub is_subscribed: bool,
}

impl From<JmapMailbox> for ClientMailbox {
    fn from(mailbox: JmapMailbox) -> Self {
        Self {
            id: mailbox.id,
            parent_id: mailbox.parent_id,
            role: mailbox.role,
            name: mailbox.name,
            sort_order: mailbox.sort_order,
            total_emails: mailbox.total_emails,
            unread_emails: mailbox.unread_emails,
            is_subscribed: mailbox.is_subscribed,
        }
    }
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
    pub photo_data: Option<String>,
    pub photo_content_type: Option<String>,
    pub categories_json: Value,
    pub birthday: Option<String>,
    pub anniversary: Option<String>,
    pub children_json: Value,
    pub spouse: String,
    pub assistant_name: String,
    pub assistant_phone: String,
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
            photo_data: None,
            photo_content_type: None,
            categories_json: Value::Array(Vec::new()),
            birthday: None,
            anniversary: None,
            children_json: Value::Array(Vec::new()),
            spouse: String::new(),
            assistant_name: String::new(),
            assistant_phone: String::new(),
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
    pub photo_data: Option<Option<String>>,
    #[serde(default)]
    pub photo_content_type: Option<Option<String>>,
    #[serde(default)]
    pub categories_json: Option<Value>,
    #[serde(default)]
    pub birthday: Option<Option<String>>,
    #[serde(default)]
    pub anniversary: Option<Option<String>>,
    #[serde(default)]
    pub children_json: Option<Value>,
    #[serde(default)]
    pub spouse: Option<String>,
    #[serde(default)]
    pub assistant_name: Option<String>,
    #[serde(default)]
    pub assistant_phone: Option<String>,
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
        let existing = if let Some(contact_id) = input.id {
            self.fetch_client_contacts_by_ids(input.account_id, &[contact_id])
                .await?
                .into_iter()
                .next()
        } else {
            None
        };
        if let Some(existing) = &existing {
            input = merge_contact_update_input(existing, input);
        }
        let name = input.name.trim();
        let emails_json = contact_emails_json(&input)?;
        let email = contact_primary_email(&emails_json);
        if name.is_empty() || email.is_empty() {
            bail!("contact name and email are required");
        }
        let phones_json = contact_phones_json(&input)?;
        let addresses_json = contact_array_json(input.addresses_json.clone())?;
        let urls_json = contact_array_json(input.urls_json.clone())?;
        let categories_json =
            contact_string_array_json(input.categories_json.clone(), "categories")?;
        let children_json = contact_string_array_json(input.children_json.clone(), "children")?;
        let source_payload_json =
            contact_source_payload_json(input.source.source_payload_json.clone())?;

        if let Some(existing) = existing {
            if contact_update_is_unchanged(
                &existing,
                &input,
                contact_book_role,
                &emails_json,
                &phones_json,
                &addresses_json,
                &urls_json,
                &categories_json,
                &children_json,
                &source_payload_json,
            ) {
                return Ok(existing);
            }
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
                display_name, name_prefix, given_name, middle_name, family_name, name_suffix,
                nickname, phonetic_given_name, phonetic_family_name, job_title, role,
                organization_name, organization_unit, emails_json, phones_json, addresses_json,
                urls_json, photo_data, photo_content_type, categories_json, birthday, anniversary,
                children_json, spouse, assistant_name, assistant_phone, notes, raw_vcard, import_source, source_uid, source_etag,
                source_payload_json
            )
            VALUES (
                $1, $2, $3, $4, $1::text,
                $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35,
                $36
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
                photo_data = EXCLUDED.photo_data,
                photo_content_type = EXCLUDED.photo_content_type,
                categories_json = EXCLUDED.categories_json,
                birthday = EXCLUDED.birthday,
                anniversary = EXCLUDED.anniversary,
                children_json = EXCLUDED.children_json,
                spouse = EXCLUDED.spouse,
                assistant_name = EXCLUDED.assistant_name,
                assistant_phone = EXCLUDED.assistant_phone,
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
                $37::text AS address_book_id,
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
                photo_data,
                photo_content_type,
                categories_json,
                birthday,
                anniversary,
                children_json,
                spouse,
                assistant_name,
                assistant_phone,
                organization_name,
                job_title,
                raw_vcard,
                import_source,
                source_uid,
                source_etag,
                source_payload_json,
                (xmax = 0) AS created
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
        .bind(input.photo_data.flatten().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()))
        .bind(input.photo_content_type.flatten().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()))
        .bind(categories_json)
        .bind(input.birthday.flatten().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()))
        .bind(input.anniversary.flatten().map(|value| value.trim().to_string()).filter(|value| !value.is_empty()))
        .bind(children_json)
        .bind(input.spouse.as_deref().unwrap_or_default().trim())
        .bind(input.assistant_name.as_deref().unwrap_or_default().trim())
        .bind(input.assistant_phone.as_deref().unwrap_or_default().trim())
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
        let versioned = sqlx::query(
            r#"
            UPDATE contacts
            SET modseq = $5
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND contact_book_id = $3
              AND id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(contact_book_id)
        .bind(contact_id)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        if versioned.rows_affected() != 1 {
            bail!("contact disappeared before version assignment");
        }
        self.rotate_active_mapi_contact_identities_in_tx(&mut tx, &tenant_id, contact_id)
            .await?;
        sqlx::query(
            r#"
            UPDATE contact_books
            SET sync_modseq = GREATEST(sync_modseq, $4),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(contact_book_id)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(input.account_id),
            None,
            "contact",
            contact_id,
            if row.created == Some(true) {
                "created"
            } else {
                "updated"
            },
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
        let mut input = input;
        if !input.uid.trim().is_empty() {
            input.uid = normalize_calendar_meeting_uid(&input.uid);
        }
        if input.date.trim().is_empty()
            || input.time.trim().is_empty()
            || input.title.trim().is_empty()
        {
            bail!("event date, time, and title are required");
        }

        let event_id = input.id.unwrap_or_else(Uuid::new_v4);
        if input.id.is_some() {
            if let Some(existing) = self
                .fetch_client_events_by_ids(input.account_id, &[event_id])
                .await?
                .into_iter()
                .next()
            {
                if event_update_is_unchanged(&existing, &input, event_id) {
                    return Ok(existing);
                }
            }
        }
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
                COALESCE(body_html, '') AS body_html,
                (xmax = 0) AS created
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
            if row.created == Some(true) {
                "created"
            } else {
                "updated"
            },
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
                contacts.photo_data,
                contacts.photo_content_type,
                contacts.categories_json,
                contacts.birthday,
                contacts.anniversary,
                contacts.children_json,
                contacts.spouse,
                contacts.assistant_name,
                contacts.assistant_phone,
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
                contacts.photo_data,
                contacts.photo_content_type,
                contacts.categories_json,
                contacts.birthday,
                contacts.anniversary,
                contacts.children_json,
                contacts.spouse,
                contacts.assistant_name,
                contacts.assistant_phone,
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

pub(crate) fn contact_emails_json(input: &UpsertClientContactInput) -> Result<Value> {
    if let Some(value) = input.emails_json.clone() {
        return contact_array_json(Some(value));
    }
    Ok(serde_json::json!([{
        "email": normalize_email(&input.email),
        "label": "work",
        "isDefault": true
    }]))
}

pub(crate) fn contact_phones_json(input: &UpsertClientContactInput) -> Result<Value> {
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

pub(crate) fn contact_array_json(value: Option<Value>) -> Result<Value> {
    match value {
        Some(array @ Value::Array(_)) => Ok(array),
        Some(_) => bail!("contact JSON field must be an array"),
        None => Ok(Value::Array(Vec::new())),
    }
}

pub(crate) fn contact_string_array_json(value: Option<Value>, name: &str) -> Result<Value> {
    let value = contact_array_json(value)?;
    let Some(items) = value.as_array() else {
        bail!("{name} must be an array");
    };
    if items.iter().any(|item| item.as_str().is_none()) {
        bail!("{name} entries must be strings");
    }
    Ok(Value::Array(
        items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| Value::String(value.to_string()))
            .collect(),
    ))
}

pub(crate) fn contact_source_payload_json(value: Value) -> Result<Value> {
    match value {
        Value::Object(_) => Ok(value),
        _ => bail!("contact source payload must be an object"),
    }
}

pub(crate) fn contact_primary_email(value: &Value) -> String {
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

fn contact_update_is_unchanged(
    existing: &ClientContact,
    input: &UpsertClientContactInput,
    contact_book_role: &str,
    emails_json: &Value,
    phones_json: &Value,
    addresses_json: &Value,
    urls_json: &Value,
    categories_json: &Value,
    children_json: &Value,
    source_payload_json: &Value,
) -> bool {
    existing.address_book_id == client_address_book_id_for_role(contact_book_role)
        && existing.name == input.name.trim()
        && existing.role == input.role.trim()
        && existing.team == input.team.trim()
        && existing.notes == input.notes.trim()
        && existing.structured_name.prefix == input.structured_name.prefix.trim()
        && existing.structured_name.given == input.structured_name.given.trim()
        && existing.structured_name.middle == input.structured_name.middle.trim()
        && existing.structured_name.family == input.structured_name.family.trim()
        && existing.structured_name.suffix == input.structured_name.suffix.trim()
        && existing.structured_name.nickname == input.structured_name.nickname.trim()
        && existing.structured_name.phonetic_given == input.structured_name.phonetic_given.trim()
        && existing.structured_name.phonetic_family == input.structured_name.phonetic_family.trim()
        && existing.emails_json == *emails_json
        && existing.phones_json == *phones_json
        && existing.addresses_json == *addresses_json
        && existing.urls_json == *urls_json
        && existing.photo_data
            == input
                .photo_data
                .clone()
                .flatten()
                .filter(|value| !value.trim().is_empty())
        && existing.photo_content_type
            == input
                .photo_content_type
                .clone()
                .flatten()
                .filter(|value| !value.trim().is_empty())
        && existing.categories_json == *categories_json
        && existing.birthday
            == input
                .birthday
                .clone()
                .flatten()
                .filter(|value| !value.trim().is_empty())
        && existing.anniversary
            == input
                .anniversary
                .clone()
                .flatten()
                .filter(|value| !value.trim().is_empty())
        && existing.children_json == *children_json
        && existing.spouse == input.spouse.as_deref().unwrap_or_default().trim()
        && existing.assistant_name == input.assistant_name.as_deref().unwrap_or_default().trim()
        && existing.assistant_phone == input.assistant_phone.as_deref().unwrap_or_default().trim()
        && existing.organization_name
            == if input.organization_name.trim().is_empty() {
                input.team.trim()
            } else {
                input.organization_name.trim()
            }
        && existing.job_title == input.job_title.trim()
        && existing.raw_vcard == input.raw_vcard
        && existing.source.import_source
            == if input.source.import_source.trim().is_empty() {
                "local"
            } else {
                input.source.import_source.trim()
            }
        && existing.source.source_uid == input.source.source_uid
        && existing.source.source_etag == input.source.source_etag
        && existing.source.source_payload_json == *source_payload_json
}

fn event_update_is_unchanged(
    existing: &ClientEvent,
    input: &UpsertClientEventInput,
    event_id: Uuid,
) -> bool {
    existing.uid
        == if input.uid.trim().is_empty() {
            event_id.to_string()
        } else {
            input.uid.trim().to_string()
        }
        && existing.date == input.date.trim()
        && existing.time == input.time.trim()
        && existing.time_zone == input.time_zone.trim()
        && existing.duration_minutes == input.duration_minutes.max(0)
        && existing.all_day == input.all_day
        && existing.status
            == if input.status.trim().is_empty() {
                "confirmed"
            } else {
                input.status.trim()
            }
        && existing.sequence == input.sequence.max(0)
        && existing.recurrence_rule == input.recurrence_rule.trim()
        && json_text_matches(&existing.recurrence_json, input.recurrence_json.trim())
        && json_text_matches(
            &existing.recurrence_exceptions_json,
            input.recurrence_exceptions_json.trim(),
        )
        && existing.title == input.title.trim()
        && existing.location == input.location.trim()
        && json_text_matches(&existing.organizer_json, input.organizer_json.trim())
        && existing.attendees == input.attendees.trim()
        && json_text_matches(&existing.attendees_json, input.attendees_json.trim())
        && existing.notes == input.notes.trim()
        && existing.body_html == input.body_html.trim()
}

fn json_text_matches(existing: &str, candidate: &str) -> bool {
    let candidate = if candidate.is_empty() {
        if existing.trim_start().starts_with('[') {
            "[]"
        } else {
            "{}"
        }
    } else {
        candidate
    };
    serde_json::from_str::<Value>(existing).ok() == serde_json::from_str(candidate).ok()
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
    if input.photo_data.is_none() {
        input.photo_data = Some(existing.photo_data.clone());
    }
    if input.photo_content_type.is_none() {
        input.photo_content_type = Some(existing.photo_content_type.clone());
    }
    if input.categories_json.is_none() {
        input.categories_json = Some(existing.categories_json.clone());
    }
    if input.birthday.is_none() {
        input.birthday = Some(existing.birthday.clone());
    }
    if input.anniversary.is_none() {
        input.anniversary = Some(existing.anniversary.clone());
    }
    if input.children_json.is_none() {
        input.children_json = Some(existing.children_json.clone());
    }
    if input.spouse.is_none() {
        input.spouse = Some(existing.spouse.clone());
    }
    if input.assistant_name.is_none() {
        input.assistant_name = Some(existing.assistant_name.clone());
    }
    if input.assistant_phone.is_none() {
        input.assistant_phone = Some(existing.assistant_phone.clone());
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

#[cfg(test)]
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

fn map_event(row: ClientEventRow) -> ClientEvent {
    ClientEvent {
        id: row.id,
        uid: external_calendar_uid(&row.uid),
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
        photo_data: row.photo_data,
        photo_content_type: row.photo_content_type,
        categories_json: row.categories_json,
        birthday: row.birthday,
        anniversary: row.anniversary,
        children_json: row.children_json,
        spouse: row.spouse,
        assistant_name: row.assistant_name,
        assistant_phone: row.assistant_phone,
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
        client_folder, json_text_matches, merge_contact_update_input, ClientContact, ClientEvent,
        ContactSourceFields, UpsertClientContactInput, Value,
    };
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn client_folder_preserves_trash_role() {
        assert_eq!(client_folder("trash"), "trash");
        assert_eq!(client_folder("unknown"), "inbox");
    }

    #[test]
    fn workspace_contact_and_event_json_use_client_camel_case_contracts() {
        let contact = ClientContact {
            id: Uuid::from_u128(1),
            address_book_id: "default".to_string(),
            ..ClientContact::default()
        };
        let event = ClientEvent {
            id: Uuid::from_u128(2),
            uid: "event-1".to_string(),
            date: "2026-08-07".to_string(),
            time: "09:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 60,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 1,
            recurrence_rule: "".to_string(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Planning".to_string(),
            location: "Room 1".to_string(),
            organizer_json: "{}".to_string(),
            attendees: "".to_string(),
            attendees_json: "[]".to_string(),
            notes: "".to_string(),
            body_html: "".to_string(),
        };

        let contact = serde_json::to_value(contact).unwrap();
        let event = serde_json::to_value(event).unwrap();
        assert_eq!(contact["addressBookId"], "default");
        assert!(contact.get("address_book_id").is_none());
        assert_eq!(event["timeZone"], "Europe/Berlin");
        assert!(event.get("time_zone").is_none());
    }

    #[test]
    fn canonical_event_json_comparison_ignores_whitespace() {
        assert!(json_text_matches(
            r#"{"weekday":"monday"}"#,
            r#"{ "weekday": "monday" }"#
        ));
        assert!(json_text_matches("[]", ""));
        assert!(!json_text_matches(
            r#"{"weekday":"monday"}"#,
            r#"{"weekday":"tuesday"}"#
        ));
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
