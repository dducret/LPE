use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    AccessibleEvent, CalendarEventAttachment, CanonicalChangeCategory, CollaborationRights,
    MapiEventAttachmentChanges, Storage, UpsertClientEventInput,
};

const MAX_MAPI_GLOBAL_COUNTER: u64 = 0x7FFF_FFFF_FFFF;
const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = 0x7FFF_FE00_0000;
const FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER: u64 = 43;
const MAPI_STORE_REPLICA_GUID: [u8; 16] = [
    0x74, 0x1f, 0x6f, 0xd3, 0x8e, 0x1a, 0x65, 0x4f, 0x9d, 0x42, 0x2d, 0xfb, 0x45, 0x1c, 0x8f, 0x10,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapiEventReminderPatch {
    pub reminder_set: Option<bool>,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventCustomPropertyValue {
    pub property_tag: u32,
    pub property_type: u16,
    pub property_value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MapiEventCommitInput {
    pub principal_account_id: Uuid,
    pub event_id: Uuid,
    pub expected_modseq: i64,
    pub force_save: bool,
    pub event: Option<UpsertClientEventInput>,
    pub reminder: MapiEventReminderPatch,
    pub custom_property_upserts: Vec<MapiEventCustomPropertyValue>,
    pub custom_property_deletes: Vec<u32>,
    pub attachment_changes: MapiEventAttachmentChanges,
}

#[derive(Debug, Clone)]
pub struct MapiEventCreateInput {
    pub principal_account_id: Uuid,
    pub collection_id: String,
    pub event: UpsertClientEventInput,
    pub reminder: MapiEventReminderPatch,
    pub custom_property_upserts: Vec<MapiEventCustomPropertyValue>,
    pub attachment_changes: MapiEventAttachmentChanges,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MapiEventReminderState {
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
}

// [MS-OXCFXICS] sections 2.2.1.2.7, 2.2.1.2.8, and 3.1.5.3:
// the canonical CAS token and the MAPI replica version are distinct durable values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventVersion {
    pub event_id: Uuid,
    pub canonical_modseq: i64,
    /// The 48-bit GLOBCNT. The Exchange adapter projects the wire CN with ReplId 1.
    pub change_number: u64,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventCommitSuccess {
    pub version: MapiEventVersion,
    pub reminder: MapiEventReminderState,
    pub attachments: Vec<CalendarEventAttachment>,
}

#[derive(Debug, Clone)]
pub struct MapiEventCreateResult {
    pub event: AccessibleEvent,
    pub mapi_object_id: u64,
    pub version: MapiEventVersion,
    pub reminder: MapiEventReminderState,
    pub attachments: Vec<CalendarEventAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapiEventCommitOutcome {
    Saved(MapiEventCommitSuccess),
    ObjectModified { current_modseq: i64 },
    NotFound,
    AccessDenied,
}

#[derive(Debug)]
pub(crate) struct EventIdentityVersion {
    account_id: Uuid,
    change_number: u64,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
}

impl Storage {
    pub(crate) async fn move_calendar_events_to_collection_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        source_calendar_id: Uuid,
        destination_calendar_id: Uuid,
    ) -> Result<()> {
        let moved_events = sqlx::query(
            r#"
            SELECT id, uid
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND calendar_id = $3
              AND lifecycle_state = 'active'
            ORDER BY id
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(source_calendar_id)
        .fetch_all(&mut **tx)
        .await?;
        let mut affected_principals = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM calendar_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND calendar_id = $3
              AND may_read
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(source_calendar_id)
        .fetch_all(&mut **tx)
        .await?;
        affected_principals.push(owner_account_id);
        affected_principals.sort();
        affected_principals.dedup();
        sqlx::query(
            r#"
            UPDATE calendar_events
            SET calendar_id = $4, updated_at = NOW()
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND calendar_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(source_calendar_id)
        .bind(destination_calendar_id)
        .execute(&mut **tx)
        .await?;
        for event in moved_events {
            let event_id = event.get::<Uuid, _>("id");
            let event_uid = event.get::<String, _>("uid");
            let event_modseq = self
                .allocate_account_modseq_in_tx(
                    tx,
                    tenant_id,
                    owner_account_id,
                    CanonicalChangeCategory::Calendar.as_str(),
                )
                .await?;
            self.advance_calendar_event_version_in_tx(
                tx,
                tenant_id,
                owner_account_id,
                event_id,
                event_modseq,
            )
            .await?;
            Self::insert_mail_change_log_in_tx(
                tx,
                tenant_id,
                Some(owner_account_id),
                None,
                "calendar_event",
                event_id,
                "moved",
                event_modseq,
                &affected_principals,
                serde_json::json!({
                    "collectionId": destination_calendar_id,
                    "oldCollectionId": source_calendar_id,
                    "objectUid": event_uid,
                }),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn create_mapi_event(
        &self,
        input: MapiEventCreateInput,
    ) -> Result<MapiEventCreateResult> {
        validate_mapi_event_create_input(&input)?;
        let tenant_id = self
            .tenant_id_for_account_id(input.principal_account_id)
            .await?;
        let collection_id = input.collection_id.trim();
        let collection = self
            .fetch_accessible_calendar_collections(input.principal_account_id)
            .await?
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .ok_or_else(|| anyhow!("calendar collection is not accessible"))?;
        if !collection.rights.may_write {
            bail!("write access is not granted on this calendar");
        }

        let owner_account_id = collection.owner_account_id;
        let event_id = input.event.id.unwrap_or_else(Uuid::new_v4);
        let event_uid = if input.event.uid.trim().is_empty() {
            event_id.to_string()
        } else {
            input.event.uid.trim().to_string()
        };
        let mut tx = self.pool.begin().await?;
        let calendar_id = match Uuid::parse_str(&collection.id) {
            Ok(calendar_id) => calendar_id,
            Err(_) => {
                Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, owner_account_id).await?
            }
        };
        let may_write = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT CASE
                WHEN calendar.owner_account_id = $4 THEN TRUE
                ELSE COALESCE(grant_row.may_write, FALSE)
            END
            FROM calendars calendar
            LEFT JOIN calendar_grants grant_row
              ON grant_row.tenant_id = calendar.tenant_id
             AND grant_row.owner_account_id = calendar.owner_account_id
             AND grant_row.calendar_id = calendar.id
             AND grant_row.grantee_account_id = $4
            WHERE calendar.tenant_id = $1
              AND calendar.owner_account_id = $2
              AND calendar.id = $3
            FOR SHARE OF calendar
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(input.principal_account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("calendar collection disappeared before Event create"))?;
        if !may_write {
            bail!("write access is not granted on this calendar");
        }

        sqlx::query(
            r#"
            INSERT INTO calendar_events (
                id, tenant_id, owner_account_id, calendar_id, uid,
                starts_at, ends_at, time_zone, all_day, status, sequence,
                recurrence_rule, recurrence_json, recurrence_exceptions_json,
                title, location, organizer_json, attendees_json, body_text, body_html,
                import_source, source_payload_json
            )
            VALUES (
                $1, $2, $3, $4, $5,
                (($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC')),
                ((($6::date + $7::time) AT TIME ZONE COALESCE(NULLIF($8, ''), 'UTC'))
                    + make_interval(mins => GREATEST($9, 0))),
                $8,
                $10,
                COALESCE(NULLIF($11, ''), 'confirmed'),
                GREATEST($12, 0),
                NULLIF($13, ''),
                CASE WHEN NULLIF($14, '') IS NULL THEN '{}'::jsonb ELSE $14::jsonb END,
                CASE WHEN NULLIF($15, '') IS NULL THEN '[]'::jsonb ELSE $15::jsonb END,
                $16,
                $17,
                CASE WHEN NULLIF($18, '') IS NULL THEN '{}'::jsonb ELSE $18::jsonb END,
                CASE
                    WHEN NULLIF($20, '') IS NOT NULL THEN $20::jsonb
                    WHEN NULLIF($19, '') IS NOT NULL THEN
                        jsonb_build_object(
                            'attendees',
                            jsonb_build_array(jsonb_build_object('email', $19::text))
                        )
                    ELSE '{}'::jsonb
                END,
                $21,
                NULLIF($22, ''),
                'mapi',
                jsonb_build_object('attendees', $19::text)
            )
            "#,
        )
        .bind(event_id)
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(&event_uid)
        .bind(input.event.date.trim())
        .bind(input.event.time.trim())
        .bind(input.event.time_zone.trim())
        .bind(input.event.duration_minutes.max(0))
        .bind(input.event.all_day)
        .bind(input.event.status.trim())
        .bind(input.event.sequence)
        .bind(input.event.recurrence_rule.trim())
        .bind(input.event.recurrence_json.trim())
        .bind(input.event.recurrence_exceptions_json.trim())
        .bind(input.event.title.trim())
        .bind(input.event.location.trim())
        .bind(input.event.organizer_json.trim())
        .bind(input.event.attendees.trim())
        .bind(input.event.attendees_json.trim())
        .bind(input.event.notes.trim())
        .bind(input.event.body_html.trim())
        .execute(&mut *tx)
        .await?;
        update_mapi_event_reminder_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
            &input.reminder,
        )
        .await?;
        apply_mapi_event_custom_properties_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
            &input.custom_property_upserts,
            &[],
        )
        .await?;
        let attachments = self
            .apply_mapi_event_attachment_changes_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                calendar_id,
                event_id,
                &input.attachment_changes,
            )
            .await?;

        let (mapi_object_id, identity_version) = allocate_mapi_event_identity_in_tx(
            &mut tx,
            &tenant_id,
            input.principal_account_id,
            event_id,
        )
        .await?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        set_created_mapi_event_modseq_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            calendar_id,
            event_id,
            modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "calendar_event",
            event_id,
            "created",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": event_uid,
                "created": true,
                "reminderChanged": reminder_patch_has_changes(&input.reminder),
                "customPropertiesChanged": !input.custom_property_upserts.is_empty(),
                "attachmentChanged": !input.attachment_changes.upserts.is_empty(),
                "mapiChangeNumber": identity_version.change_number,
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            owner_account_id,
        )
        .await?;

        let event = fetch_created_accessible_event_in_tx(
            &mut tx,
            &tenant_id,
            event_id,
            collection.id,
            collection.owner_email,
            collection.owner_display_name,
            collection.rights,
        )
        .await?;
        let reminder = fetch_mapi_event_reminder_state_in_tx(&mut tx, &tenant_id, event_id).await?;
        let updated_at = fetch_event_updated_at_in_tx(&mut tx, &tenant_id, event_id).await?;
        let version = MapiEventVersion {
            event_id,
            canonical_modseq: modseq,
            change_number: identity_version.change_number,
            change_key: identity_version.change_key,
            predecessor_change_list: identity_version.predecessor_change_list,
            updated_at,
        };
        tx.commit().await?;

        Ok(MapiEventCreateResult {
            event,
            mapi_object_id,
            version,
            reminder,
            attachments,
        })
    }

    pub async fn fetch_mapi_event_versions(
        &self,
        principal_account_id: Uuid,
        event_ids: &[Uuid],
    ) -> Result<Vec<MapiEventVersion>> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                event.id AS event_id,
                event.modseq,
                identity.mapi_change_number,
                identity.change_key,
                identity.predecessor_change_list,
                to_char(
                    event.updated_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) AS updated_at
            FROM calendar_events event
            JOIN mapi_object_identities identity
              ON identity.tenant_id = event.tenant_id
             AND identity.account_id = $2
             AND (
                    (event.lifecycle_state = 'active'
                        AND identity.object_kind = 'calendar_event')
                    OR (event.lifecycle_state = 'deleted'
                        AND identity.object_kind = 'deleted_calendar_event')
             )
             AND identity.canonical_id = event.id
             AND identity.deleted_at IS NULL
            WHERE event.tenant_id = $1
              AND event.id = ANY($3)
              AND (
                    event.owner_account_id = $2
                    OR EXISTS (
                        SELECT 1
                        FROM calendar_grants grant_row
                        WHERE grant_row.tenant_id = event.tenant_id
                          AND grant_row.owner_account_id = event.owner_account_id
                          AND grant_row.calendar_id = event.calendar_id
                          AND grant_row.grantee_account_id = $2
                          AND grant_row.may_read
                    )
              )
            ORDER BY event.id
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(event_ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(mapi_event_version_from_row).collect()
    }

    pub async fn commit_mapi_event_update(
        &self,
        input: MapiEventCommitInput,
    ) -> Result<MapiEventCommitOutcome> {
        validate_mapi_event_commit_input(&input)?;
        let tenant_id = self
            .tenant_id_for_account_id(input.principal_account_id)
            .await?;
        let mut tx = self.pool.begin().await?;
        let event = sqlx::query(
            r#"
            SELECT
                event.owner_account_id,
                event.calendar_id,
                event.uid,
                event.modseq,
                (
                    event.owner_account_id = $3
                    OR EXISTS (
                        SELECT 1
                        FROM calendar_grants grant_row
                        WHERE grant_row.tenant_id = event.tenant_id
                          AND grant_row.owner_account_id = event.owner_account_id
                          AND grant_row.calendar_id = event.calendar_id
                          AND grant_row.grantee_account_id = $3
                          AND grant_row.may_write
                    )
                ) AS may_write
            FROM calendar_events event
            WHERE event.tenant_id = $1
              AND event.id = $2
              AND event.lifecycle_state = 'active'
            FOR UPDATE OF event
            "#,
        )
        .bind(tenant_id)
        .bind(input.event_id)
        .bind(input.principal_account_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(event) = event else {
            return Ok(MapiEventCommitOutcome::NotFound);
        };
        if !event.get::<bool, _>("may_write") {
            return Ok(MapiEventCommitOutcome::AccessDenied);
        }

        let owner_account_id = event.get::<Uuid, _>("owner_account_id");
        let calendar_id = event.get::<Uuid, _>("calendar_id");
        let event_uid = event.get::<String, _>("uid");
        let current_modseq = event.get::<i64, _>("modseq");
        // [MS-OXCMSG] section 3.2.5.3: independently opened handles conflict at Save,
        // while ForceSave bypasses only the object-modified check.
        if current_modseq != input.expected_modseq && !input.force_save {
            return Ok(MapiEventCommitOutcome::ObjectModified { current_modseq });
        }
        if let Some(event_input) = input.event.as_ref() {
            if event_input.id != Some(input.event_id) || event_input.account_id != owner_account_id
            {
                bail!("MAPI Event update target does not match the canonical Event owner");
            }
            update_mapi_event_core_in_tx(&mut tx, &tenant_id, event_input).await?;
        }
        update_mapi_event_reminder_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            input.event_id,
            &input.reminder,
        )
        .await?;
        apply_mapi_event_custom_properties_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            input.event_id,
            &input.custom_property_upserts,
            &input.custom_property_deletes,
        )
        .await?;
        let attachments = self
            .apply_mapi_event_attachment_changes_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                calendar_id,
                input.event_id,
                &input.attachment_changes,
            )
            .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        let versions = self
            .advance_calendar_event_version_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                input.event_id,
                modseq,
            )
            .await?;
        let principal_version = versions
            .into_iter()
            .find(|version| version.account_id == input.principal_account_id)
            .ok_or_else(|| anyhow!("active MAPI Event identity is missing for the principal"))?;

        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            input.event_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "calendar_event",
            input.event_id,
            "updated",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": event_uid,
                "coreChanged": input.event.is_some(),
                "reminderChanged": reminder_patch_has_changes(&input.reminder),
                "customPropertiesChanged": !input.custom_property_upserts.is_empty()
                    || !input.custom_property_deletes.is_empty(),
                "attachmentChanged": !input.attachment_changes.upserts.is_empty()
                    || !input.attachment_changes.delete_attachment_ids.is_empty(),
                "mapiChangeNumber": principal_version.change_number,
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            owner_account_id,
        )
        .await?;
        let reminder =
            fetch_mapi_event_reminder_state_in_tx(&mut tx, &tenant_id, input.event_id).await?;
        let updated_at = fetch_event_updated_at_in_tx(&mut tx, &tenant_id, input.event_id).await?;
        tx.commit().await?;

        Ok(MapiEventCommitOutcome::Saved(MapiEventCommitSuccess {
            version: MapiEventVersion {
                event_id: input.event_id,
                canonical_modseq: modseq,
                change_number: principal_version.change_number,
                change_key: principal_version.change_key,
                predecessor_change_list: principal_version.predecessor_change_list,
                updated_at,
            },
            reminder,
            attachments,
        }))
    }

    pub(crate) async fn advance_calendar_event_version_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        event_id: Uuid,
        modseq: i64,
    ) -> Result<Vec<EventIdentityVersion>> {
        let calendar_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            UPDATE calendar_events
            SET modseq = $4,
                updated_at = GREATEST(clock_timestamp(), updated_at + INTERVAL '1 microsecond')
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
            RETURNING calendar_id
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .bind(modseq)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("calendar Event not found while advancing its version"))?;
        sqlx::query(
            r#"
            UPDATE calendars
            SET sync_modseq = GREATEST(sync_modseq, $4),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(modseq)
        .execute(&mut **tx)
        .await?;
        rotate_active_mapi_event_identities_in_tx(tx, tenant_id, event_id).await
    }
}

fn validate_mapi_event_create_input(input: &MapiEventCreateInput) -> Result<()> {
    validate_mapi_event_fields(&input.event)?;
    validate_mapi_event_reminder(&input.reminder)?;
    validate_mapi_event_custom_properties(&input.custom_property_upserts, &[])?;
    crate::attachments::validate_mapi_event_attachment_changes(&input.attachment_changes)
}

fn validate_mapi_event_commit_input(input: &MapiEventCommitInput) -> Result<()> {
    if input.expected_modseq <= 0 {
        bail!("MAPI Event expected modseq must be positive");
    }
    if let Some(event) = input.event.as_ref() {
        validate_mapi_event_fields(event)?;
    }
    validate_mapi_event_reminder(&input.reminder)?;
    validate_mapi_event_custom_properties(
        &input.custom_property_upserts,
        &input.custom_property_deletes,
    )?;
    crate::attachments::validate_mapi_event_attachment_changes(&input.attachment_changes)
}

fn validate_mapi_event_fields(event: &UpsertClientEventInput) -> Result<()> {
    if event.date.trim().is_empty() || event.time.trim().is_empty() {
        bail!("event date and time are required");
    }
    Ok(())
}

fn validate_mapi_event_reminder(reminder: &MapiEventReminderPatch) -> Result<()> {
    if reminder.reminder_set == Some(true)
        && reminder
            .reminder_at
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
    {
        bail!("calendar reminder time is required when reminder is set");
    }

    Ok(())
}

fn validate_mapi_event_custom_properties(
    upserts: &[MapiEventCustomPropertyValue],
    deletes: &[u32],
) -> Result<()> {
    let mut upsert_tags = HashSet::new();
    for value in upserts {
        if value.property_type != (value.property_tag & 0xFFFF) as u16 {
            bail!("MAPI custom property type does not match its property tag");
        }
        if !upsert_tags.insert(value.property_tag) {
            bail!("MAPI custom property upserts contain a duplicate property tag");
        }
    }
    let mut delete_tags = HashSet::new();
    for tag in deletes {
        if !delete_tags.insert(*tag) {
            bail!("MAPI custom property deletes contain a duplicate property tag");
        }
        if upsert_tags.contains(tag) {
            bail!("MAPI custom property tag cannot be set and deleted in the same commit");
        }
    }
    Ok(())
}

async fn update_mapi_event_core_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    input: &UpsertClientEventInput,
) -> Result<()> {
    let event_id = input
        .id
        .ok_or_else(|| anyhow!("MAPI Event update requires a canonical Event id"))?;
    let updated = sqlx::query(
        r#"
        UPDATE calendar_events
        SET uid = COALESCE(NULLIF($4, ''), id::text),
            starts_at = (($5::date + $6::time) AT TIME ZONE COALESCE(NULLIF($7, ''), 'UTC')),
            ends_at = ((($5::date + $6::time) AT TIME ZONE COALESCE(NULLIF($7, ''), 'UTC'))
                + make_interval(mins => GREATEST($8, 0))),
            time_zone = $7,
            all_day = $9,
            status = COALESCE(NULLIF($10, ''), 'confirmed'),
            sequence = GREATEST($11, 0),
            recurrence_rule = NULLIF($12, ''),
            recurrence_json = CASE
                WHEN NULLIF($13, '') IS NOT NULL THEN $13::jsonb
                ELSE '{}'::jsonb
            END,
            recurrence_exceptions_json = CASE
                WHEN NULLIF($14, '') IS NOT NULL THEN $14::jsonb
                ELSE '[]'::jsonb
            END,
            title = $15,
            location = $16,
            organizer_json = CASE
                WHEN NULLIF($17, '') IS NOT NULL THEN $17::jsonb
                ELSE '{}'::jsonb
            END,
            attendees_json = CASE
                WHEN NULLIF($19, '') IS NOT NULL THEN $19::jsonb
                WHEN NULLIF($18, '') IS NOT NULL THEN
                    jsonb_build_object(
                        'attendees',
                        jsonb_build_array(jsonb_build_object('email', $18::text))
                    )
                ELSE '{}'::jsonb
            END,
            body_text = $20,
            body_html = NULLIF($21, ''),
            source_payload_json = jsonb_build_object('attendees', $18::text),
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(input.account_id)
    .bind(event_id)
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
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("canonical MAPI calendar Event was not updated");
    }
    Ok(())
}

async fn update_mapi_event_reminder_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    event_id: Uuid,
    reminder: &MapiEventReminderPatch,
) -> Result<()> {
    if !reminder_patch_has_changes(reminder) {
        return Ok(());
    }
    sqlx::query(
        r#"
        UPDATE calendar_events
        SET reminder_set = CASE
                WHEN $4::bool IS NULL THEN reminder_set
                ELSE $4
            END,
            reminder_at = CASE
                WHEN $4 = FALSE THEN NULL
                WHEN $5::text IS NOT NULL THEN NULLIF($5, '')::timestamptz
                ELSE reminder_at
            END,
            reminder_dismissed_at = CASE
                WHEN $4 = FALSE THEN NULL
                WHEN $6::text IS NOT NULL THEN NULLIF($6, '')::timestamptz
                WHEN $5::text IS NOT NULL THEN NULL
                ELSE reminder_dismissed_at
            END,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(event_id)
    .bind(reminder.reminder_set)
    .bind(reminder.reminder_at.as_deref())
    .bind(reminder.reminder_dismissed_at.as_deref())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn reminder_patch_has_changes(reminder: &MapiEventReminderPatch) -> bool {
    reminder.reminder_set.is_some()
        || reminder.reminder_at.is_some()
        || reminder.reminder_dismissed_at.is_some()
}

async fn apply_mapi_event_custom_properties_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    event_id: Uuid,
    upserts: &[MapiEventCustomPropertyValue],
    deletes: &[u32],
) -> Result<()> {
    let mut replaced_tags = deletes.iter().copied().collect::<Vec<_>>();
    replaced_tags.extend(upserts.iter().map(|value| value.property_tag));
    replaced_tags.sort_unstable();
    replaced_tags.dedup();
    if !replaced_tags.is_empty() {
        let replaced_tags = replaced_tags.into_iter().map(i64::from).collect::<Vec<_>>();
        sqlx::query(
            r#"
            DELETE FROM mapi_custom_property_values
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'calendar_event'
              AND canonical_id = $3
              AND property_tag = ANY($4)
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(event_id)
        .bind(&replaced_tags)
        .execute(&mut **tx)
        .await?;
    }
    for value in upserts {
        sqlx::query(
            r#"
            INSERT INTO mapi_custom_property_values (
                tenant_id,
                account_id,
                object_kind,
                canonical_id,
                property_tag,
                property_type,
                property_value
            )
            VALUES ($1, $2, 'calendar_event', $3, $4, $5, $6)
            "#,
        )
        .bind(tenant_id)
        .bind(principal_account_id)
        .bind(event_id)
        .bind(i64::from(value.property_tag))
        .bind(i32::from(value.property_type))
        .bind(&value.property_value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn allocate_mapi_event_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    principal_account_id: Uuid,
    event_id: Uuid,
) -> Result<(u64, EventIdentityVersion)> {
    sqlx::query(
        r#"
        INSERT INTO mapi_mailbox_replicas (
            tenant_id, account_id, replica_guid, next_global_counter
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET
            next_global_counter = GREATEST(
                mapi_mailbox_replicas.next_global_counter,
                $4
            )
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(Uuid::from_bytes(MAPI_STORE_REPLICA_GUID))
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas replica
        SET next_global_counter = GREATEST(
                replica.next_global_counter,
                COALESCE(
                    (
                        SELECT MAX(identity.mapi_global_counter) + 1
                        FROM mapi_object_identities identity
                        WHERE identity.tenant_id = replica.tenant_id
                          AND identity.account_id = replica.account_id
                          AND identity.mapi_global_counter < $3
                    ),
                    $4
                )
            ),
            updated_at = NOW()
        WHERE replica.tenant_id = $1
          AND replica.account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .execute(&mut **tx)
    .await?;
    let allocated = sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
          AND next_global_counter >= $3
          AND next_global_counter < $4
        RETURNING replica_guid, next_global_counter - 1 AS global_counter
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER as i64)
    .bind(FIRST_RESERVED_HIGH_GLOBAL_COUNTER as i64)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| anyhow!("MAPI dynamic global counter space exhausted"))?;
    let global_counter = allocated.get::<i64, _>("global_counter");
    if global_counter <= 0 || global_counter as u64 > MAX_MAPI_GLOBAL_COUNTER {
        bail!("MAPI dynamic global counter space exhausted");
    }
    let global_counter = global_counter as u64;
    let replica_guid = allocated.get::<Uuid, _>("replica_guid");
    let object_id = mapi_store_id(global_counter);
    let source_key = mapi_change_key(replica_guid, global_counter);
    let change_key = source_key.clone();
    let predecessor_change_list = merge_predecessor_change_list(&[], &change_key)?;
    sqlx::query(
        r#"
        INSERT INTO mapi_object_identities (
            tenant_id, account_id, object_kind, canonical_id,
            mapi_global_counter, mapi_object_id, source_key, change_key,
            instance_key, mapi_change_number, predecessor_change_list
        )
        VALUES ($1, $2, 'calendar_event', $3, $4, $5, $6, $7, $6, $4, $8)
        "#,
    )
    .bind(tenant_id)
    .bind(principal_account_id)
    .bind(event_id)
    .bind(global_counter as i64)
    .bind(object_id as i64)
    .bind(&source_key)
    .bind(&change_key)
    .bind(&predecessor_change_list)
    .execute(&mut **tx)
    .await?;
    Ok((
        object_id,
        EventIdentityVersion {
            account_id: principal_account_id,
            change_number: global_counter,
            change_key,
            predecessor_change_list,
        },
    ))
}

async fn set_created_mapi_event_modseq_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    event_id: Uuid,
    modseq: i64,
) -> Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE calendar_events
        SET modseq = $5,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND calendar_id = $3
          AND id = $4
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(calendar_id)
    .bind(event_id)
    .bind(modseq)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        bail!("canonical MAPI calendar Event disappeared before version assignment");
    }
    sqlx::query(
        r#"
        UPDATE calendars
        SET sync_modseq = GREATEST(sync_modseq, $4),
            updated_at = NOW()
        WHERE tenant_id = $1
          AND owner_account_id = $2
          AND id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(owner_account_id)
    .bind(calendar_id)
    .bind(modseq)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn fetch_created_accessible_event_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
    collection_id: String,
    owner_email: String,
    owner_display_name: String,
    rights: CollaborationRights,
) -> Result<AccessibleEvent> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            uid,
            owner_account_id,
            to_char(
                starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'),
                'YYYY-MM-DD'
            ) AS date,
            to_char(
                starts_at AT TIME ZONE COALESCE(NULLIF(time_zone, ''), 'UTC'),
                'HH24:MI'
            ) AS time,
            time_zone,
            GREATEST(0, EXTRACT(EPOCH FROM (ends_at - starts_at))::int / 60)
                AS duration_minutes,
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
          AND id = $2
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(AccessibleEvent {
        id: row.get("id"),
        uid: row.get("uid"),
        collection_id,
        owner_account_id: row.get("owner_account_id"),
        owner_email,
        owner_display_name,
        rights,
        date: row.get("date"),
        time: row.get("time"),
        time_zone: row.get("time_zone"),
        duration_minutes: row.get("duration_minutes"),
        all_day: row.get("all_day"),
        status: row.get("status"),
        sequence: row.get("sequence"),
        recurrence_rule: row.get("recurrence_rule"),
        recurrence_json: row.get("recurrence_json"),
        recurrence_exceptions_json: row.get("recurrence_exceptions_json"),
        title: row.get("title"),
        location: row.get("location"),
        organizer_json: row.get("organizer_json"),
        attendees: row.get("attendees"),
        attendees_json: row.get("attendees_json"),
        notes: row.get("notes"),
        body_html: row.get("body_html"),
    })
}

async fn fetch_mapi_event_reminder_state_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
) -> Result<MapiEventReminderState> {
    let row = sqlx::query(
        r#"
        SELECT
            reminder_set,
            CASE
                WHEN reminder_at IS NULL THEN NULL
                ELSE to_char(
                    reminder_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                )
            END AS reminder_at,
            CASE
                WHEN reminder_dismissed_at IS NULL THEN NULL
                ELSE to_char(
                    reminder_dismissed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                )
            END AS reminder_dismissed_at
        FROM calendar_events
        WHERE tenant_id = $1
          AND id = $2
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(MapiEventReminderState {
        reminder_set: row.get("reminder_set"),
        reminder_at: row.get("reminder_at"),
        reminder_dismissed_at: row.get("reminder_dismissed_at"),
    })
}

pub(crate) const fn mapi_store_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

async fn rotate_active_mapi_event_identities_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
) -> Result<Vec<EventIdentityVersion>> {
    let identities = sqlx::query(
        r#"
        SELECT account_id, mapi_change_number, predecessor_change_list
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND object_kind = 'calendar_event'
          AND canonical_id = $2
          AND deleted_at IS NULL
        ORDER BY account_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut versions = Vec::with_capacity(identities.len());
    for identity in identities {
        let account_id = identity.get::<Uuid, _>("account_id");
        let current_change_number = identity.get::<i64, _>("mapi_change_number");
        if current_change_number <= 0
            || current_change_number as u64 >= FIRST_RESERVED_HIGH_GLOBAL_COUNTER
        {
            bail!("stored MAPI Event change number is outside the dynamic GLOBCNT range");
        }
        let predecessor_change_list = identity.get::<Vec<u8>, _>("predecessor_change_list");
        let replica = sqlx::query(
            r#"
            UPDATE mapi_mailbox_replicas
            SET next_global_counter = next_global_counter + 1,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
            RETURNING replica_guid, next_global_counter - 1 AS change_number
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("MAPI mailbox replica is missing for an active Event identity"))?;
        let change_number = replica.get::<i64, _>("change_number");
        if change_number <= 0
            || change_number as u64 >= FIRST_RESERVED_HIGH_GLOBAL_COUNTER
            || change_number as u64 > MAX_MAPI_GLOBAL_COUNTER
        {
            bail!("MAPI dynamic global counter space exhausted");
        }
        let change_number = change_number as u64;
        let change_key = mapi_change_key(replica.get::<Uuid, _>("replica_guid"), change_number);
        let predecessor_change_list =
            merge_predecessor_change_list(&predecessor_change_list, &change_key)?;
        let updated = sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_change_number = $4,
                change_key = $5,
                predecessor_change_list = $6,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'calendar_event'
              AND canonical_id = $3
              AND deleted_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(event_id)
        .bind(change_number as i64)
        .bind(&change_key)
        .bind(&predecessor_change_list)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("active MAPI Event identity disappeared during version rotation");
        }
        versions.push(EventIdentityVersion {
            account_id,
            change_number,
            change_key,
            predecessor_change_list,
        });
    }
    Ok(versions)
}

impl Storage {
    pub(crate) async fn calendar_event_affected_principals_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<Vec<Uuid>> {
        let mut principals = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grant_row.grantee_account_id
            FROM calendar_events event
            JOIN calendar_grants grant_row
              ON grant_row.tenant_id = event.tenant_id
             AND grant_row.owner_account_id = event.owner_account_id
             AND grant_row.calendar_id = event.calendar_id
             AND grant_row.may_read
            WHERE event.tenant_id = $1
              AND event.owner_account_id = $2
              AND event.id = $3
              AND event.lifecycle_state = 'active'
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .fetch_all(&mut **tx)
        .await?;
        principals.push(owner_account_id);
        principals.sort();
        principals.dedup();
        Ok(principals)
    }
}

async fn fetch_event_updated_at_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
) -> Result<String> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT to_char(
            updated_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        FROM calendar_events
        WHERE tenant_id = $1
          AND id = $2
          AND lifecycle_state = 'active'
        "#,
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}

fn mapi_event_version_from_row(row: sqlx::postgres::PgRow) -> Result<MapiEventVersion> {
    let change_number = row.get::<i64, _>("mapi_change_number");
    if change_number <= 0 || change_number as u64 > MAX_MAPI_GLOBAL_COUNTER {
        bail!("stored MAPI Event change number is outside the GLOBCNT range");
    }
    Ok(MapiEventVersion {
        event_id: row.get("event_id"),
        canonical_modseq: row.get("modseq"),
        change_number: change_number as u64,
        change_key: row.get("change_key"),
        predecessor_change_list: row.get("predecessor_change_list"),
        updated_at: row.get("updated_at"),
    })
}

pub(crate) fn mapi_change_key(replica_guid: Uuid, change_number: u64) -> Vec<u8> {
    let mut value = replica_guid.as_bytes().to_vec();
    let bytes = change_number.to_be_bytes();
    value.extend_from_slice(&bytes[2..]);
    value
}

pub(crate) fn merge_predecessor_change_list(current: &[u8], change_key: &[u8]) -> Result<Vec<u8>> {
    // [MS-OXCFXICS] sections 2.2.2.3 and 3.1.5.6.1: serialize SizedXids
    // in GUID order and retain the greatest integrated LocalId for each replica.
    let mut entries = parse_predecessor_change_list(current)?;
    let (guid, local_id) = split_xid(change_key)?;
    match entries.get(&guid) {
        Some(existing) if existing.len() != local_id.len() => {
            bail!("MAPI PCL LocalIds for one replica have inconsistent lengths")
        }
        Some(existing) if existing.as_slice() >= local_id => {}
        _ => {
            entries.insert(guid, local_id.to_vec());
        }
    }
    serialize_predecessor_change_list(entries)
}

fn parse_predecessor_change_list(bytes: &[u8]) -> Result<BTreeMap<[u8; 16], Vec<u8>>> {
    let mut entries: BTreeMap<[u8; 16], Vec<u8>> = BTreeMap::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let size = usize::from(
            *bytes
                .get(offset)
                .ok_or_else(|| anyhow!("truncated MAPI PCL SizedXid"))?,
        );
        offset += 1;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| anyhow!("MAPI PCL SizedXid length overflow"))?;
        let xid = bytes
            .get(offset..end)
            .ok_or_else(|| anyhow!("truncated MAPI PCL XID"))?;
        offset = end;
        let (guid, local_id) = split_xid(xid)?;
        match entries.get(&guid) {
            Some(existing) if existing.len() != local_id.len() => {
                bail!("MAPI PCL LocalIds for one replica have inconsistent lengths")
            }
            Some(existing) if existing.as_slice() >= local_id => {}
            _ => {
                entries.insert(guid, local_id.to_vec());
            }
        }
    }
    Ok(entries)
}

fn split_xid(bytes: &[u8]) -> Result<([u8; 16], &[u8])> {
    if !(17..=24).contains(&bytes.len()) {
        bail!("MAPI XID length must be between 17 and 24 bytes");
    }
    let guid = bytes[..16]
        .try_into()
        .map_err(|_| anyhow!("MAPI XID replica GUID is malformed"))?;
    Ok((guid, &bytes[16..]))
}

fn serialize_predecessor_change_list(entries: BTreeMap<[u8; 16], Vec<u8>>) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    for (guid, local_id) in entries {
        let xid_size = guid.len() + local_id.len();
        let xid_size = u8::try_from(xid_size)
            .map_err(|_| anyhow!("MAPI PCL XID is too large to serialize"))?;
        result.push(xid_size);
        result.extend_from_slice(&guid);
        result.extend_from_slice(&local_id);
    }
    if result.is_empty() {
        bail!("MAPI PCL cannot be empty after a committed Event change");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcl_merge_keeps_the_latest_xid_per_replica_and_sorts_replicas() {
        let first_guid = Uuid::from_u128(1);
        let second_guid = Uuid::from_u128(2);
        let first_old = mapi_change_key(first_guid, 7);
        let first_new = mapi_change_key(first_guid, 9);
        let second = mapi_change_key(second_guid, 4);
        let mut pcl = vec![second.len() as u8];
        pcl.extend_from_slice(&second);
        pcl.push(first_old.len() as u8);
        pcl.extend_from_slice(&first_old);

        let merged = merge_predecessor_change_list(&pcl, &first_new).unwrap();
        let entries = parse_predecessor_change_list(&merged).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&first_guid.into_bytes()], first_new[16..]);
        assert_eq!(entries[&second_guid.into_bytes()], second[16..]);
        assert!(merged[1..17] < merged[24..40]);
    }
}
