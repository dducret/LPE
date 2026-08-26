mod core_update;
mod custom_properties;
mod imported_identity;
mod submission_placeholder;

use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    mapi_store_identity::{
        MAPI_FIRST_GLOBAL_COUNTER, MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
    normalize_calendar_meeting_uid, AccessibleEvent, CalendarEventAttachment,
    CanonicalChangeCategory, CollaborationRights, MapiEventAttachmentChanges, Storage,
    UpsertClientEventInput,
};
use core_update::{
    ensure_mapi_event_uid_is_not_hidden_in_tx, lock_incoming_mapi_event_uid_in_tx,
    update_mapi_event_core_in_tx,
};
use custom_properties::{
    apply_mapi_event_custom_properties_in_tx, fetch_mapi_event_search_key_in_tx,
    mapi_event_search_key, PID_TAG_SEARCH_KEY,
};
use imported_identity::{
    allocate_mapi_event_identity_in_tx, rotate_active_mapi_event_identities_in_tx,
    rotate_mapi_event_identities_in_tx, validate_imported_identity,
};
use submission_placeholder::insert_mapi_event_in_tx;
use submission_placeholder::try_adopt_mapi_submission_placeholder_in_tx;
pub(crate) use submission_placeholder::{
    lock_calendar_event_uid_in_tx, CALENDAR_EVENT_PROJECTION_MAPI_SUBMISSION_PLACEHOLDER,
};

pub(crate) const MAX_MAPI_GLOBAL_COUNTER: u64 = MAPI_MAX_GLOBAL_COUNTER;
pub(crate) const FIRST_RESERVED_HIGH_GLOBAL_COUNTER: u64 = MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER;
pub(crate) const FIRST_DYNAMIC_MAPI_GLOBAL_COUNTER: u64 = MAPI_FIRST_GLOBAL_COUNTER;

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
    pub imported_identity: Option<MapiEventImportedIdentity>,
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
    pub imported_identity: Option<MapiEventImportedIdentity>,
    pub reminder: MapiEventReminderPatch,
    pub custom_property_upserts: Vec<MapiEventCustomPropertyValue>,
    pub attachment_changes: MapiEventAttachmentChanges,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventImportedIdentity {
    pub source_key: Vec<u8>,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
    pub last_modification_time: u64,
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
    /// Initial imported 16-byte Calendar SearchKey; absent for web-native events.
    pub search_key: Option<Vec<u8>>,
    pub change_key: Vec<u8>,
    pub predecessor_change_list: Vec<u8>,
    /// Durable MAPI version time from mapi_object_identities.updated_at.
    pub last_modification_time: u64,
    pub created_at: String,
    /// Canonical Event commit time, projected as PidTagLocalCommitTime.
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapiEventCommitSuccess {
    pub mapi_object_id: u64,
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
    mapi_object_id: u64,
    retired_mapi_object_id: Option<u64>,
    change_number: u64,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
    last_modification_time: u64,
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
              AND projection_state = 'visible'
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
        let requested_event_id = input.event.id.unwrap_or_else(Uuid::new_v4);
        let event_uid = if input.event.uid.trim().is_empty() {
            requested_event_id.to_string()
        } else {
            normalize_calendar_meeting_uid(&input.event.uid)
        };
        let mut tx = self.pool.begin().await?;
        lock_calendar_event_uid_in_tx(&mut tx, &tenant_id, owner_account_id, &event_uid).await?;
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

        let adopted_event_id = try_adopt_mapi_submission_placeholder_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            calendar_id,
            &event_uid,
            &input,
        )
        .await?;
        let event_id = adopted_event_id.unwrap_or(requested_event_id);
        if adopted_event_id.is_none() {
            insert_mapi_event_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                calendar_id,
                event_id,
                &event_uid,
                &input,
            )
            .await?;
        }
        update_mapi_event_reminder_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
            &input.reminder,
        )
        .await?;
        let allow_initial_search_key = input.imported_identity.is_some();
        apply_mapi_event_custom_properties_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            event_id,
            &input.custom_property_upserts,
            &[],
            allow_initial_search_key,
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
            input.imported_identity.as_ref(),
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
                "adoptedSubmissionPlaceholder": adopted_event_id.is_some(),
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
        let (created_at, updated_at) =
            fetch_event_timestamps_in_tx(&mut tx, &tenant_id, event_id).await?;
        let version = MapiEventVersion {
            event_id,
            canonical_modseq: modseq,
            change_number: identity_version.change_number,
            search_key: allow_initial_search_key
                .then(|| mapi_event_search_key(&input.custom_property_upserts))
                .flatten(),
            change_key: identity_version.change_key,
            predecessor_change_list: identity_version.predecessor_change_list,
            last_modification_time: identity_version.last_modification_time,
            created_at,
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
                CASE WHEN octet_length(search_key.property_value) = 18
                           AND get_byte(search_key.property_value, 0) = 16
                           AND get_byte(search_key.property_value, 1) = 0
                     THEN substring(search_key.property_value FROM 3 FOR 16)
                END AS search_key,
                identity.change_key,
                identity.predecessor_change_list,
                (EXTRACT(EPOCH FROM (
                    identity.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'
                )) * 10000000)::bigint AS last_modification_time,
                to_char(
                    event.created_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) AS created_at,
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
            LEFT JOIN mapi_custom_property_values search_key
              ON search_key.tenant_id = event.tenant_id
             AND search_key.account_id = event.owner_account_id
             AND search_key.object_kind = 'calendar_event'
             AND search_key.canonical_id = event.id
             AND search_key.property_tag = 806027522
             AND search_key.property_type = 258
            WHERE event.tenant_id = $1
              AND event.id = ANY($3)
              AND event.projection_state = 'visible'
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
        let incoming_event_uid = lock_incoming_mapi_event_uid_in_tx(
            &mut tx,
            &tenant_id,
            input.event_id,
            input.event.as_ref(),
        )
        .await?;
        let event = sqlx::query(
            r#"
            SELECT
                event.owner_account_id,
                event.calendar_id,
                event.uid,
                event.modseq,
                event.lifecycle_state,
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
              AND event.lifecycle_state IN ('active', 'deleted')
              AND event.projection_state = 'visible'
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
        let lifecycle_state = event.get::<String, _>("lifecycle_state");
        let object_kind = mapi_event_identity_object_kind(&lifecycle_state)?;
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
            let incoming_event_uid = incoming_event_uid
                .as_deref()
                .expect("an Event update has a normalized incoming UID");
            ensure_mapi_event_uid_is_not_hidden_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                incoming_event_uid,
            )
            .await?;
            update_mapi_event_core_in_tx(&mut tx, &tenant_id, event_input).await?;
        }
        let committed_event_uid = incoming_event_uid.as_deref().unwrap_or(&event_uid);
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
            false,
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
            .advance_mapi_event_version_for_lifecycle_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                input.event_id,
                modseq,
                &lifecycle_state,
                Some(input.principal_account_id),
                input.imported_identity.as_ref(),
            )
            .await?;
        let principal_version = versions
            .into_iter()
            .find(|version| version.account_id == input.principal_account_id)
            .ok_or_else(|| anyhow!("MAPI Event identity is missing for the principal"))?;

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
            object_kind,
            input.event_id,
            "updated",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": committed_event_uid,
                "coreChanged": input.event.is_some(),
                "reminderChanged": reminder_patch_has_changes(&input.reminder),
                "customPropertiesChanged": !input.custom_property_upserts.is_empty()
                    || !input.custom_property_deletes.is_empty(),
                "attachmentChanged": !input.attachment_changes.upserts.is_empty()
                    || !input.attachment_changes.delete_attachment_ids.is_empty(),
                "mapiChangeNumber": principal_version.change_number,
                "mapiIdentityAccountId": input.principal_account_id,
                "oldMapiObjectId": principal_version.retired_mapi_object_id,
                "newMapiObjectId": principal_version.mapi_object_id,
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
        let (created_at, updated_at) =
            fetch_event_timestamps_in_tx(&mut tx, &tenant_id, input.event_id).await?;
        let search_key = fetch_mapi_event_search_key_in_tx(
            &mut tx,
            &tenant_id,
            owner_account_id,
            input.event_id,
        )
        .await?;
        tx.commit().await?;

        Ok(MapiEventCommitOutcome::Saved(MapiEventCommitSuccess {
            mapi_object_id: principal_version.mapi_object_id,
            version: MapiEventVersion {
                event_id: input.event_id,
                canonical_modseq: modseq,
                change_number: principal_version.change_number,
                search_key,
                change_key: principal_version.change_key,
                predecessor_change_list: principal_version.predecessor_change_list,
                last_modification_time: principal_version.last_modification_time,
                created_at,
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
        self.advance_mapi_event_version_for_lifecycle_in_tx(
            tx,
            tenant_id,
            owner_account_id,
            event_id,
            modseq,
            "active",
            None,
            None,
        )
        .await
    }

    async fn advance_mapi_event_version_for_lifecycle_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        event_id: Uuid,
        modseq: i64,
        lifecycle_state: &str,
        imported_principal_account_id: Option<Uuid>,
        imported_identity: Option<&MapiEventImportedIdentity>,
    ) -> Result<Vec<EventIdentityVersion>> {
        let object_kind = mapi_event_identity_object_kind(lifecycle_state)?;
        let calendar_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            UPDATE calendar_events
            SET modseq = $4,
                updated_at = GREATEST(clock_timestamp(), updated_at + INTERVAL '1 microsecond')
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND lifecycle_state = $5
            RETURNING calendar_id
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(event_id)
        .bind(modseq)
        .bind(lifecycle_state)
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
        if lifecycle_state == "active" && imported_identity.is_none() {
            rotate_active_mapi_event_identities_in_tx(tx, tenant_id, event_id).await
        } else {
            rotate_mapi_event_identities_in_tx(
                tx,
                tenant_id,
                event_id,
                object_kind,
                imported_principal_account_id,
                imported_identity,
            )
            .await
        }
    }
}

fn validate_mapi_event_create_input(input: &MapiEventCreateInput) -> Result<()> {
    validate_mapi_event_fields(&input.event)?;
    if let Some(identity) = input.imported_identity.as_ref() {
        validate_imported_identity(identity)?;
    }
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
    if let Some(identity) = input.imported_identity.as_ref() {
        validate_imported_identity(identity)?;
    }
    validate_mapi_event_reminder(&input.reminder)?;
    validate_mapi_event_custom_properties(
        &input.custom_property_upserts,
        &input.custom_property_deletes,
    )?;
    crate::attachments::validate_mapi_event_attachment_changes(&input.attachment_changes)
}

fn mapi_event_identity_object_kind(lifecycle_state: &str) -> Result<&'static str> {
    match lifecycle_state {
        "active" => Ok("calendar_event"),
        "deleted" => Ok("deleted_calendar_event"),
        _ => bail!("unsupported MAPI Event lifecycle state"),
    }
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
        if value.property_tag == PID_TAG_SEARCH_KEY
            && mapi_event_search_key(std::slice::from_ref(value)).is_none()
        {
            bail!("MAPI Event SearchKey must be a 16-byte binary value");
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
          AND lifecycle_state IN ('active', 'deleted')
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
          AND projection_state = 'visible'
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
          AND projection_state = 'visible'
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
          AND lifecycle_state IN ('active', 'deleted')
          AND projection_state = 'visible'
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
              AND event.lifecycle_state IN ('active', 'deleted')
              AND event.projection_state = 'visible'
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

async fn fetch_event_timestamps_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    event_id: Uuid,
) -> Result<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT
            to_char(
                created_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ),
            to_char(
                updated_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            )
        FROM calendar_events
        WHERE tenant_id = $1
          AND id = $2
          AND lifecycle_state IN ('active', 'deleted')
          AND projection_state = 'visible'
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
    let last_modification_time = row.get::<i64, _>("last_modification_time");
    if last_modification_time < 0 {
        bail!("stored MAPI Event LastModificationTime is invalid");
    }
    Ok(MapiEventVersion {
        event_id: row.get("event_id"),
        canonical_modseq: row.get("modseq"),
        change_number: change_number as u64,
        search_key: row.get("search_key"),
        change_key: row.get("change_key"),
        predecessor_change_list: row.get("predecessor_change_list"),
        last_modification_time: last_modification_time as u64,
        created_at: row.get("created_at"),
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
