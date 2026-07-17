use anyhow::{anyhow, bail, Result};
use uuid::Uuid;

use sqlx::{Postgres, Row};

use crate::{
    CanonicalChangeCategory, CollaborationCollectionRow, Storage, UpsertClientContactInput,
    UpsertClientEventInput, DEFAULT_COLLECTION_ID, DEFAULT_CONTACT_BOOK_ROLE,
    DEFAULT_TASK_LIST_ROLE, IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE,
    QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE, SUGGESTED_CONTACTS_COLLECTION_ID,
    SUGGESTED_CONTACTS_ROLE,
};

mod deleted_events;
mod grants;
mod types;

pub(crate) use types::validate_collaboration_rights;
pub use types::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights, ContactNameFields,
    ContactSourceFields, DelegateAccessObject, DelegateFreeBusyMessageObject, FreeBusyBlock,
    MapiEventIdentityMove, MapiEventImportedMoveIdentity, MoveAccessibleEventToDeletedItemsResult,
};

use types::{
    calendar_collection_id_for_event, collection_id_for_owner, contact_book_role_for_collection_id,
    delegate_freebusy_message_objects, merge_free_busy_rows, shared_collection_display_name,
    shared_collection_id_for_row,
};

impl Storage {
    pub async fn fetch_accessible_contact_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_collections(principal_account_id, CollaborationResourceKind::Contacts)
            .await
    }

    pub async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_collections(principal_account_id, CollaborationResourceKind::Calendar)
            .await
    }

    pub async fn fetch_delegate_access_objects(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<DelegateAccessObject>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                owner.id AS owner_account_id,
                owner.primary_email AS owner_email,
                grantee.id AS grantee_account_id,
                grantee.primary_email AS grantee_email,
                COALESCE(g.may_read, FALSE) AS may_read,
                COALESCE(g.may_write, FALSE) AS may_write,
                COALESCE(g.may_delete, FALSE) AS may_delete,
                EXISTS(
                    SELECT 1
                    FROM sender_rights sender
                    WHERE sender.tenant_id = c.tenant_id
                      AND sender.owner_account_id = c.owner_account_id
                      AND sender.grantee_account_id = $2
                      AND sender.sender_right = 'send_on_behalf'
                      AND sender.identity_id IS NULL
                ) AS may_send_on_behalf
            FROM calendar_grants g
            JOIN calendars c
             ON c.tenant_id = g.tenant_id
             AND c.owner_account_id = g.owner_account_id
             AND c.id = g.calendar_id
             AND c.role = 'calendar'
            JOIN accounts owner
              ON owner.tenant_id = g.tenant_id
             AND owner.id = g.owner_account_id
            JOIN accounts grantee
              ON grantee.tenant_id = g.tenant_id
             AND grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.grantee_account_id = $2
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let may_write = row.get::<bool, _>("may_write");
                let may_delete = row.get::<bool, _>("may_delete");
                let may_send_on_behalf = row.get::<bool, _>("may_send_on_behalf");
                DelegateAccessObject {
                    owner_account_id: row.get("owner_account_id"),
                    owner_email: row.get("owner_email"),
                    grantee_account_id: row.get("grantee_account_id"),
                    grantee_email: row.get("grantee_email"),
                    can_view_free_busy: true,
                    can_open_calendar: row.get("may_read"),
                    can_create_or_update_calendar_items: may_write,
                    can_delete_calendar_items: may_delete,
                    can_receive_meeting_objects: may_write && may_send_on_behalf,
                    can_send_on_behalf: may_send_on_behalf,
                }
            })
            .collect())
    }

    pub async fn fetch_free_busy_blocks(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Uuid,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<FreeBusyBlock>> {
        let principal_tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let owner = self
            .account_identity_for_id(owner_account_id)
            .await
            .map_err(|_| anyhow!("calendar owner not found"))?;
        let owner_tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        if principal_tenant_id != owner_tenant_id {
            bail!("free/busy is available only inside one tenant");
        }

        let can_read_details = principal_account_id == owner_account_id
            || self
                .fetch_accessible_calendar_collections(principal_account_id)
                .await?
                .into_iter()
                .any(|collection| collection.owner_account_id == owner_account_id);

        let rows = sqlx::query_as::<_, crate::FreeBusyEventRow>(
            r#"
            SELECT
                to_char(GREATEST(e.starts_at, $4::timestamptz) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS starts_at,
                to_char(LEAST(e.ends_at, $3::timestamptz) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ends_at,
                e.status
            FROM calendar_events e
            JOIN calendars c
              ON c.tenant_id = e.tenant_id
             AND c.owner_account_id = e.owner_account_id
             AND c.id = e.calendar_id
             AND c.role = 'calendar'
            WHERE e.tenant_id = $1
              AND e.owner_account_id = $2
              AND e.lifecycle_state = 'active'
              AND e.status <> 'cancelled'
              AND e.starts_at < $3::timestamptz
              AND e.ends_at > $4::timestamptz
            ORDER BY e.starts_at ASC, e.ends_at ASC, e.id ASC
            "#,
        )
        .bind(&principal_tenant_id)
        .bind(owner_account_id)
        .bind(starts_before)
        .bind(ends_after)
        .fetch_all(&self.pool)
        .await?;

        Ok(merge_free_busy_rows(
            rows,
            owner_account_id,
            owner.email,
            can_read_details,
        ))
    }

    pub async fn project_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Uuid,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        self.compute_delegate_freebusy_messages(
            principal_account_id,
            Some(owner_account_id),
            starts_before,
            ends_after,
        )
        .await
    }

    pub async fn fetch_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Option<Uuid>,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        self.compute_delegate_freebusy_messages(
            principal_account_id,
            owner_account_id,
            "9999-12-31T23:59:59Z",
            "1970-01-01T00:00:00Z",
        )
        .await
    }

    async fn compute_delegate_freebusy_messages(
        &self,
        principal_account_id: Uuid,
        owner_account_id: Option<Uuid>,
        starts_before: &str,
        ends_after: &str,
    ) -> Result<Vec<DelegateFreeBusyMessageObject>> {
        let delegate_objects = self
            .fetch_delegate_access_objects(principal_account_id)
            .await?;
        let mut messages = Vec::new();
        if let Some(owner_account_id) = owner_account_id {
            let delegate = delegate_objects
                .iter()
                .find(|object| object.owner_account_id == owner_account_id);
            let free_busy = self
                .fetch_free_busy_blocks(
                    principal_account_id,
                    owner_account_id,
                    starts_before,
                    ends_after,
                )
                .await?;
            messages.extend(delegate_freebusy_message_objects(
                principal_account_id,
                owner_account_id,
                delegate,
                free_busy,
            )?);
        } else {
            for delegate in &delegate_objects {
                let free_busy = self
                    .fetch_free_busy_blocks(
                        principal_account_id,
                        delegate.owner_account_id,
                        starts_before,
                        ends_after,
                    )
                    .await?;
                messages.extend(delegate_freebusy_message_objects(
                    principal_account_id,
                    delegate.owner_account_id,
                    Some(delegate),
                    free_busy,
                )?);
            }
        }
        messages.sort_by(|left, right| {
            left.owner_account_id
                .cmp(&right.owner_account_id)
                .then(left.message_kind.cmp(&right.message_kind))
                .then(left.starts_at.cmp(&right.starts_at))
                .then(left.id.cmp(&right.id))
        });
        Ok(messages)
    }

    pub async fn create_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        display_name: &str,
    ) -> Result<CollaborationCollection> {
        let display_name = display_name.trim();
        if display_name.is_empty() {
            bail!("calendar name is required");
        }
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let calendar_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO calendars (id, tenant_id, owner_account_id, display_name, role)
            VALUES ($1, $2, $3, $4, 'custom')
            "#,
        )
        .bind(calendar_id)
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(display_name)
        .execute(&mut *tx)
        .await?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                principal_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(principal_account_id),
            None,
            "calendar",
            calendar_id,
            "created",
            modseq,
            &[principal_account_id],
            serde_json::json!({}),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            principal_account_id,
        )
        .await?;
        tx.commit().await?;
        self.resolve_collection_access(
            principal_account_id,
            CollaborationResourceKind::Calendar,
            &calendar_id.to_string(),
        )
        .await
    }

    pub async fn update_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
        display_name: &str,
    ) -> Result<CollaborationCollection> {
        let calendar_id = Uuid::parse_str(collection_id.trim())
            .map_err(|_| anyhow!("default calendar cannot be renamed through Calendar/set"))?;
        let display_name = display_name.trim();
        if display_name.is_empty() {
            bail!("calendar name is required");
        }
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Calendar,
                collection_id,
            )
            .await?;
        if !access.is_owned || !access.rights.may_write {
            bail!("write access is not granted on this calendar");
        }
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            r#"
            UPDATE calendars
            SET display_name = $4, updated_at = NOW()
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND role = 'custom'
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(calendar_id)
        .bind(display_name)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() == 0 {
            bail!("calendar not found");
        }
        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                principal_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(principal_account_id),
            None,
            "calendar",
            calendar_id,
            "updated",
            modseq,
            &[principal_account_id],
            serde_json::json!({}),
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            principal_account_id,
        )
        .await?;
        tx.commit().await?;
        self.resolve_collection_access(
            principal_account_id,
            CollaborationResourceKind::Calendar,
            &calendar_id.to_string(),
        )
        .await
    }

    pub async fn delete_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<()> {
        let calendar_id = Uuid::parse_str(collection_id.trim())
            .map_err(|_| anyhow!("default calendar cannot be deleted through Calendar/set"))?;
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Calendar,
                collection_id,
            )
            .await?;
        if !access.is_owned || !access.rights.may_delete {
            bail!("delete access is not granted on this calendar");
        }
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let default_calendar_id =
            Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, principal_account_id).await?;
        self.move_calendar_events_to_collection_in_tx(
            &mut tx,
            &tenant_id,
            principal_account_id,
            calendar_id,
            default_calendar_id,
        )
        .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM calendars
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
              AND role = 'custom'
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(calendar_id)
        .execute(&mut *tx)
        .await?;
        if deleted.rows_affected() == 0 {
            bail!("calendar not found");
        }
        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            principal_account_id,
            None,
            "calendar",
            calendar_id,
            None,
            &[principal_account_id],
        )
        .await?;
        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            principal_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_accessible_task_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        let task_lists = self.fetch_task_lists(principal_account_id).await?;
        Ok(task_lists
            .into_iter()
            .map(|task_list| CollaborationCollection {
                id: task_collection_id_for_list(
                    principal_account_id,
                    task_list.owner_account_id,
                    task_list.id,
                    task_list.role.as_deref(),
                ),
                kind: CollaborationResourceKind::Tasks.as_str().to_string(),
                owner_account_id: task_list.owner_account_id,
                owner_email: task_list.owner_email.clone(),
                owner_display_name: task_list.owner_display_name.clone(),
                display_name: task_list.name.clone(),
                is_owned: task_list.is_owned,
                rights: task_list.rights.clone(),
            })
            .collect())
    }

    pub async fn fetch_accessible_task_list_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        Ok(self
            .fetch_accessible_task_collections(principal_account_id)
            .await?
            .into_iter()
            .filter(|collection| !collection.is_owned)
            .collect())
    }

    pub async fn fetch_accessible_contacts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts_internal(principal_account_id, None, None)
            .await
    }

    pub async fn fetch_accessible_contacts_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleContact>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.fetch_accessible_contacts_internal(principal_account_id, None, Some(ids))
            .await
    }

    pub async fn fetch_accessible_contacts_in_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts_internal(principal_account_id, Some(collection_id), None)
            .await
    }

    pub async fn create_accessible_contact(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Contacts,
                collection_id.unwrap_or(DEFAULT_COLLECTION_ID),
            )
            .await?;
        if !access.rights.may_write {
            bail!("write access is not granted on this address book");
        }
        let contact_book_role = contact_book_role_for_collection_id(collection_id);

        let contact = self
            .upsert_client_contact_in_book_role(
                UpsertClientContactInput {
                    id: input.id,
                    account_id: access.owner_account_id,
                    name: input.name,
                    role: input.role,
                    email: input.email,
                    phone: input.phone,
                    team: input.team,
                    notes: input.notes,
                    structured_name: input.structured_name,
                    emails_json: input.emails_json,
                    phones_json: input.phones_json,
                    addresses_json: input.addresses_json,
                    urls_json: input.urls_json,
                    organization_name: input.organization_name,
                    job_title: input.job_title,
                    raw_vcard: input.raw_vcard,
                    raw_vcard_is_explicit: input.raw_vcard_is_explicit,
                    source: input.source,
                    source_is_explicit: input.source_is_explicit,
                },
                contact_book_role,
            )
            .await?;

        self.fetch_accessible_contacts_by_ids(principal_account_id, &[contact.id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not visible after create"))
    }

    pub async fn update_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        let existing = self
            .fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not found"))?;
        if !existing.rights.may_write {
            bail!("write access is not granted on this address book");
        }

        let contact_book_role = contact_book_role_for_collection_id(Some(&existing.collection_id));
        self.upsert_client_contact_in_book_role(
            UpsertClientContactInput {
                id: Some(contact_id),
                account_id: existing.owner_account_id,
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
                structured_name: input.structured_name,
                emails_json: input.emails_json,
                phones_json: input.phones_json,
                addresses_json: input.addresses_json,
                urls_json: input.urls_json,
                organization_name: input.organization_name,
                job_title: input.job_title,
                raw_vcard: input.raw_vcard,
                raw_vcard_is_explicit: input.raw_vcard_is_explicit,
                source: input.source,
                source_is_explicit: input.source_is_explicit,
            },
            contact_book_role,
        )
        .await?;

        self.fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not visible after update"))
    }

    pub async fn delete_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> Result<()> {
        let existing = self
            .fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this address book");
        }

        self.delete_client_contact(existing.owner_account_id, contact_id)
            .await
    }

    pub async fn fetch_accessible_events(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(principal_account_id, None, None, "active")
            .await
    }

    pub async fn fetch_accessible_events_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.fetch_accessible_events_internal(principal_account_id, None, Some(ids), "active")
            .await
    }

    pub async fn fetch_accessible_events_in_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(
            principal_account_id,
            Some(collection_id),
            None,
            "active",
        )
        .await
    }

    pub async fn create_accessible_event(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Calendar,
                collection_id.unwrap_or(DEFAULT_COLLECTION_ID),
            )
            .await?;
        if !access.rights.may_write {
            bail!("write access is not granted on this calendar");
        }

        let calendar_id = Uuid::parse_str(&access.id).ok();
        let event = self
            .upsert_client_event_in_calendar(
                UpsertClientEventInput {
                    id: input.id,
                    account_id: access.owner_account_id,
                    uid: input.uid,
                    date: input.date,
                    time: input.time,
                    time_zone: input.time_zone,
                    duration_minutes: input.duration_minutes,
                    all_day: input.all_day,
                    status: input.status,
                    sequence: input.sequence,
                    recurrence_rule: input.recurrence_rule,
                    recurrence_json: input.recurrence_json,
                    recurrence_exceptions_json: input.recurrence_exceptions_json,
                    title: input.title,
                    location: input.location,
                    organizer_json: input.organizer_json,
                    attendees: input.attendees,
                    attendees_json: input.attendees_json,
                    notes: input.notes,
                    body_html: input.body_html,
                },
                calendar_id,
            )
            .await?;

        self.fetch_accessible_events_by_ids(principal_account_id, &[event.id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not visible after create"))
    }

    pub async fn update_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        let existing = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !existing.rights.may_write {
            bail!("write access is not granted on this calendar");
        }

        let calendar_id = Uuid::parse_str(&existing.collection_id).ok();
        self.upsert_client_event_in_calendar(
            UpsertClientEventInput {
                id: Some(event_id),
                account_id: existing.owner_account_id,
                uid: if input.uid.trim().is_empty() {
                    existing.uid.clone()
                } else {
                    input.uid
                },
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                all_day: input.all_day,
                status: input.status,
                sequence: input.sequence,
                recurrence_rule: input.recurrence_rule,
                recurrence_json: input.recurrence_json,
                recurrence_exceptions_json: input.recurrence_exceptions_json,
                title: input.title,
                location: input.location,
                organizer_json: input.organizer_json,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
                body_html: input.body_html,
            },
            calendar_id,
        )
        .await?;

        self.fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not visible after update"))
    }

    pub async fn update_accessible_event_reminder(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> Result<()> {
        let existing = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !existing.rights.may_write {
            bail!("write access is not granted on this calendar");
        }
        if reminder_set == Some(true) && reminder_at.as_deref().unwrap_or_default().is_empty() {
            bail!("calendar reminder time is required when reminder is set");
        }

        let tenant_id = self
            .tenant_id_for_account_id(existing.owner_account_id)
            .await?;
        let mut tx = self.pool.begin().await?;
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
        .bind(&tenant_id)
        .bind(existing.owner_account_id)
        .bind(event_id)
        .bind(reminder_set)
        .bind(reminder_at.as_deref())
        .bind(reminder_dismissed_at.as_deref())
        .execute(&mut *tx)
        .await?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                existing.owner_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        self.advance_calendar_event_version_in_tx(
            &mut tx,
            &tenant_id,
            existing.owner_account_id,
            event_id,
            modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            &mut tx,
            &tenant_id,
            existing.owner_account_id,
            event_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(existing.owner_account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": existing.collection_id,
                "objectUid": existing.uid,
                "reminderChanged": true,
            }),
        )
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            existing.owner_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<()> {
        let existing = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this calendar");
        }

        self.delete_client_event(existing.owner_account_id, event_id)
            .await
    }

    async fn fetch_accessible_collections(
        &self,
        principal_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Vec<CollaborationCollection>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let principal = self.account_identity_for_id(principal_account_id).await?;
        if kind == CollaborationResourceKind::Calendar {
            let mut tx = self.pool.begin().await?;
            Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, principal.id).await?;
            tx.commit().await?;
        }
        let mut collections = vec![CollaborationCollection {
            id: DEFAULT_COLLECTION_ID.to_string(),
            kind: kind.as_str().to_string(),
            owner_account_id: principal.id,
            owner_email: principal.email.clone(),
            owner_display_name: principal.display_name.clone(),
            display_name: kind.collection_label().to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        }];
        if kind == CollaborationResourceKind::Contacts {
            for (id, display_name) in [
                (SUGGESTED_CONTACTS_COLLECTION_ID, "Suggested Contacts"),
                (QUICK_CONTACTS_COLLECTION_ID, "Quick Contacts"),
                (IM_CONTACT_LIST_COLLECTION_ID, "IM Contact List"),
            ] {
                collections.push(CollaborationCollection {
                    id: id.to_string(),
                    kind: kind.as_str().to_string(),
                    owner_account_id: principal.id,
                    owner_email: principal.email.clone(),
                    owner_display_name: principal.display_name.clone(),
                    display_name: display_name.to_string(),
                    is_owned: true,
                    rights: CollaborationRights {
                        may_read: true,
                        may_write: true,
                        may_delete: true,
                        may_share: false,
                    },
                });
            }
        }
        if kind == CollaborationResourceKind::Calendar {
            let rows = sqlx::query(
                r#"
                SELECT id, display_name
                FROM calendars
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND role = 'custom'
                ORDER BY lower(display_name), id
                "#,
            )
            .bind(&tenant_id)
            .bind(principal.id)
            .fetch_all(&self.pool)
            .await?;
            for row in rows {
                collections.push(CollaborationCollection {
                    id: row.try_get::<Uuid, _>("id")?.to_string(),
                    kind: kind.as_str().to_string(),
                    owner_account_id: principal.id,
                    owner_email: principal.email.clone(),
                    owner_display_name: principal.display_name.clone(),
                    display_name: row.try_get("display_name")?,
                    is_owned: true,
                    rights: CollaborationRights {
                        may_read: true,
                        may_write: true,
                        may_delete: true,
                        may_share: true,
                    },
                });
            }
        }

        let rows = match kind {
            CollaborationResourceKind::Contacts => {
                sqlx::query_as::<_, CollaborationCollectionRow>(
                    r#"
                SELECT
                    b.id,
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
                    b.display_name,
                    b.role,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM contact_book_grants g
                JOIN contact_books b
                  ON b.tenant_id = g.tenant_id
                 AND b.owner_account_id = g.owner_account_id
                 AND b.id = g.contact_book_id
                 AND b.role = 'contacts'
                JOIN accounts owner ON owner.id = g.owner_account_id
                WHERE g.tenant_id = $1
                  AND g.grantee_account_id = $2
                  AND g.may_read = TRUE
                ORDER BY lower(owner.primary_email) ASC
                "#,
                )
                .bind(&tenant_id)
                .bind(principal_account_id)
                .fetch_all(&self.pool)
                .await?
            }
            CollaborationResourceKind::Calendar => {
                sqlx::query_as::<_, CollaborationCollectionRow>(
                    r#"
                SELECT
                    c.id,
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
                    c.display_name,
                    c.role,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM calendar_grants g
                JOIN calendars c
                 ON c.tenant_id = g.tenant_id
                 AND c.owner_account_id = g.owner_account_id
                 AND c.id = g.calendar_id
                JOIN accounts owner ON owner.id = g.owner_account_id
                WHERE g.tenant_id = $1
                  AND g.grantee_account_id = $2
                  AND g.may_read = TRUE
                ORDER BY lower(owner.primary_email) ASC
                "#,
                )
                .bind(&tenant_id)
                .bind(principal_account_id)
                .fetch_all(&self.pool)
                .await?
            }
            CollaborationResourceKind::Tasks => {
                sqlx::query_as::<_, CollaborationCollectionRow>(
                    r#"
                SELECT
                    l.id,
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
                    l.display_name,
                    l.role,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM task_list_grants g
                JOIN task_lists l
                  ON l.tenant_id = g.tenant_id
                 AND l.owner_account_id = g.owner_account_id
                 AND l.id = g.task_list_id
                 AND l.role = $3
                JOIN accounts owner ON owner.id = g.owner_account_id
                WHERE g.tenant_id = $1
                  AND g.grantee_account_id = $2
                  AND g.may_read = TRUE
                ORDER BY lower(owner.primary_email) ASC
                "#,
                )
                .bind(&tenant_id)
                .bind(principal_account_id)
                .bind(DEFAULT_TASK_LIST_ROLE)
                .fetch_all(&self.pool)
                .await?
            }
        };

        collections.extend(rows.into_iter().map(|row| CollaborationCollection {
            id: shared_collection_id_for_row(kind, &row),
            kind: kind.as_str().to_string(),
            owner_account_id: row.owner_account_id,
            owner_email: row.owner_email.clone(),
            owner_display_name: row.owner_display_name.clone(),
            display_name: if kind == CollaborationResourceKind::Calendar && row.role == "custom" {
                row.display_name.clone()
            } else {
                shared_collection_display_name(kind, &row.owner_display_name, &row.owner_email)
            },
            is_owned: false,
            rights: CollaborationRights {
                may_read: row.may_read,
                may_write: row.may_write,
                may_delete: row.may_delete,
                may_share: row.may_share,
            },
        }));

        Ok(collections)
    }

    async fn resolve_collection_access(
        &self,
        principal_account_id: Uuid,
        kind: CollaborationResourceKind,
        collection_id: &str,
    ) -> Result<CollaborationCollection> {
        let collection_id = collection_id.trim();
        let collections = self
            .fetch_accessible_collections(principal_account_id, kind)
            .await?;
        collections
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .ok_or_else(|| anyhow!("collection not found"))
    }

    async fn fetch_accessible_contacts_internal(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        ids: Option<&[Uuid]>,
    ) -> Result<Vec<AccessibleContact>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let collection_scope =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                let access = self
                    .resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Contacts,
                        collection_id,
                    )
                    .await?;
                Some((
                    access.owner_account_id,
                    contact_book_role_for_collection_id(Some(collection_id)).to_string(),
                ))
            } else {
                None
            };
        let owner_account_id = collection_scope
            .as_ref()
            .map(|(owner_account_id, _)| *owner_account_id);
        let contact_book_role = collection_scope.as_ref().map(|(_, role)| role.as_str());

        let rows = sqlx::query_as::<_, crate::AccessibleContactRow>(
            r#"
            SELECT
                c.id,
                c.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                b.role AS contact_book_role,
                CASE WHEN c.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN c.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN c.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN c.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                c.display_name AS name,
                c.role,
                COALESCE(c.emails_json->0->>'email', '') AS email,
                COALESCE(c.phones_json->0->>'phone', '') AS phone,
                c.organization_unit AS team,
                c.notes,
                c.name_prefix,
                c.given_name,
                c.middle_name,
                c.family_name,
                c.name_suffix,
                c.nickname,
                c.phonetic_given_name,
                c.phonetic_family_name,
                c.emails_json,
                c.phones_json,
                c.addresses_json,
                c.urls_json,
                c.organization_name,
                c.job_title,
                c.raw_vcard,
                c.import_source,
                c.source_uid,
                c.source_etag,
                c.source_payload_json
            FROM contacts c
            JOIN accounts owner ON owner.id = c.owner_account_id
            JOIN contact_books b
              ON b.tenant_id = c.tenant_id
             AND b.owner_account_id = c.owner_account_id
             AND b.id = c.contact_book_id
            LEFT JOIN contact_book_grants g
              ON g.tenant_id = c.tenant_id
             AND g.contact_book_id = b.id
             AND g.owner_account_id = c.owner_account_id
             AND g.grantee_account_id = $2
            WHERE c.tenant_id = $1
              AND (c.owner_account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR c.owner_account_id = $3)
              AND ($4::text IS NULL OR b.role = $4)
              AND ($5::uuid[] IS NULL OR c.id = ANY($5))
            ORDER BY lower(c.display_name) ASC, c.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
        .bind(contact_book_role)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessibleContact {
                id: row.id,
                collection_id: collection_id_for_owner(
                    CollaborationResourceKind::Contacts,
                    principal_account_id,
                    row.owner_account_id,
                    &row.contact_book_role,
                ),
                owner_account_id: row.owner_account_id,
                owner_email: row.owner_email,
                owner_display_name: row.owner_display_name,
                rights: CollaborationRights {
                    may_read: row.may_read,
                    may_write: row.may_write,
                    may_delete: row.may_delete,
                    may_share: row.may_share,
                },
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
            })
            .collect())
    }

    async fn fetch_accessible_events_internal(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        ids: Option<&[Uuid]>,
        lifecycle_state: &str,
    ) -> Result<Vec<AccessibleEvent>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let collection_scope =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                let access = self
                    .resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Calendar,
                        collection_id,
                    )
                    .await?;
                Some((
                    access.owner_account_id,
                    Uuid::parse_str(collection_id).ok(),
                    if Uuid::parse_str(collection_id).is_ok() {
                        None
                    } else {
                        Some("calendar".to_string())
                    },
                ))
            } else {
                None
            };
        let owner_account_id = collection_scope
            .as_ref()
            .map(|(owner_account_id, _, _)| *owner_account_id);
        let calendar_id = collection_scope
            .as_ref()
            .and_then(|(_, calendar_id, _)| *calendar_id);
        let calendar_role = collection_scope
            .as_ref()
            .and_then(|(_, _, role)| role.as_deref());

        let rows = sqlx::query_as::<_, crate::AccessibleEventRow>(
            r#"
            SELECT
                e.id,
                e.uid,
                e.calendar_id,
                c.role AS calendar_role,
                e.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                CASE WHEN e.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN e.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN e.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN e.owner_account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                to_char(e.starts_at AT TIME ZONE COALESCE(NULLIF(e.time_zone, ''), 'UTC'), 'YYYY-MM-DD') AS date,
                to_char(e.starts_at AT TIME ZONE COALESCE(NULLIF(e.time_zone, ''), 'UTC'), 'HH24:MI') AS time,
                e.time_zone,
                GREATEST(0, EXTRACT(EPOCH FROM (e.ends_at - e.starts_at))::int / 60) AS duration_minutes,
                e.all_day,
                e.status,
                e.sequence,
                COALESCE(e.recurrence_rule, '') AS recurrence_rule,
                e.recurrence_json::text AS recurrence_json,
                e.recurrence_exceptions_json::text AS recurrence_exceptions_json,
                e.title,
                e.location,
                e.organizer_json::text AS organizer_json,
                COALESCE(e.source_payload_json->>'attendees', '') AS attendees,
                e.attendees_json::text AS attendees_json,
                e.body_text AS notes,
                COALESCE(e.body_html, '') AS body_html
            FROM calendar_events e
            JOIN accounts owner ON owner.id = e.owner_account_id
            JOIN calendars c
              ON c.tenant_id = e.tenant_id
             AND c.owner_account_id = e.owner_account_id
             AND c.id = e.calendar_id
            LEFT JOIN calendar_grants g
              ON g.tenant_id = e.tenant_id
             AND g.calendar_id = c.id
             AND g.owner_account_id = e.owner_account_id
             AND g.grantee_account_id = $2
            WHERE e.tenant_id = $1
              AND (e.owner_account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR e.owner_account_id = $3)
              AND ($4::uuid IS NULL OR e.calendar_id = $4)
              AND ($5::text IS NULL OR c.role = $5)
              AND ($6::uuid[] IS NULL OR e.id = ANY($6))
              AND e.lifecycle_state = $7
            ORDER BY e.starts_at ASC, e.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(calendar_role)
        .bind(ids)
        .bind(lifecycle_state)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessibleEvent {
                id: row.id,
                uid: row.uid,
                collection_id: calendar_collection_id_for_event(
                    principal_account_id,
                    row.owner_account_id,
                    row.calendar_id,
                    &row.calendar_role,
                ),
                owner_account_id: row.owner_account_id,
                owner_email: row.owner_email,
                owner_display_name: row.owner_display_name,
                rights: CollaborationRights {
                    may_read: row.may_read,
                    may_write: row.may_write,
                    may_delete: row.may_delete,
                    may_share: row.may_share,
                },
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
            })
            .collect())
    }

    pub(crate) async fn ensure_default_contact_book_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
    ) -> Result<Uuid> {
        Self::ensure_contact_book_in_tx(tx, tenant_id, owner_account_id, DEFAULT_CONTACT_BOOK_ROLE)
            .await
    }

    pub(crate) async fn ensure_contact_book_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        role: &str,
    ) -> Result<Uuid> {
        let (role, display_name) = match role {
            SUGGESTED_CONTACTS_ROLE => (SUGGESTED_CONTACTS_ROLE, "Suggested Contacts"),
            QUICK_CONTACTS_ROLE => (QUICK_CONTACTS_ROLE, "Quick Contacts"),
            IM_CONTACT_LIST_ROLE => (IM_CONTACT_LIST_ROLE, "IM Contact List"),
            _ => (DEFAULT_CONTACT_BOOK_ROLE, "Contacts"),
        };
        sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO contact_books (id, tenant_id, owner_account_id, display_name, role)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, owner_account_id, role)
            WHERE role <> 'custom'
            DO UPDATE SET display_name = contact_books.display_name
            RETURNING id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(display_name)
        .bind(role)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn ensure_default_calendar_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
    ) -> Result<Uuid> {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO calendars (id, tenant_id, owner_account_id, display_name, role)
            VALUES ($1, $2, $3, 'Calendar', 'calendar')
            ON CONFLICT (tenant_id, owner_account_id, role)
            WHERE role <> 'custom'
            DO UPDATE SET display_name = calendars.display_name
            RETURNING id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(owner_account_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }
}

fn task_collection_id_for_list(
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    task_list_id: Uuid,
    role: Option<&str>,
) -> String {
    if role == Some(DEFAULT_TASK_LIST_ROLE) {
        collection_id_for_owner(
            CollaborationResourceKind::Tasks,
            principal_account_id,
            owner_account_id,
            DEFAULT_TASK_LIST_ROLE,
        )
    } else {
        task_list_id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_task_list_uses_stable_default_collection_id() {
        let account_id = Uuid::from_u128(0x11111111_2222_3333_4444_555555555555);
        let task_list_id = Uuid::from_u128(0xaaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee);

        assert_eq!(
            task_collection_id_for_list(
                account_id,
                account_id,
                task_list_id,
                Some(DEFAULT_TASK_LIST_ROLE),
            ),
            DEFAULT_COLLECTION_ID
        );
        assert_eq!(
            task_collection_id_for_list(account_id, account_id, task_list_id, Some("custom")),
            task_list_id.to_string()
        );
    }
}
