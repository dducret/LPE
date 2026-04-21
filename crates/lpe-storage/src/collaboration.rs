use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    normalize_email, AuditEntryInput, CollaborationCollectionRow, CollaborationGrantRow,
    DEFAULT_COLLECTION_ID, Storage, UpsertClientContactInput, UpsertClientEventInput,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CollaborationResourceKind {
    Contacts,
    Calendar,
    Tasks,
}

impl CollaborationResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Contacts => "contacts",
            Self::Calendar => "calendar",
            Self::Tasks => "tasks",
        }
    }

    pub fn collection_label(&self) -> &'static str {
        match self {
            Self::Contacts => "Contacts",
            Self::Calendar => "Calendar",
            Self::Tasks => "Task List",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationRights {
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationCollection {
    pub id: String,
    pub kind: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleContact {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleEvent {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub recurrence_rule: String,
    pub title: String,
    pub location: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationGrant {
    pub id: Uuid,
    pub kind: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub rights: CollaborationRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CollaborationGrantInput {
    pub kind: CollaborationResourceKind,
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

impl Storage {
    pub async fn upsert_collaboration_grant(
        &self,
        input: CollaborationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<CollaborationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        validate_collaboration_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO collaboration_collection_grants (
                id, tenant_id, collection_kind, owner_account_id, grantee_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, collection_kind, owner_account_id, grantee_account_id)
            DO UPDATE SET
                may_read = EXCLUDED.may_read,
                may_write = EXCLUDED.may_write,
                may_delete = EXCLUDED.may_delete,
                may_share = EXCLUDED.may_share,
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.kind.as_str())
        .bind(input.owner_account_id)
        .bind(grantee.id)
        .bind(input.may_read)
        .bind(input.may_write)
        .bind(input.may_delete)
        .bind(input.may_share)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_collaboration_grant(input.kind, owner.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("collaboration grant not found after upsert"))
    }

    pub async fn delete_collaboration_grant(
        &self,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM collaboration_collection_grants
            WHERE tenant_id = $1
              AND collection_kind = $2
              AND owner_account_id = $3
              AND grantee_account_id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("collaboration grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_grant_change(
            &mut tx,
            &tenant_id,
            kind,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_collaboration_grant(
        &self,
        kind: CollaborationResourceKind,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<CollaborationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, CollaborationGrantRow>(
            r#"
            SELECT
                g.id,
                g.collection_kind AS kind,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.owner_account_id = $3
              AND g.grantee_account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_collaboration_grant))
    }

    pub async fn fetch_outgoing_collaboration_grants(
        &self,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Vec<CollaborationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, CollaborationGrantRow>(
            r#"
            SELECT
                g.id,
                g.collection_kind AS kind,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.owner_account_id = $3
            ORDER BY lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_collaboration_grant).collect())
    }

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

    pub async fn fetch_accessible_task_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        let task_lists = self.fetch_task_lists(principal_account_id).await?;
        Ok(task_lists
            .into_iter()
            .map(|task_list| CollaborationCollection {
                id: task_list.id.to_string(),
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

        let contact = self
            .upsert_client_contact(UpsertClientContactInput {
                id: input.id,
                account_id: access.owner_account_id,
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            })
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

        self.upsert_client_contact(UpsertClientContactInput {
            id: Some(contact_id),
            account_id: existing.owner_account_id,
            name: input.name,
            role: input.role,
            email: input.email,
            phone: input.phone,
            team: input.team,
            notes: input.notes,
        })
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
        self.fetch_accessible_events_internal(principal_account_id, None, None)
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
        self.fetch_accessible_events_internal(principal_account_id, None, Some(ids))
            .await
    }

    pub async fn fetch_accessible_events_in_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(principal_account_id, Some(collection_id), None)
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

        let event = self
            .upsert_client_event(UpsertClientEventInput {
                id: input.id,
                account_id: access.owner_account_id,
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
            })
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

        self.upsert_client_event(UpsertClientEventInput {
            id: Some(event_id),
            account_id: existing.owner_account_id,
            date: input.date,
            time: input.time,
            time_zone: input.time_zone,
            duration_minutes: input.duration_minutes,
            recurrence_rule: input.recurrence_rule,
            title: input.title,
            location: input.location,
            attendees: input.attendees,
            attendees_json: input.attendees_json,
            notes: input.notes,
        })
        .await?;

        self.fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not visible after update"))
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

        let rows = sqlx::query_as::<_, CollaborationCollectionRow>(
            r#"
            SELECT
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.grantee_account_id = $3
              AND g.may_read = TRUE
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        collections.extend(rows.into_iter().map(|row| CollaborationCollection {
            id: shared_collection_id(kind, row.owner_account_id),
            kind: kind.as_str().to_string(),
            owner_account_id: row.owner_account_id,
            owner_email: row.owner_email.clone(),
            owner_display_name: row.owner_display_name.clone(),
            display_name: shared_collection_display_name(
                kind,
                &row.owner_display_name,
                &row.owner_email,
            ),
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
        let owner_account_id =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                Some(
                    self.resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Contacts,
                        collection_id,
                    )
                    .await?
                    .owner_account_id,
                )
            } else {
                None
            };

        let rows = sqlx::query_as::<_, crate::AccessibleContactRow>(
            r#"
            SELECT
                c.id,
                c.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                c.name,
                c.role,
                c.email,
                c.phone,
                c.team,
                c.notes
            FROM contacts c
            JOIN accounts owner ON owner.id = c.account_id
            LEFT JOIN collaboration_collection_grants g
              ON g.tenant_id = c.tenant_id
             AND g.collection_kind = 'contacts'
             AND g.owner_account_id = c.account_id
             AND g.grantee_account_id = $2
            WHERE c.tenant_id = $1
              AND (c.account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR c.account_id = $3)
              AND ($4::uuid[] IS NULL OR c.id = ANY($4))
            ORDER BY lower(c.name) ASC, c.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
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
            })
            .collect())
    }

    async fn fetch_accessible_events_internal(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        ids: Option<&[Uuid]>,
    ) -> Result<Vec<AccessibleEvent>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let owner_account_id =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                Some(
                    self.resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Calendar,
                        collection_id,
                    )
                    .await?
                    .owner_account_id,
                )
            } else {
                None
            };

        let rows = sqlx::query_as::<_, crate::AccessibleEventRow>(
            r#"
            SELECT
                e.id,
                e.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                to_char(e.event_date, 'YYYY-MM-DD') AS date,
                to_char(e.event_time, 'HH24:MI') AS time,
                e.time_zone,
                e.duration_minutes,
                e.recurrence_rule,
                e.title,
                e.location,
                e.attendees,
                e.attendees_json,
                e.notes
            FROM calendar_events e
            JOIN accounts owner ON owner.id = e.account_id
            LEFT JOIN collaboration_collection_grants g
              ON g.tenant_id = e.tenant_id
             AND g.collection_kind = 'calendar'
             AND g.owner_account_id = e.account_id
             AND g.grantee_account_id = $2
            WHERE e.tenant_id = $1
              AND (e.account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR e.account_id = $3)
              AND ($4::uuid[] IS NULL OR e.id = ANY($4))
            ORDER BY e.event_date ASC, e.event_time ASC, e.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessibleEvent {
                id: row.id,
                collection_id: collection_id_for_owner(
                    CollaborationResourceKind::Calendar,
                    principal_account_id,
                    row.owner_account_id,
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
                recurrence_rule: row.recurrence_rule,
                title: row.title,
                location: row.location,
                attendees: row.attendees,
                attendees_json: row.attendees_json,
                notes: row.notes,
            })
            .collect())
    }
}

pub(crate) fn validate_collaboration_rights(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> Result<()> {
    if !may_read && (may_write || may_delete || may_share) {
        bail!("read access is required when granting write, delete, or share");
    }
    if may_delete && !may_write {
        bail!("delete access requires write access");
    }
    if may_share && !may_write {
        bail!("share access requires write access");
    }
    Ok(())
}

fn collection_id_for_owner(
    kind: CollaborationResourceKind,
    principal_account_id: Uuid,
    owner_account_id: Uuid,
) -> String {
    if principal_account_id == owner_account_id {
        DEFAULT_COLLECTION_ID.to_string()
    } else {
        shared_collection_id(kind, owner_account_id)
    }
}

fn shared_collection_id(kind: CollaborationResourceKind, owner_account_id: Uuid) -> String {
    format!("shared-{}-{}", kind.as_str(), owner_account_id)
}

fn shared_collection_display_name(
    kind: CollaborationResourceKind,
    owner_display_name: &str,
    owner_email: &str,
) -> String {
    let owner_label = if owner_display_name.trim().is_empty() {
        owner_email.trim()
    } else {
        owner_display_name.trim()
    };
    format!("{owner_label} {}", kind.collection_label())
}

fn map_collaboration_grant(row: CollaborationGrantRow) -> CollaborationGrant {
    CollaborationGrant {
        id: row.id,
        kind: row.kind,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}
