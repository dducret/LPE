use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use sqlx::{Postgres, Row};

use crate::{
    normalize_email, AuditEntryInput, CanonicalChangeCategory, CollaborationCollectionRow,
    CollaborationGrantRow, Storage, UpsertClientContactInput, UpsertClientEventInput,
    DEFAULT_COLLECTION_ID, DEFAULT_CONTACT_BOOK_ROLE, DEFAULT_TASK_LIST_ROLE,
    IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE, QUICK_CONTACTS_COLLECTION_ID,
    QUICK_CONTACTS_ROLE, SUGGESTED_CONTACTS_COLLECTION_ID, SUGGESTED_CONTACTS_ROLE,
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
    pub uid: String,
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

        let (grant_id, collection_id, object_kind, category) = match input.kind {
            CollaborationResourceKind::Contacts => {
                let collection_id =
                    Self::ensure_default_contact_book_in_tx(&mut tx, &tenant_id, owner.id).await?;
                let grant_id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    INSERT INTO contact_book_grants (
                        id, tenant_id, contact_book_id, owner_account_id, grantee_account_id,
                        may_read, may_write, may_delete, may_share
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (tenant_id, contact_book_id, grantee_account_id)
                    DO UPDATE SET
                        may_read = EXCLUDED.may_read,
                        may_write = EXCLUDED.may_write,
                        may_delete = EXCLUDED.may_delete,
                        may_share = EXCLUDED.may_share,
                        updated_at = NOW()
                    RETURNING id
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(&tenant_id)
                .bind(collection_id)
                .bind(owner.id)
                .bind(grantee.id)
                .bind(input.may_read)
                .bind(input.may_write)
                .bind(input.may_delete)
                .bind(input.may_share)
                .fetch_one(&mut *tx)
                .await?;
                (
                    grant_id,
                    collection_id,
                    "contact_book_grant",
                    CanonicalChangeCategory::Contacts,
                )
            }
            CollaborationResourceKind::Calendar => {
                let collection_id =
                    Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, owner.id).await?;
                let grant_id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    INSERT INTO calendar_grants (
                        id, tenant_id, calendar_id, owner_account_id, grantee_account_id,
                        may_read, may_write, may_delete, may_share
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (tenant_id, calendar_id, grantee_account_id)
                    DO UPDATE SET
                        may_read = EXCLUDED.may_read,
                        may_write = EXCLUDED.may_write,
                        may_delete = EXCLUDED.may_delete,
                        may_share = EXCLUDED.may_share,
                        updated_at = NOW()
                    RETURNING id
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(&tenant_id)
                .bind(collection_id)
                .bind(owner.id)
                .bind(grantee.id)
                .bind(input.may_read)
                .bind(input.may_write)
                .bind(input.may_delete)
                .bind(input.may_share)
                .fetch_one(&mut *tx)
                .await?;
                (
                    grant_id,
                    collection_id,
                    "calendar_grant",
                    CanonicalChangeCategory::Calendar,
                )
            }
            CollaborationResourceKind::Tasks => {
                let task_list =
                    Self::ensure_default_task_list(&mut tx, &tenant_id, owner.id).await?;
                let grant_id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    INSERT INTO task_list_grants (
                        id, tenant_id, task_list_id, owner_account_id, grantee_account_id,
                        may_read, may_write, may_delete, may_share
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (tenant_id, task_list_id, grantee_account_id)
                    DO UPDATE SET
                        may_read = EXCLUDED.may_read,
                        may_write = EXCLUDED.may_write,
                        may_delete = EXCLUDED.may_delete,
                        may_share = EXCLUDED.may_share,
                        updated_at = NOW()
                    RETURNING id
                    "#,
                )
                .bind(Uuid::new_v4())
                .bind(&tenant_id)
                .bind(task_list.id)
                .bind(owner.id)
                .bind(grantee.id)
                .bind(input.may_read)
                .bind(input.may_write)
                .bind(input.may_delete)
                .bind(input.may_share)
                .fetch_one(&mut *tx)
                .await?;
                (
                    grant_id,
                    task_list.id,
                    "task_list_grant",
                    CanonicalChangeCategory::Tasks,
                )
            }
        };
        let modseq = self
            .allocate_account_modseq_in_tx(&mut tx, &tenant_id, owner.id, category.as_str())
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner.id),
            None,
            object_kind,
            grant_id,
            "updated",
            modseq,
            &[owner.id, grantee.id],
            serde_json::json!({
                "collectionId": collection_id,
                "granteeId": grantee.id
            }),
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_grant_change(
            &mut tx, &tenant_id, input.kind, owner.id, grantee.id,
        )
        .await?;
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
        let (grant_id, collection_id, object_kind, category) = match kind {
            CollaborationResourceKind::Contacts => {
                let row = sqlx::query(
                    r#"
                DELETE FROM contact_book_grants g
                USING contact_books b
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                  AND b.tenant_id = g.tenant_id
                  AND b.owner_account_id = g.owner_account_id
                  AND b.id = g.contact_book_id
                  AND b.role = 'contacts'
                RETURNING g.id, g.contact_book_id
                "#,
                )
                .bind(&tenant_id)
                .bind(owner_account_id)
                .bind(grantee_account_id)
                .fetch_optional(&mut *tx)
                .await?;
                row.map(|row| -> Result<_> {
                    Ok((
                        row.try_get::<Uuid, _>("id")?,
                        row.try_get::<Uuid, _>("contact_book_id")?,
                        "contact_book_grant",
                        CanonicalChangeCategory::Contacts,
                    ))
                })
            }
            CollaborationResourceKind::Calendar => {
                let row = sqlx::query(
                    r#"
                DELETE FROM calendar_grants g
                USING calendars c
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                  AND c.tenant_id = g.tenant_id
                  AND c.owner_account_id = g.owner_account_id
                  AND c.id = g.calendar_id
                  AND c.role = 'calendar'
                RETURNING g.id, g.calendar_id
                "#,
                )
                .bind(&tenant_id)
                .bind(owner_account_id)
                .bind(grantee_account_id)
                .fetch_optional(&mut *tx)
                .await?;
                row.map(|row| -> Result<_> {
                    Ok((
                        row.try_get::<Uuid, _>("id")?,
                        row.try_get::<Uuid, _>("calendar_id")?,
                        "calendar_grant",
                        CanonicalChangeCategory::Calendar,
                    ))
                })
            }
            CollaborationResourceKind::Tasks => {
                let row = sqlx::query(
                    r#"
                DELETE FROM task_list_grants g
                USING task_lists l
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                  AND l.tenant_id = g.tenant_id
                  AND l.owner_account_id = g.owner_account_id
                  AND l.id = g.task_list_id
                  AND l.role = $4
                RETURNING g.id, g.task_list_id
                "#,
                )
                .bind(&tenant_id)
                .bind(owner_account_id)
                .bind(grantee_account_id)
                .bind(DEFAULT_TASK_LIST_ROLE)
                .fetch_optional(&mut *tx)
                .await?;
                row.map(|row| -> Result<_> {
                    Ok((
                        row.try_get::<Uuid, _>("id")?,
                        row.try_get::<Uuid, _>("task_list_id")?,
                        "task_list_grant",
                        CanonicalChangeCategory::Tasks,
                    ))
                })
            }
        }
        .transpose()?
        .ok_or_else(|| anyhow!("collaboration grant not found"))?;

        let modseq = self
            .allocate_account_modseq_in_tx(&mut tx, &tenant_id, owner_account_id, category.as_str())
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            object_kind,
            grant_id,
            "destroyed",
            modseq,
            &[owner_account_id, grantee_account_id],
            serde_json::json!({
                "collectionId": collection_id,
                "granteeId": grantee_account_id
            }),
        )
        .await?;

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
        let row = match kind {
            CollaborationResourceKind::Contacts => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'contacts'::text AS kind,
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
                FROM contact_book_grants g
                JOIN contact_books b
                  ON b.tenant_id = g.tenant_id
                 AND b.owner_account_id = g.owner_account_id
                 AND b.id = g.contact_book_id
                 AND b.role = 'contacts'
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .bind(grantee_account_id)
            .fetch_optional(&self.pool)
            .await?,
            CollaborationResourceKind::Calendar => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'calendar'::text AS kind,
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
                FROM calendar_grants g
                JOIN calendars c
                  ON c.tenant_id = g.tenant_id
                 AND c.owner_account_id = g.owner_account_id
                 AND c.id = g.calendar_id
                 AND c.role = 'calendar'
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .bind(grantee_account_id)
            .fetch_optional(&self.pool)
            .await?,
            CollaborationResourceKind::Tasks => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'tasks'::text AS kind,
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
                FROM task_list_grants g
                JOIN task_lists l
                  ON l.tenant_id = g.tenant_id
                 AND l.owner_account_id = g.owner_account_id
                 AND l.id = g.task_list_id
                 AND l.role = $4
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                  AND g.grantee_account_id = $3
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .bind(grantee_account_id)
            .bind(DEFAULT_TASK_LIST_ROLE)
            .fetch_optional(&self.pool)
            .await?,
        };

        Ok(row.map(map_collaboration_grant))
    }

    pub async fn fetch_outgoing_collaboration_grants(
        &self,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Vec<CollaborationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = match kind {
            CollaborationResourceKind::Contacts => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'contacts'::text AS kind,
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
                FROM contact_book_grants g
                JOIN contact_books b
                  ON b.tenant_id = g.tenant_id
                 AND b.owner_account_id = g.owner_account_id
                 AND b.id = g.contact_book_id
                 AND b.role = 'contacts'
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .fetch_all(&self.pool)
            .await?,
            CollaborationResourceKind::Calendar => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'calendar'::text AS kind,
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
                FROM calendar_grants g
                JOIN calendars c
                  ON c.tenant_id = g.tenant_id
                 AND c.owner_account_id = g.owner_account_id
                 AND c.id = g.calendar_id
                 AND c.role = 'calendar'
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .fetch_all(&self.pool)
            .await?,
            CollaborationResourceKind::Tasks => sqlx::query_as::<_, CollaborationGrantRow>(
                r#"
                SELECT
                    g.id,
                    'tasks'::text AS kind,
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
                FROM task_list_grants g
                JOIN task_lists l
                  ON l.tenant_id = g.tenant_id
                 AND l.owner_account_id = g.owner_account_id
                 AND l.id = g.task_list_id
                 AND l.role = $3
                JOIN accounts owner ON owner.id = g.owner_account_id
                JOIN accounts grantee ON grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.owner_account_id = $2
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(owner_account_id)
            .bind(DEFAULT_TASK_LIST_ROLE)
            .fetch_all(&self.pool)
            .await?,
        };

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
                uid: input.uid,
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
            uid: if input.uid.trim().is_empty() {
                existing.uid.clone()
            } else {
                input.uid
            },
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
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(existing.owner_account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &[existing.owner_account_id],
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

        let rows = match kind {
            CollaborationResourceKind::Contacts => {
                sqlx::query_as::<_, CollaborationCollectionRow>(
                    r#"
                SELECT
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
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
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM calendar_grants g
                JOIN calendars c
                  ON c.tenant_id = g.tenant_id
                 AND c.owner_account_id = g.owner_account_id
                 AND c.id = g.calendar_id
                 AND c.role = 'calendar'
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
                    g.owner_account_id,
                    owner.primary_email AS owner_email,
                    owner.display_name AS owner_display_name,
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
                c.notes
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
                e.uid,
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
                COALESCE(e.recurrence_rule, '') AS recurrence_rule,
                e.title,
                e.location,
                COALESCE(e.source_payload_json->>'attendees', '') AS attendees,
                e.attendees_json::text AS attendees_json,
                e.body_text AS notes
            FROM calendar_events e
            JOIN accounts owner ON owner.id = e.owner_account_id
            LEFT JOIN calendars c
              ON c.tenant_id = e.tenant_id
             AND c.owner_account_id = e.owner_account_id
             AND c.role = 'calendar'
            LEFT JOIN calendar_grants g
              ON g.tenant_id = e.tenant_id
             AND g.calendar_id = c.id
             AND g.owner_account_id = e.owner_account_id
             AND g.grantee_account_id = $2
            WHERE e.tenant_id = $1
              AND (e.owner_account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR e.owner_account_id = $3)
              AND ($4::uuid[] IS NULL OR e.id = ANY($4))
            ORDER BY e.starts_at ASC, e.id ASC
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
                uid: row.uid,
                collection_id: collection_id_for_owner(
                    CollaborationResourceKind::Calendar,
                    principal_account_id,
                    row.owner_account_id,
                    "",
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
    role: &str,
) -> String {
    if principal_account_id == owner_account_id {
        if kind == CollaborationResourceKind::Contacts && role == SUGGESTED_CONTACTS_ROLE {
            SUGGESTED_CONTACTS_COLLECTION_ID.to_string()
        } else if kind == CollaborationResourceKind::Contacts && role == QUICK_CONTACTS_ROLE {
            QUICK_CONTACTS_COLLECTION_ID.to_string()
        } else if kind == CollaborationResourceKind::Contacts && role == IM_CONTACT_LIST_ROLE {
            IM_CONTACT_LIST_COLLECTION_ID.to_string()
        } else {
            DEFAULT_COLLECTION_ID.to_string()
        }
    } else {
        shared_collection_id(kind, owner_account_id)
    }
}

fn contact_book_role_for_collection_id(collection_id: Option<&str>) -> &'static str {
    match collection_id.map(str::trim) {
        Some(SUGGESTED_CONTACTS_COLLECTION_ID) => SUGGESTED_CONTACTS_ROLE,
        Some(QUICK_CONTACTS_COLLECTION_ID) => QUICK_CONTACTS_ROLE,
        Some(IM_CONTACT_LIST_COLLECTION_ID) => IM_CONTACT_LIST_ROLE,
        _ => DEFAULT_CONTACT_BOOK_ROLE,
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
