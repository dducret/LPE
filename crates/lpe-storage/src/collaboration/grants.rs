use anyhow::{anyhow, bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    normalize_email, AuditEntryInput, CanonicalChangeCategory, CollaborationGrantRow, Storage,
    DEFAULT_TASK_LIST_ROLE,
};

use super::types::{
    map_collaboration_grant, validate_collaboration_rights, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind,
};

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
                let collection_id = if let Some(calendar_id) = input.calendar_id {
                    let calendar_exists = sqlx::query_scalar::<_, bool>(
                        r#"
                        SELECT EXISTS (
                            SELECT 1
                            FROM calendars
                            WHERE tenant_id = $1
                              AND owner_account_id = $2
                              AND id = $3
                        )
                        "#,
                    )
                    .bind(&tenant_id)
                    .bind(owner.id)
                    .bind(calendar_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    if !calendar_exists {
                        bail!("calendar collection not found");
                    }
                    calendar_id
                } else {
                    Self::ensure_default_calendar_in_tx(&mut tx, &tenant_id, owner.id).await?
                };
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

        if input.kind == CollaborationResourceKind::Calendar {
            if let Some(calendar_id) = input.calendar_id {
                return self
                    .fetch_outgoing_collaboration_grants(
                        owner.id,
                        CollaborationResourceKind::Calendar,
                    )
                    .await?
                    .into_iter()
                    .find(|grant| {
                        grant.calendar_id == Some(calendar_id)
                            && grant.grantee_account_id == grantee.id
                    })
                    .ok_or_else(|| anyhow!("collaboration grant not found after upsert"));
            }
        }

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

    pub async fn delete_calendar_collection_grant(
        &self,
        owner_account_id: Uuid,
        calendar_collection_id: &str,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let calendar_id = Uuid::parse_str(calendar_collection_id.trim())
            .map_err(|_| anyhow!("calendarId must be a custom calendar UUID"))?;
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let grant_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            DELETE FROM calendar_grants g
            USING calendars c
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.calendar_id = $3
              AND g.grantee_account_id = $4
              AND c.tenant_id = g.tenant_id
              AND c.owner_account_id = g.owner_account_id
              AND c.id = g.calendar_id
            RETURNING g.id
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(calendar_id)
        .bind(grantee_account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("calendar grant not found"))?;

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner_account_id),
            None,
            "calendar_grant",
            grant_id,
            "destroyed",
            modseq,
            &[owner_account_id, grantee_account_id],
            serde_json::json!({
                "collectionId": calendar_id,
                "granteeId": grantee_account_id
            }),
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_grant_change(
            &mut tx,
            &tenant_id,
            CollaborationResourceKind::Calendar,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_calendar_collection_grant(
        &self,
        owner_account_id: Uuid,
        calendar_collection_id: &str,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> Result<()> {
        validate_collaboration_rights(may_read, may_write, may_delete, may_share)?;
        let calendar_id = Uuid::parse_str(calendar_collection_id.trim())
            .map_err(|_| anyhow!("calendar collection id must be a UUID"))?;
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, grantee_account_id)
            .await?;
        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        let calendar_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM calendars
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND id = $3
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(calendar_id)
        .fetch_one(&mut *tx)
        .await?;
        if !calendar_exists {
            bail!("calendar collection not found");
        }

        let (grant_id, change_type) = if may_read {
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
            .bind(calendar_id)
            .bind(owner.id)
            .bind(grantee.id)
            .bind(may_read)
            .bind(may_write)
            .bind(may_delete)
            .bind(may_share)
            .fetch_one(&mut *tx)
            .await?;
            (grant_id, "updated")
        } else {
            let grant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                DELETE FROM calendar_grants
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND calendar_id = $3
                  AND grantee_account_id = $4
                RETURNING id
                "#,
            )
            .bind(&tenant_id)
            .bind(owner.id)
            .bind(calendar_id)
            .bind(grantee.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("calendar grant not found"))?;
            (grant_id, "destroyed")
        };

        let modseq = self
            .allocate_account_modseq_in_tx(
                &mut tx,
                &tenant_id,
                owner.id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(owner.id),
            None,
            "calendar_grant",
            grant_id,
            change_type,
            modseq,
            &[owner.id, grantee.id],
            serde_json::json!({
                "collectionId": calendar_id,
                "granteeId": grantee.id
            }),
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_grant_change(
            &mut tx,
            &tenant_id,
            CollaborationResourceKind::Calendar,
            owner.id,
            grantee.id,
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
                    NULL::uuid AS calendar_id,
                    NULL::text AS calendar_name,
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
                    g.calendar_id,
                    c.display_name AS calendar_name,
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
                    NULL::uuid AS calendar_id,
                    NULL::text AS calendar_name,
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
                    NULL::uuid AS calendar_id,
                    NULL::text AS calendar_name,
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
                    g.calendar_id,
                    c.display_name AS calendar_name,
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
                    NULL::uuid AS calendar_id,
                    NULL::text AS calendar_name,
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
}
