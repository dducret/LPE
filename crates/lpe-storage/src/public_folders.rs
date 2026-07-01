use anyhow::{bail, Result};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    collaboration::validate_collaboration_rights, AuditEntryInput, PublicFolderItemRow,
    PublicFolderPerUserStateRow, PublicFolderPermissionRow, PublicFolderReplicaRow,
    PublicFolderRow, PublicFolderTreeRow, Storage,
};

mod changes;
mod types;

pub(crate) use types::{
    ensure_delete, ensure_read, ensure_share, ensure_tree_admin, ensure_write, map_public_folder,
    map_public_folder_item, map_public_folder_per_user_state, map_public_folder_permission,
    map_public_folder_replica, map_public_folder_tree, public_folder_item_select_sql,
    public_folder_select_sql, PublicFolderAccess,
};
pub use types::{
    CreatePublicFolderInput, CreatePublicFolderTreeInput, PublicFolder, PublicFolderItem,
    PublicFolderPerUserState, PublicFolderPerUserStatePatch, PublicFolderPermission,
    PublicFolderPermissionInput, PublicFolderReplica, PublicFolderReplicaInput, PublicFolderRights,
    PublicFolderTree, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
};

impl Storage {
    pub async fn create_public_folder_tree(
        &self,
        input: CreatePublicFolderTreeInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolder> {
        let display_name = input.display_name.trim();
        if display_name.is_empty() {
            bail!("public folder tree display name is required");
        }
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let tree_id = Uuid::new_v4();
        let root_folder_id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO public_folder_trees (
                id, tenant_id, canonical_id, display_name, admin_owner_account_id
            )
            VALUES ($1, $2, $1, $3, $4)
            "#,
        )
        .bind(tree_id)
        .bind(&tenant_id)
        .bind(display_name)
        .bind(input.account_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO public_folders (
                id, tenant_id, tree_id, canonical_id, display_name, folder_class, path, sort_order
            )
            VALUES ($1, $2, $3, $1, $4, 'IPF.Note', $5, 0)
            "#,
        )
        .bind(root_folder_id)
        .bind(&tenant_id)
        .bind(tree_id)
        .bind(display_name)
        .bind(format!("/{display_name}"))
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE public_folder_trees
            SET root_folder_id = $3, updated_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(tree_id)
        .bind(root_folder_id)
        .execute(&mut *tx)
        .await?;
        let access = PublicFolderAccess {
            tenant_id,
            tree_admin_owner_account_id: input.account_id,
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        };
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            root_folder_id,
            "public_folder_tree",
            tree_id,
            "created",
            json!({"rootFolderId": root_folder_id}),
        )
        .await?;
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            root_folder_id,
            "public_folder",
            root_folder_id,
            "created",
            json!({"folderId": root_folder_id, "treeId": tree_id}),
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        self.fetch_public_folder(input.account_id, root_folder_id)
            .await
    }

    pub async fn create_public_folder_child(
        &self,
        input: CreatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolder> {
        let display_name = input.display_name.trim();
        if display_name.is_empty() {
            bail!("public folder display name is required");
        }
        let access = self
            .public_folder_access(input.account_id, input.parent_folder_id)
            .await?;
        ensure_tree_admin(input.account_id, access)?;
        let parent = self
            .fetch_public_folder_row(input.account_id, input.parent_folder_id)
            .await?;
        let folder_id = Uuid::new_v4();
        let folder_class = if input.folder_class.trim().is_empty() {
            "IPF.Note"
        } else {
            input.folder_class.trim()
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO public_folders (
                id, tenant_id, tree_id, parent_folder_id, canonical_id, display_name,
                folder_class, path, sort_order
            )
            VALUES ($1, $2, $3, $4, $1, $5, $6, $7, $8)
            "#,
        )
        .bind(folder_id)
        .bind(&access.tenant_id)
        .bind(parent.tree_id)
        .bind(input.parent_folder_id)
        .bind(display_name)
        .bind(folder_class)
        .bind(format!(
            "{}/{}",
            parent.path.trim_end_matches('/'),
            display_name
        ))
        .bind(input.sort_order)
        .execute(&mut *tx)
        .await?;
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            input.parent_folder_id,
            "public_folder",
            folder_id,
            "created",
            json!({
                "folderId": folder_id,
                "parentFolderId": input.parent_folder_id,
                "treeId": parent.tree_id,
            }),
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        self.fetch_public_folder(input.account_id, folder_id).await
    }

    pub async fn fetch_public_folder_trees(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<PublicFolderTree>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, PublicFolderTreeRow>(
            r#"
            SELECT DISTINCT
                t.id,
                t.canonical_id,
                t.display_name,
                t.lifecycle_state,
                t.admin_owner_account_id,
                t.root_folder_id,
                to_char(t.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(t.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_trees t
            JOIN public_folders f
              ON f.tenant_id = t.tenant_id
             AND f.tree_id = t.id
             AND f.lifecycle_state <> 'deleted'
            LEFT JOIN public_folder_permissions p
              ON p.tenant_id = f.tenant_id
             AND p.public_folder_id = f.id
             AND p.principal_account_id = $2
            WHERE t.tenant_id = $1
              AND t.lifecycle_state = 'active'
              AND (t.admin_owner_account_id = $2 OR COALESCE(p.may_read, FALSE))
            ORDER BY t.display_name ASC, t.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder_tree).collect())
    }

    pub async fn fetch_public_folder(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<PublicFolder> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        self.fetch_public_folder_row(account_id, folder_id).await
    }

    pub async fn fetch_public_folder_children(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<Vec<PublicFolder>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        let rows = sqlx::query_as::<_, PublicFolderRow>(&public_folder_select_sql(
            "WHERE f.tenant_id = $1 AND f.parent_folder_id = $3 AND f.lifecycle_state <> 'deleted'",
        ))
        .bind(&access.tenant_id)
        .bind(account_id)
        .bind(folder_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder).collect())
    }

    pub async fn update_public_folder(
        &self,
        input: UpdatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolder> {
        let access = self
            .public_folder_access(input.account_id, input.folder_id)
            .await?;
        ensure_tree_admin(input.account_id, access)?;
        let current = self
            .fetch_public_folder_row(input.account_id, input.folder_id)
            .await?;
        let target_parent = if let Some(parent_folder_id) = input.parent_folder_id {
            if current.parent_folder_id.is_none() {
                bail!("public folder tree root cannot be moved");
            }
            if parent_folder_id == input.folder_id {
                bail!("public folder cannot be moved under itself");
            }
            let parent_access = self
                .public_folder_access(input.account_id, parent_folder_id)
                .await?;
            ensure_tree_admin(input.account_id, parent_access)?;
            let is_descendant = sqlx::query_scalar::<_, i64>(
                r#"
                WITH RECURSIVE subtree AS (
                    SELECT id
                    FROM public_folders
                    WHERE tenant_id = $1
                      AND id = $2
                      AND lifecycle_state <> 'deleted'
                    UNION ALL
                    SELECT child.id
                    FROM public_folders child
                    JOIN subtree parent
                      ON parent.id = child.parent_folder_id
                    WHERE child.tenant_id = $1
                      AND child.lifecycle_state <> 'deleted'
                )
                SELECT 1::bigint
                FROM subtree
                WHERE id = $3
                LIMIT 1
                "#,
            )
            .bind(&access.tenant_id)
            .bind(input.folder_id)
            .bind(parent_folder_id)
            .fetch_optional(&self.pool)
            .await?
            .is_some();
            if is_descendant {
                bail!("public folder cannot be moved under its descendant");
            }
            Some(
                self.fetch_public_folder_row(input.account_id, parent_folder_id)
                    .await?,
            )
        } else {
            None
        };
        let display_name = input
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&current.display_name);
        let folder_class = input
            .folder_class
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&current.folder_class);
        let sort_order = input.sort_order.unwrap_or(current.sort_order);
        let display_name_changed = display_name != current.display_name;
        let parent_changed = target_parent.is_some();
        if display_name_changed && !parent_changed {
            let has_children = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT 1::bigint
                FROM public_folders
                WHERE tenant_id = $1
                  AND parent_folder_id = $2
                  AND lifecycle_state <> 'deleted'
                LIMIT 1
                "#,
            )
            .bind(&access.tenant_id)
            .bind(input.folder_id)
            .fetch_optional(&self.pool)
            .await?
            .is_some();
            if has_children {
                bail!("public folder with active children cannot be renamed");
            }
        }
        let path_changed = display_name_changed || parent_changed;
        let path = if let Some(parent) = target_parent.as_ref() {
            format!("{}/{}", parent.path.trim_end_matches('/'), display_name)
        } else if display_name_changed {
            match current.parent_folder_id {
                Some(parent_folder_id) => {
                    let parent = self
                        .fetch_public_folder_row(input.account_id, parent_folder_id)
                        .await?;
                    format!("{}/{}", parent.path.trim_end_matches('/'), display_name)
                }
                None => format!("/{display_name}"),
            }
        } else {
            current.path.clone()
        };
        let mut tx = self.pool.begin().await?;
        if path_changed {
            let new_parent_id = target_parent
                .as_ref()
                .map(|parent| parent.id)
                .or(current.parent_folder_id);
            let new_tree_id = target_parent
                .as_ref()
                .map(|parent| parent.tree_id)
                .unwrap_or(current.tree_id);
            sqlx::query(
                r#"
                WITH RECURSIVE subtree AS (
                    SELECT id, path
                    FROM public_folders
                    WHERE tenant_id = $1
                      AND id = $2
                      AND lifecycle_state <> 'deleted'
                    UNION ALL
                    SELECT child.id, child.path
                    FROM public_folders child
                    JOIN subtree parent
                      ON parent.id = child.parent_folder_id
                    WHERE child.tenant_id = $1
                      AND child.lifecycle_state <> 'deleted'
                )
                UPDATE public_folders f
                SET tree_id = $3,
                    parent_folder_id = CASE WHEN f.id = $2 THEN $4 ELSE f.parent_folder_id END,
                    display_name = CASE WHEN f.id = $2 THEN $5 ELSE f.display_name END,
                    folder_class = CASE WHEN f.id = $2 THEN $6 ELSE f.folder_class END,
                    path = CASE
                        WHEN f.id = $2 THEN $7
                        ELSE $7 || substring(f.path from char_length($8) + 1)
                    END,
                    sort_order = CASE WHEN f.id = $2 THEN $9 ELSE f.sort_order END,
                    change_counter = f.change_counter + 1,
                    updated_at = NOW()
                FROM subtree
                WHERE f.tenant_id = $1
                  AND f.id = subtree.id
                "#,
            )
            .bind(&access.tenant_id)
            .bind(input.folder_id)
            .bind(new_tree_id)
            .bind(new_parent_id)
            .bind(display_name)
            .bind(folder_class)
            .bind(&path)
            .bind(&current.path)
            .bind(sort_order)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                r#"
                UPDATE public_folders
                SET display_name = $3,
                    folder_class = $4,
                    path = $5,
                    sort_order = $6,
                    change_counter = change_counter + 1,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND id = $2
                  AND lifecycle_state <> 'deleted'
                "#,
            )
            .bind(&access.tenant_id)
            .bind(input.folder_id)
            .bind(display_name)
            .bind(folder_class)
            .bind(&path)
            .bind(sort_order)
            .execute(&mut *tx)
            .await?;
        }
        if current.parent_folder_id.is_none() && display_name_changed {
            sqlx::query(
                r#"
                UPDATE public_folder_trees
                SET display_name = $3,
                    updated_at = NOW()
                WHERE tenant_id = $1 AND id = $2
                "#,
            )
            .bind(&access.tenant_id)
            .bind(current.tree_id)
            .bind(display_name)
            .execute(&mut *tx)
            .await?;
        }
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            input.folder_id,
            "public_folder",
            input.folder_id,
            "updated",
            json!({"folderId": input.folder_id, "treeId": current.tree_id}),
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        self.fetch_public_folder(input.account_id, input.folder_id)
            .await
    }

    pub async fn delete_public_folder(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_tree_admin(account_id, access)?;
        let folder = self.fetch_public_folder_row(account_id, folder_id).await?;
        if folder.parent_folder_id.is_none() {
            bail!("public folder tree root cannot be deleted");
        }
        let has_children = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM public_folders
            WHERE tenant_id = $1
              AND parent_folder_id = $2
              AND lifecycle_state <> 'deleted'
            LIMIT 1
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_optional(&self.pool)
        .await?
        .is_some();
        if has_children {
            bail!("public folder with active children cannot be deleted");
        }
        let has_items = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1::bigint
            FROM public_folder_items
            WHERE tenant_id = $1
              AND public_folder_id = $2
              AND lifecycle_state = 'active'
            LIMIT 1
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_optional(&self.pool)
        .await?
        .is_some();
        if has_items {
            bail!("public folder with active items cannot be deleted");
        }
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE public_folders
            SET lifecycle_state = 'deleted',
                change_counter = change_counter + 1,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND id = $2
              AND lifecycle_state <> 'deleted'
            RETURNING change_counter
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(deleted_modseq) = deleted else {
            bail!("public folder not found");
        };
        let cursor = self
            .record_public_folder_change(
                &mut tx,
                &access,
                account_id,
                folder_id,
                "public_folder",
                folder_id,
                "destroyed",
                json!({"folderId": folder_id, "treeId": folder.tree_id}),
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, collection_id, object_kind, object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'public_folder', $5, $6, $7, 'delete')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(access.tree_admin_owner_account_id)
        .bind(folder.parent_folder_id)
        .bind(folder_id)
        .bind(deleted_modseq)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_public_folder_items(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<Vec<PublicFolderItem>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        let rows = sqlx::query_as::<_, PublicFolderItemRow>(&public_folder_item_select_sql(
            "WHERE i.tenant_id = $1 AND i.public_folder_id = $2 AND i.lifecycle_state = 'active'",
        ))
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder_item).collect())
    }

    pub async fn fetch_public_folder_items_by_ids(
        &self,
        account_id: Uuid,
        item_ids: &[Uuid],
    ) -> Result<Vec<PublicFolderItem>> {
        if item_ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, PublicFolderItemRow>(&public_folder_item_select_sql(
            r#"
            WHERE i.tenant_id = $1
              AND i.id = ANY($2)
              AND i.lifecycle_state = 'active'
              AND EXISTS (
                  SELECT 1
                  FROM public_folders f
                  JOIN public_folder_trees t
                    ON t.tenant_id = f.tenant_id
                   AND t.id = f.tree_id
                   AND t.lifecycle_state = 'active'
                  LEFT JOIN public_folder_permissions p
                    ON p.tenant_id = f.tenant_id
                   AND p.public_folder_id = f.id
                   AND p.principal_account_id = $3
                  WHERE f.tenant_id = i.tenant_id
                    AND f.id = i.public_folder_id
                    AND f.lifecycle_state <> 'deleted'
                    AND (t.admin_owner_account_id = $3 OR COALESCE(p.may_read, FALSE))
              )
            "#,
        ))
        .bind(&tenant_id)
        .bind(item_ids)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder_item).collect())
    }

    pub async fn upsert_public_folder_item(
        &self,
        input: UpsertPublicFolderItemInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolderItem> {
        let access = self
            .public_folder_access(input.account_id, input.public_folder_id)
            .await?;
        ensure_write(access)?;
        let subject = input.subject.trim();
        if subject.is_empty() && input.body_text.trim().is_empty() {
            bail!("public folder item subject or body is required");
        }
        let item_id = input.id.unwrap_or_else(Uuid::new_v4);
        let source_payload_json = if input.source_payload_json.trim().is_empty() {
            "{}"
        } else {
            input.source_payload_json.trim()
        };
        let existed = input.id.is_some();
        let mut tx = self.pool.begin().await?;
        if existed {
            let found = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT 1::bigint
                FROM public_folder_items
                WHERE tenant_id = $1
                  AND public_folder_id = $2
                  AND id = $3
                  AND lifecycle_state = 'active'
                LIMIT 1
                "#,
            )
            .bind(&access.tenant_id)
            .bind(input.public_folder_id)
            .bind(item_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
            if !found {
                bail!("public folder item not found");
            }
        }
        let row = sqlx::query_as::<_, PublicFolderItemRow>(
            r#"
            INSERT INTO public_folder_items (
                id, tenant_id, public_folder_id, item_kind, message_class, subject,
                body_text, body_html_sanitized, source_payload_json,
                created_by_account_id, updated_by_account_id
            )
            VALUES ($1, $2, $3, lower($4), COALESCE(NULLIF($5, ''), 'IPM.Post'), $6, $7, $8, $9::jsonb, $10, $10)
            ON CONFLICT (id) DO UPDATE SET
                item_kind = EXCLUDED.item_kind,
                message_class = EXCLUDED.message_class,
                subject = EXCLUDED.subject,
                body_text = EXCLUDED.body_text,
                body_html_sanitized = EXCLUDED.body_html_sanitized,
                source_payload_json = EXCLUDED.source_payload_json,
                updated_by_account_id = EXCLUDED.updated_by_account_id,
                change_counter = public_folder_items.change_counter + 1,
                updated_at = NOW()
            WHERE public_folder_items.tenant_id = EXCLUDED.tenant_id
              AND public_folder_items.public_folder_id = EXCLUDED.public_folder_id
            RETURNING
                id,
                public_folder_id,
                message_id,
                item_kind,
                message_class,
                subject,
                body_text,
                body_html_sanitized,
                source_payload_json::text AS source_payload_json,
                lifecycle_state,
                change_counter,
                created_by_account_id,
                updated_by_account_id,
                FALSE AS is_read,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(item_id)
        .bind(&access.tenant_id)
        .bind(input.public_folder_id)
        .bind(input.item_kind.trim())
        .bind(input.message_class.trim())
        .bind(subject)
        .bind(input.body_text.trim())
        .bind(input.body_html_sanitized.as_deref().map(str::trim))
        .bind(source_payload_json)
        .bind(input.account_id)
        .fetch_one(&mut *tx)
        .await?;
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            input.public_folder_id,
            "public_folder_item",
            item_id,
            if existed { "updated" } else { "created" },
            json!({"folderId": input.public_folder_id}),
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(map_public_folder_item(row))
    }

    pub async fn delete_public_folder_item(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        item_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_delete(access)?;
        let mut tx = self.pool.begin().await?;
        let deleted_modseq = sqlx::query_scalar::<_, i64>(
            r#"
            UPDATE public_folder_items
            SET lifecycle_state = 'deleted',
                change_counter = change_counter + 1,
                updated_by_account_id = $4,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND public_folder_id = $2
              AND id = $3
              AND lifecycle_state = 'active'
            RETURNING change_counter
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(item_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder item not found"))?;
        let cursor = self
            .record_public_folder_change(
                &mut tx,
                &access,
                account_id,
                folder_id,
                "public_folder_item",
                item_id,
                "destroyed",
                json!({"folderId": folder_id}),
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, collection_id, object_kind, object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'public_folder_item', $5, $6, $7, 'delete')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(access.tree_admin_owner_account_id)
        .bind(folder_id)
        .bind(item_id)
        .bind(deleted_modseq)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_public_folder_permissions(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<Vec<PublicFolderPermission>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_share(access)?;
        let rows = sqlx::query_as::<_, PublicFolderPermissionRow>(
            r#"
            SELECT
                p.id,
                p.public_folder_id,
                p.principal_account_id,
                a.primary_email AS principal_email,
                a.display_name AS principal_display_name,
                p.may_read,
                p.may_write,
                p.may_delete,
                p.may_share,
                to_char(p.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_permissions p
            JOIN accounts a
              ON a.tenant_id = p.tenant_id
             AND a.id = p.principal_account_id
            WHERE p.tenant_id = $1 AND p.public_folder_id = $2
            ORDER BY a.primary_email ASC, p.id ASC
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder_permission).collect())
    }

    pub async fn upsert_public_folder_permission(
        &self,
        input: PublicFolderPermissionInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolderPermission> {
        validate_collaboration_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        let access = self
            .public_folder_access(input.account_id, input.public_folder_id)
            .await?;
        ensure_share(access)?;
        let same_tenant = sqlx::query_scalar::<_, i64>(
            "SELECT 1::bigint FROM accounts WHERE tenant_id = $1 AND id = $2 LIMIT 1",
        )
        .bind(&access.tenant_id)
        .bind(input.principal_account_id)
        .fetch_optional(&self.pool)
        .await?
        .is_some();
        if !same_tenant {
            bail!("public folder permission principal not found");
        }
        let mut tx = self.pool.begin().await?;
        let permission = sqlx::query(
            r#"
            INSERT INTO public_folder_permissions (
                id, tenant_id, public_folder_id, principal_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (tenant_id, public_folder_id, principal_account_id)
            DO UPDATE SET
                may_read = EXCLUDED.may_read,
                may_write = EXCLUDED.may_write,
                may_delete = EXCLUDED.may_delete,
                may_share = EXCLUDED.may_share,
                updated_at = NOW()
            RETURNING id, (xmax = 0) AS inserted
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(input.public_folder_id)
        .bind(input.principal_account_id)
        .bind(input.may_read)
        .bind(input.may_write)
        .bind(input.may_delete)
        .bind(input.may_share)
        .fetch_one(&mut *tx)
        .await?;
        let permission_id = permission.try_get::<Uuid, _>("id")?;
        let change_kind = if permission.try_get::<bool, _>("inserted")? {
            "created"
        } else {
            "updated"
        };
        self.record_public_folder_change_with_extra_affected(
            &mut tx,
            &access,
            input.account_id,
            input.public_folder_id,
            "public_folder_permission",
            permission_id,
            change_kind,
            json!({
                "folderId": input.public_folder_id,
                "principalAccountId": input.principal_account_id
            }),
            &[input.principal_account_id],
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        self.fetch_public_folder_permission(
            input.account_id,
            input.public_folder_id,
            input.principal_account_id,
        )
        .await
    }

    pub async fn delete_public_folder_permission(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        principal_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_share(access)?;
        let mut tx = self.pool.begin().await?;
        let permission_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            DELETE FROM public_folder_permissions
            WHERE tenant_id = $1
              AND public_folder_id = $2
              AND principal_account_id = $3
            RETURNING id
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(principal_account_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder permission not found"))?;
        let cursor = self
            .record_public_folder_change_with_extra_affected(
                &mut tx,
                &access,
                account_id,
                folder_id,
                "public_folder_permission",
                permission_id,
                "destroyed",
                json!({"folderId": folder_id, "principalAccountId": principal_account_id}),
                &[principal_account_id],
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, collection_id, object_kind, object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'public_folder_permission', $5, $6, $7, 'delete')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(access.tree_admin_owner_account_id)
        .bind(folder_id)
        .bind(permission_id)
        .bind(cursor)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_public_folder_replicas(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<Vec<PublicFolderReplica>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        let rows = sqlx::query_as::<_, PublicFolderReplicaRow>(
            r#"
            SELECT
                id,
                public_folder_id,
                server_name,
                lifecycle_state,
                sort_order,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_replicas
            WHERE tenant_id = $1
              AND public_folder_id = $2
              AND lifecycle_state = 'active'
            ORDER BY sort_order ASC, server_name ASC, id ASC
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_public_folder_replica).collect())
    }

    pub async fn upsert_public_folder_replica(
        &self,
        input: PublicFolderReplicaInput,
        audit: AuditEntryInput,
    ) -> Result<PublicFolderReplica> {
        let server_name = input.server_name.trim();
        if server_name.is_empty() {
            bail!("public folder replica server name is required");
        }
        let access = self
            .public_folder_access(input.account_id, input.public_folder_id)
            .await?;
        ensure_share(access)?;
        let mut tx = self.pool.begin().await?;
        let replica = sqlx::query(
            r#"
            INSERT INTO public_folder_replicas (
                id, tenant_id, public_folder_id, server_name, lifecycle_state, sort_order
            )
            VALUES ($1, $2, $3, $4, 'active', $5)
            ON CONFLICT (tenant_id, public_folder_id, server_name)
            DO UPDATE SET
                lifecycle_state = 'active',
                sort_order = EXCLUDED.sort_order,
                updated_at = NOW()
            RETURNING
                id,
                public_folder_id,
                server_name,
                lifecycle_state,
                sort_order,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
                (xmax = 0) AS inserted
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(input.public_folder_id)
        .bind(server_name)
        .bind(input.sort_order.unwrap_or(0))
        .fetch_one(&mut *tx)
        .await?;
        let inserted = replica.try_get::<bool, _>("inserted")?;
        let replica = PublicFolderReplicaRow {
            id: replica.try_get("id")?,
            public_folder_id: replica.try_get("public_folder_id")?,
            server_name: replica.try_get("server_name")?,
            lifecycle_state: replica.try_get("lifecycle_state")?,
            sort_order: replica.try_get("sort_order")?,
            created_at: replica.try_get("created_at")?,
            updated_at: replica.try_get("updated_at")?,
        };
        let change_kind = if inserted { "created" } else { "updated" };
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            input.public_folder_id,
            "public_folder_replica",
            replica.id,
            change_kind,
            json!({"folderId": input.public_folder_id, "serverName": server_name}),
        )
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(map_public_folder_replica(replica))
    }

    pub async fn delete_public_folder_replica(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        replica_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_share(access)?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE public_folder_replicas
            SET lifecycle_state = 'deleted', updated_at = NOW()
            WHERE tenant_id = $1
              AND public_folder_id = $2
              AND id = $3
              AND lifecycle_state <> 'deleted'
            RETURNING server_name
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(replica_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(server_name) = deleted else {
            bail!("public folder replica not found");
        };
        let cursor = self
            .record_public_folder_change(
                &mut tx,
                &access,
                account_id,
                folder_id,
                "public_folder_replica",
                replica_id,
                "destroyed",
                json!({"folderId": folder_id, "serverName": server_name}),
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, collection_id, object_kind, object_id,
                deleted_modseq, change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'public_folder_replica', $5, $6, $7, 'delete')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&access.tenant_id)
        .bind(access.tree_admin_owner_account_id)
        .bind(folder_id)
        .bind(replica_id)
        .bind(cursor)
        .bind(cursor)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &access.tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_public_folder_per_user_state(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<Vec<PublicFolderPerUserState>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        let rows = sqlx::query_as::<_, PublicFolderPerUserStateRow>(
            r#"
            SELECT
                state.public_folder_id,
                state.item_id,
                state.account_id,
                state.is_read,
                state.last_seen_change,
                state.private_json::text AS private_json,
                to_char(state.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_per_user_state state
            JOIN public_folder_items item
              ON item.tenant_id = state.tenant_id
             AND item.public_folder_id = state.public_folder_id
             AND item.id = state.item_id
             AND item.lifecycle_state = 'active'
            WHERE state.tenant_id = $1
              AND state.public_folder_id = $2
              AND state.account_id = $3
            ORDER BY state.updated_at DESC, state.item_id ASC
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(map_public_folder_per_user_state)
            .collect())
    }

    pub async fn patch_public_folder_per_user_state(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        patches: &[PublicFolderPerUserStatePatch],
    ) -> Result<Vec<PublicFolderPerUserState>> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        ensure_read(access)?;
        let mut tx = self.pool.begin().await?;
        for patch in patches {
            let item_change_counter = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT change_counter
                FROM public_folder_items
                WHERE tenant_id = $1
                  AND public_folder_id = $2
                  AND id = $3
                  AND lifecycle_state = 'active'
                LIMIT 1
                "#,
            )
            .bind(&access.tenant_id)
            .bind(folder_id)
            .bind(patch.item_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("public folder item not found"))?;
            let last_seen_change = patch.last_seen_change.unwrap_or(item_change_counter);
            let private_json = patch
                .private_json
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("{}");
            let state = sqlx::query(
                r#"
                INSERT INTO public_folder_per_user_state (
                    tenant_id, public_folder_id, item_id, account_id,
                    is_read, last_seen_change, private_json
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                ON CONFLICT (tenant_id, public_folder_id, item_id, account_id)
                DO UPDATE SET
                    is_read = EXCLUDED.is_read,
                    last_seen_change = EXCLUDED.last_seen_change,
                    private_json = EXCLUDED.private_json,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
                "#,
            )
            .bind(&access.tenant_id)
            .bind(folder_id)
            .bind(patch.item_id)
            .bind(account_id)
            .bind(patch.is_read)
            .bind(last_seen_change)
            .bind(private_json)
            .fetch_one(&mut *tx)
            .await?;
            let change_kind = if state.try_get::<bool, _>("inserted")? {
                "created"
            } else {
                "updated"
            };
            self.record_public_folder_private_change(
                &mut tx,
                &access,
                account_id,
                "public_folder_per_user_state",
                patch.item_id,
                change_kind,
                json!({"folderId": folder_id, "itemId": patch.item_id}),
            )
            .await?;
        }
        tx.commit().await?;
        self.fetch_public_folder_per_user_state(account_id, folder_id)
            .await
    }

    async fn public_folder_access(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<PublicFolderAccess> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, (Uuid, bool, bool, bool, bool)>(
            r#"
            SELECT
                t.admin_owner_account_id,
                CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_read, FALSE) END AS may_read,
                CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_write, FALSE) END AS may_write,
                CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_delete, FALSE) END AS may_delete,
                CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_share, FALSE) END AS may_share
            FROM public_folders f
            JOIN public_folder_trees t
              ON t.tenant_id = f.tenant_id
             AND t.id = f.tree_id
            LEFT JOIN public_folder_permissions p
              ON p.tenant_id = f.tenant_id
             AND p.public_folder_id = f.id
             AND p.principal_account_id = $2
            WHERE f.tenant_id = $1
              AND f.id = $3
              AND f.lifecycle_state <> 'deleted'
              AND t.lifecycle_state = 'active'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(folder_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder not found"))?;
        Ok(PublicFolderAccess {
            tenant_id,
            tree_admin_owner_account_id: row.0,
            may_read: row.1,
            may_write: row.2,
            may_delete: row.3,
            may_share: row.4,
        })
    }

    async fn fetch_public_folder_row(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
    ) -> Result<PublicFolder> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, PublicFolderRow>(&public_folder_select_sql(
            "WHERE f.tenant_id = $1 AND f.id = $3 AND f.lifecycle_state <> 'deleted'",
        ))
        .bind(&tenant_id)
        .bind(account_id)
        .bind(folder_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder not found"))?;
        Ok(map_public_folder(row))
    }

    async fn fetch_public_folder_permission(
        &self,
        account_id: Uuid,
        folder_id: Uuid,
        principal_account_id: Uuid,
    ) -> Result<PublicFolderPermission> {
        let access = self.public_folder_access(account_id, folder_id).await?;
        let row = sqlx::query_as::<_, PublicFolderPermissionRow>(
            r#"
            SELECT
                p.id,
                p.public_folder_id,
                p.principal_account_id,
                a.primary_email AS principal_email,
                a.display_name AS principal_display_name,
                p.may_read,
                p.may_write,
                p.may_delete,
                p.may_share,
                to_char(p.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_permissions p
            JOIN accounts a
              ON a.tenant_id = p.tenant_id
             AND a.id = p.principal_account_id
            WHERE p.tenant_id = $1
              AND p.public_folder_id = $2
              AND p.principal_account_id = $3
            LIMIT 1
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(principal_account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("public folder permission not found"))?;
        Ok(map_public_folder_permission(row))
    }
}
