use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    collaboration::validate_collaboration_rights, AuditEntryInput, CanonicalChangeCategory,
    PublicFolderItemRow, PublicFolderPerUserStateRow, PublicFolderPermissionRow, PublicFolderRow,
    PublicFolderTreeRow, Storage,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderRights {
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderTree {
    pub id: Uuid,
    pub canonical_id: Uuid,
    pub display_name: String,
    pub lifecycle_state: String,
    pub admin_owner_account_id: Uuid,
    pub root_folder_id: Option<Uuid>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreatePublicFolderTreeInput {
    pub account_id: Uuid,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolder {
    pub id: Uuid,
    pub tree_id: Uuid,
    pub parent_folder_id: Option<Uuid>,
    pub canonical_id: Uuid,
    pub display_name: String,
    pub folder_class: String,
    pub path: String,
    pub sort_order: i32,
    pub lifecycle_state: String,
    pub change_counter: i64,
    pub rights: PublicFolderRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreatePublicFolderInput {
    pub account_id: Uuid,
    pub parent_folder_id: Uuid,
    pub display_name: String,
    pub folder_class: String,
    pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct UpdatePublicFolderInput {
    pub account_id: Uuid,
    pub folder_id: Uuid,
    pub display_name: Option<String>,
    pub folder_class: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderItem {
    pub id: Uuid,
    pub public_folder_id: Uuid,
    pub message_id: Option<Uuid>,
    pub item_kind: String,
    pub message_class: String,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub source_payload_json: String,
    pub lifecycle_state: String,
    pub change_counter: i64,
    pub created_by_account_id: Uuid,
    pub updated_by_account_id: Uuid,
    pub is_read: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UpsertPublicFolderItemInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub public_folder_id: Uuid,
    pub item_kind: String,
    pub message_class: String,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub source_payload_json: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderPermission {
    pub id: Uuid,
    pub public_folder_id: Uuid,
    pub principal_account_id: Uuid,
    pub principal_email: String,
    pub principal_display_name: String,
    pub rights: PublicFolderRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct PublicFolderPermissionInput {
    pub account_id: Uuid,
    pub public_folder_id: Uuid,
    pub principal_account_id: Uuid,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderPerUserState {
    pub public_folder_id: Uuid,
    pub item_id: Uuid,
    pub account_id: Uuid,
    pub is_read: bool,
    pub last_seen_change: i64,
    pub private_json: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PublicFolderPerUserStatePatch {
    pub item_id: Uuid,
    pub is_read: bool,
    pub last_seen_change: Option<i64>,
    pub private_json: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct PublicFolderAccess {
    tenant_id: Uuid,
    tree_admin_owner_account_id: Uuid,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
}

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
        if display_name_changed {
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
        let parent_path = if display_name_changed {
            match current.parent_folder_id {
                Some(parent_folder_id) => Some(
                    self.fetch_public_folder_row(input.account_id, parent_folder_id)
                        .await?
                        .path,
                ),
                None => None,
            }
        } else {
            None
        };
        let path = if display_name_changed {
            parent_path
                .map(|path| format!("{}/{}", path.trim_end_matches('/'), display_name))
                .unwrap_or_else(|| format!("/{display_name}"))
        } else {
            current.path.clone()
        };
        let mut tx = self.pool.begin().await?;
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
        let deleted = sqlx::query_scalar::<_, i64>(
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
            RETURNING 1::bigint
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .bind(item_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        if !deleted {
            bail!("public folder item not found");
        }
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
        .bind(cursor)
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
        let permission_id = sqlx::query_scalar::<_, Uuid>(
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
            RETURNING id
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
        self.record_public_folder_change(
            &mut tx,
            &access,
            input.account_id,
            input.public_folder_id,
            "public_folder_permission",
            permission_id,
            "updated",
            json!({
                "folderId": input.public_folder_id,
                "principalAccountId": input.principal_account_id
            }),
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
        self.record_public_folder_change(
            &mut tx,
            &access,
            account_id,
            folder_id,
            "public_folder_permission",
            permission_id,
            "destroyed",
            json!({"folderId": folder_id, "principalAccountId": principal_account_id}),
        )
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
                public_folder_id,
                item_id,
                account_id,
                is_read,
                last_seen_change,
                private_json::text AS private_json,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM public_folder_per_user_state
            WHERE tenant_id = $1 AND public_folder_id = $2 AND account_id = $3
            ORDER BY updated_at DESC, item_id ASC
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
            sqlx::query(
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
                "#,
            )
            .bind(&access.tenant_id)
            .bind(folder_id)
            .bind(patch.item_id)
            .bind(account_id)
            .bind(patch.is_read)
            .bind(last_seen_change)
            .bind(private_json)
            .execute(&mut *tx)
            .await?;
            self.record_public_folder_change(
                &mut tx,
                &access,
                account_id,
                folder_id,
                "public_folder_per_user_state",
                patch.item_id,
                "updated",
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

    async fn record_public_folder_change(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        access: &PublicFolderAccess,
        actor_account_id: Uuid,
        folder_id: Uuid,
        object_kind: &str,
        object_id: Uuid,
        change_kind: &str,
        summary_json: serde_json::Value,
    ) -> Result<i64> {
        let mut affected = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT principal_account_id
            FROM public_folder_permissions
            WHERE tenant_id = $1 AND public_folder_id = $2 AND may_read
            "#,
        )
        .bind(&access.tenant_id)
        .bind(folder_id)
        .fetch_all(&mut **tx)
        .await?;
        affected.push(access.tree_admin_owner_account_id);
        affected.push(actor_account_id);
        affected.sort();
        affected.dedup();
        let modseq = self
            .allocate_account_modseq_in_tx(
                tx,
                &access.tenant_id,
                access.tree_admin_owner_account_id,
                CanonicalChangeCategory::PublicFolders.as_str(),
            )
            .await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            tx,
            &access.tenant_id,
            Some(access.tree_admin_owner_account_id),
            None,
            object_kind,
            object_id,
            change_kind,
            modseq,
            &affected,
            summary_json,
        )
        .await?;
        Self::emit_canonical_change(
            tx,
            &access.tenant_id,
            CanonicalChangeCategory::PublicFolders,
            &affected,
            &[access.tree_admin_owner_account_id],
        )
        .await?;
        Ok(cursor)
    }
}

fn ensure_read(access: PublicFolderAccess) -> Result<()> {
    if access.may_read {
        Ok(())
    } else {
        bail!("public folder read access is not granted")
    }
}

fn ensure_write(access: PublicFolderAccess) -> Result<()> {
    if access.may_write {
        Ok(())
    } else {
        bail!("public folder write access is not granted")
    }
}

fn ensure_delete(access: PublicFolderAccess) -> Result<()> {
    if access.may_delete {
        Ok(())
    } else {
        bail!("public folder delete access is not granted")
    }
}

fn ensure_share(access: PublicFolderAccess) -> Result<()> {
    if access.may_share {
        Ok(())
    } else {
        bail!("public folder share access is not granted")
    }
}

fn ensure_tree_admin(account_id: Uuid, access: PublicFolderAccess) -> Result<()> {
    if account_id == access.tree_admin_owner_account_id {
        Ok(())
    } else {
        bail!("public folder structural changes require tree owner access")
    }
}

fn public_folder_select_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            f.id,
            f.tree_id,
            f.parent_folder_id,
            f.canonical_id,
            f.display_name,
            f.folder_class,
            f.path,
            f.sort_order,
            f.lifecycle_state,
            f.change_counter,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_read, FALSE) END AS may_read,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_write, FALSE) END AS may_write,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_delete, FALSE) END AS may_delete,
            CASE WHEN t.admin_owner_account_id = $2 THEN TRUE ELSE COALESCE(p.may_share, FALSE) END AS may_share,
            to_char(f.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
            to_char(f.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
        FROM public_folders f
        JOIN public_folder_trees t
          ON t.tenant_id = f.tenant_id
         AND t.id = f.tree_id
        LEFT JOIN public_folder_permissions p
          ON p.tenant_id = f.tenant_id
         AND p.public_folder_id = f.id
         AND p.principal_account_id = $2
        {where_clause}
          AND t.lifecycle_state = 'active'
          AND (t.admin_owner_account_id = $2 OR COALESCE(p.may_read, FALSE))
        ORDER BY f.sort_order ASC, f.display_name ASC, f.id ASC
        "#
    )
}

fn public_folder_item_select_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            i.id,
            i.public_folder_id,
            i.message_id,
            i.item_kind,
            i.message_class,
            i.subject,
            i.body_text,
            i.body_html_sanitized,
            i.source_payload_json::text AS source_payload_json,
            i.lifecycle_state,
            i.change_counter,
            i.created_by_account_id,
            i.updated_by_account_id,
            COALESCE(s.is_read, FALSE) AS is_read,
            to_char(i.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
            to_char(i.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
        FROM public_folder_items i
        LEFT JOIN public_folder_per_user_state s
          ON s.tenant_id = i.tenant_id
         AND s.public_folder_id = i.public_folder_id
         AND s.item_id = i.id
         AND s.account_id = $3
        {where_clause}
        ORDER BY i.updated_at DESC, i.id ASC
        "#
    )
}

fn map_public_folder_tree(row: PublicFolderTreeRow) -> PublicFolderTree {
    PublicFolderTree {
        id: row.id,
        canonical_id: row.canonical_id,
        display_name: row.display_name,
        lifecycle_state: row.lifecycle_state,
        admin_owner_account_id: row.admin_owner_account_id,
        root_folder_id: row.root_folder_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_public_folder(row: PublicFolderRow) -> PublicFolder {
    PublicFolder {
        id: row.id,
        tree_id: row.tree_id,
        parent_folder_id: row.parent_folder_id,
        canonical_id: row.canonical_id,
        display_name: row.display_name,
        folder_class: row.folder_class,
        path: row.path,
        sort_order: row.sort_order,
        lifecycle_state: row.lifecycle_state,
        change_counter: row.change_counter,
        rights: PublicFolderRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_public_folder_item(row: PublicFolderItemRow) -> PublicFolderItem {
    PublicFolderItem {
        id: row.id,
        public_folder_id: row.public_folder_id,
        message_id: row.message_id,
        item_kind: row.item_kind,
        message_class: row.message_class,
        subject: row.subject,
        body_text: row.body_text,
        body_html_sanitized: row.body_html_sanitized,
        source_payload_json: row.source_payload_json,
        lifecycle_state: row.lifecycle_state,
        change_counter: row.change_counter,
        created_by_account_id: row.created_by_account_id,
        updated_by_account_id: row.updated_by_account_id,
        is_read: row.is_read,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_public_folder_permission(row: PublicFolderPermissionRow) -> PublicFolderPermission {
    PublicFolderPermission {
        id: row.id,
        public_folder_id: row.public_folder_id,
        principal_account_id: row.principal_account_id,
        principal_email: row.principal_email,
        principal_display_name: row.principal_display_name,
        rights: PublicFolderRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_public_folder_per_user_state(row: PublicFolderPerUserStateRow) -> PublicFolderPerUserState {
    PublicFolderPerUserState {
        public_folder_id: row.public_folder_id,
        item_id: row.item_id,
        account_id: row.account_id,
        is_read: row.is_read,
        last_seen_change: row.last_seen_change,
        private_json: row.private_json,
        updated_at: row.updated_at,
    }
}
