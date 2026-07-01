use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    PublicFolderItemRow, PublicFolderPerUserStateRow, PublicFolderPermissionRow,
    PublicFolderReplicaRow, PublicFolderRow, PublicFolderTreeRow,
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
    pub parent_folder_id: Option<Uuid>,
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
pub struct PublicFolderReplica {
    pub id: Uuid,
    pub public_folder_id: Uuid,
    pub server_name: String,
    pub lifecycle_state: String,
    pub sort_order: i32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicFolderReplicaInput {
    pub account_id: Uuid,
    pub public_folder_id: Uuid,
    pub server_name: String,
    pub sort_order: Option<i32>,
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
pub(crate) struct PublicFolderAccess {
    pub(crate) tenant_id: Uuid,
    pub(crate) tree_admin_owner_account_id: Uuid,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
}

pub(crate) fn ensure_read(access: PublicFolderAccess) -> Result<()> {
    if access.may_read {
        Ok(())
    } else {
        bail!("public folder read access is not granted")
    }
}

pub(crate) fn ensure_write(access: PublicFolderAccess) -> Result<()> {
    if access.may_write {
        Ok(())
    } else {
        bail!("public folder write access is not granted")
    }
}

pub(crate) fn ensure_delete(access: PublicFolderAccess) -> Result<()> {
    if access.may_delete {
        Ok(())
    } else {
        bail!("public folder delete access is not granted")
    }
}

pub(crate) fn ensure_share(access: PublicFolderAccess) -> Result<()> {
    if access.may_share {
        Ok(())
    } else {
        bail!("public folder share access is not granted")
    }
}

pub(crate) fn ensure_tree_admin(account_id: Uuid, access: PublicFolderAccess) -> Result<()> {
    if account_id == access.tree_admin_owner_account_id {
        Ok(())
    } else {
        bail!("public folder structural changes require tree owner access")
    }
}

pub(crate) fn public_folder_select_sql(where_clause: &str) -> String {
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

pub(crate) fn public_folder_item_select_sql(where_clause: &str) -> String {
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

pub(crate) fn map_public_folder_tree(row: PublicFolderTreeRow) -> PublicFolderTree {
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

pub(crate) fn map_public_folder(row: PublicFolderRow) -> PublicFolder {
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

pub(crate) fn map_public_folder_item(row: PublicFolderItemRow) -> PublicFolderItem {
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

pub(crate) fn map_public_folder_permission(
    row: PublicFolderPermissionRow,
) -> PublicFolderPermission {
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

pub(crate) fn map_public_folder_replica(row: PublicFolderReplicaRow) -> PublicFolderReplica {
    PublicFolderReplica {
        id: row.id,
        public_folder_id: row.public_folder_id,
        server_name: row.server_name,
        lifecycle_state: row.lifecycle_state,
        sort_order: row.sort_order,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_public_folder_per_user_state(
    row: PublicFolderPerUserStateRow,
) -> PublicFolderPerUserState {
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
