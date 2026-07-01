use serde::Serialize;
use uuid::Uuid;

use crate::{
    collaboration::CollaborationRights, ClientTaskListRow, ClientTaskRow, DavTaskRow,
    TaskListGrantRow,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskListGrant {
    pub id: Uuid,
    pub task_list_id: Uuid,
    pub task_list_name: String,
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
pub struct TaskListGrantInput {
    pub owner_account_id: Uuid,
    pub task_list_id: Uuid,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTaskList {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTask {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_sort_order: i32,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub recurrence_rule: String,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DavTask {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_name: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub recurrence_rule: String,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CreateTaskListInput {
    pub account_id: Uuid,
    pub name: String,
    pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct UpdateTaskListInput {
    pub account_id: Uuid,
    pub task_list_id: Uuid,
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UpsertClientTaskInput {
    pub id: Option<Uuid>,
    pub principal_account_id: Uuid,
    pub account_id: Uuid,
    pub task_list_id: Option<Uuid>,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub recurrence_rule: String,
    pub sort_order: i32,
}

pub(crate) fn map_task_list(row: ClientTaskListRow) -> ClientTaskList {
    ClientTaskList {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        name: row.name,
        role: row.role,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_task_list_grant(row: TaskListGrantRow) -> TaskListGrant {
    TaskListGrant {
        id: row.id,
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
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

pub(crate) fn map_task(row: ClientTaskRow) -> ClientTask {
    ClientTask {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_sort_order: row.task_list_sort_order,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        recurrence_rule: row.recurrence_rule,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

pub(crate) fn map_dav_task(row: DavTaskRow) -> DavTask {
    DavTask {
        id: row.id,
        collection_id: row.task_list_id.to_string(),
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        recurrence_rule: row.recurrence_rule,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}
