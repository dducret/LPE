---
type: Rust Module
title: tasks
resource: crates/lpe-storage/src/tasks.rs#L1-L1537
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-types-chrono-datetime-fixedoffset
  - external/sqlx-postgres
  - external/uuid-uuid
  - external/crate-collaboration-validate-collaboration-rights-normalize-email-normalize-task-list-name-normalize-task-status-auditentryinput-canonicalchangecategory-clienttasklistrow-clienttaskrow-davtaskrow-storage-tasklistgrantrow-default-task-list-name-default-task-list-role
  - external/pub-crate-use-types-map-dav-task-map-task-map-task-list-map-task-list-grant
  - external/pub-use-types-clienttask-clienttasklist-createtasklistinput-davtask-tasklistgrant-tasklistgrantinput-updatetasklistinput-upsertclienttaskinput
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [upsert_client_task](../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_client_task.md)
- [update_accessible_task_reminder](../../../../functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder.md)
- [upsert_task_list_grant](../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_task_list_grant.md)
- [delete_task_list_grant](../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list_grant.md)
- [fetch_task_list_grant](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_list_grant.md)
- [fetch_outgoing_task_list_grants](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_outgoing_task_list_grants.md)
- [fetch_task_lists](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists.md)
- [fetch_task_lists_by_ids](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_task_lists_by_ids.md)
- [create_task_list](../../../../functions/crates/lpe-storage/src/tasks/Storage/create_task_list.md)
- [update_task_list](../../../../functions/crates/lpe-storage/src/tasks/Storage/update_task_list.md)
- [delete_task_list](../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_task_list.md)
- [delete_client_task](../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_client_task.md)
- [fetch_dav_tasks](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks.md)
- [fetch_dav_tasks_by_ids](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_dav_tasks_by_ids.md)
- [upsert_dav_task](../../../../functions/crates/lpe-storage/src/tasks/Storage/upsert_dav_task.md)
- [delete_dav_task](../../../../functions/crates/lpe-storage/src/tasks/Storage/delete_dav_task.md)
- [fetch_client_tasks](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks.md)
- [fetch_client_tasks_by_ids](../../../../functions/crates/lpe-storage/src/tasks/Storage/fetch_client_tasks_by_ids.md)
- [ensure_default_task_list](../../../../functions/crates/lpe-storage/src/tasks/Storage/ensure_default_task_list.md)
- [load_task_list_in_tx](../../../../functions/crates/lpe-storage/src/tasks/Storage/load_task_list_in_tx.md)
- [parse_task_timestamp](../../../../functions/crates/lpe-storage/src/tasks/parse_task_timestamp.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::types::chrono::{DateTime, FixedOffset}`
- `sqlx::Postgres`
- `uuid::Uuid`
- `crate::{
    collaboration::validate_collaboration_rights, normalize_email, normalize_task_list_name,
    normalize_task_status, AuditEntryInput, CanonicalChangeCategory, ClientTaskListRow,
    ClientTaskRow, DavTaskRow, Storage, TaskListGrantRow, DEFAULT_TASK_LIST_NAME,
    DEFAULT_TASK_LIST_ROLE,
}`
- `pub(crate) use types::{map_dav_task, map_task, map_task_list, map_task_list_grant}`
- `pub use types::{
    ClientTask, ClientTaskList, CreateTaskListInput, DavTask, TaskListGrant, TaskListGrantInput,
    UpdateTaskListInput, UpsertClientTaskInput,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)