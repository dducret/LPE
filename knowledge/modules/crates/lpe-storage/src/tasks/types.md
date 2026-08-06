---
type: Rust Module
title: types
resource: crates/lpe-storage/src/tasks/types.rs#L1-L226
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/serde-serialize
  - external/uuid-uuid
  - external/crate-collaboration-collaborationrights-clienttasklistrow-clienttaskrow-davtaskrow-tasklistgrantrow
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [TaskListGrant](../../../../../classes/crates/lpe-storage/src/tasks/types/TaskListGrant.md)
- [TaskListGrantInput](../../../../../classes/crates/lpe-storage/src/tasks/types/TaskListGrantInput.md)
- [ClientTaskList](../../../../../classes/crates/lpe-storage/src/tasks/types/ClientTaskList.md)
- [ClientTask](../../../../../classes/crates/lpe-storage/src/tasks/types/ClientTask.md)
- [DavTask](../../../../../classes/crates/lpe-storage/src/tasks/types/DavTask.md)
- [CreateTaskListInput](../../../../../classes/crates/lpe-storage/src/tasks/types/CreateTaskListInput.md)
- [UpdateTaskListInput](../../../../../classes/crates/lpe-storage/src/tasks/types/UpdateTaskListInput.md)
- [UpsertClientTaskInput](../../../../../classes/crates/lpe-storage/src/tasks/types/UpsertClientTaskInput.md)
- [map_task_list](../../../../../functions/crates/lpe-storage/src/tasks/types/map_task_list.md)
- [map_task_list_grant](../../../../../functions/crates/lpe-storage/src/tasks/types/map_task_list_grant.md)
- [map_task](../../../../../functions/crates/lpe-storage/src/tasks/types/map_task.md)
- [map_dav_task](../../../../../functions/crates/lpe-storage/src/tasks/types/map_dav_task.md)

# Imports

- `serde::Serialize`
- `uuid::Uuid`
- `crate::{
    collaboration::CollaborationRights, ClientTaskListRow, ClientTaskRow, DavTaskRow,
    TaskListGrantRow,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)