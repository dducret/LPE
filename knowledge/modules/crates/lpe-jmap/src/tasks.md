---
type: Rust Module
title: tasks
resource: crates/lpe-jmap/src/tasks.rs#L1-L668
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-storage-authenticatedaccount-clienttask-clienttasklist-createtasklistinput-updatetasklistinput-upsertclienttaskinput
  - external/serde-json-json-map-value
  - external/std-collections-hashmap-hashset
  - external/uuid-uuid
  - external/crate-convert-insert-if-error-set-error-parse-parse-optional-string-parse-required-string-parse-uuid-parse-uuid-list-protocol-changesarguments-querychangesarguments-taskgetarguments-tasklistgetarguments-tasklistsetarguments-taskqueryarguments-taskqueryfilter-taskquerysort-tasksetarguments-state-query-changes-response-query-position-validation-validate-task-filter-validate-task-sort-jmapservice-default-get-limit-max-query-limit
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [handle_task_list_get](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_list_changes](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes.md)
- [handle_task_list_set](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_get](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get.md)
- [handle_task_query](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)
- [handle_task_query_changes](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)
- [handle_task_changes](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes.md)
- [handle_task_set](../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [task_list_properties](../../../../functions/crates/lpe-jmap/src/tasks/task_list_properties.md)
- [task_properties](../../../../functions/crates/lpe-jmap/src/tasks/task_properties.md)
- [task_list_to_value](../../../../functions/crates/lpe-jmap/src/tasks/task_list_to_value.md)
- [task_to_value](../../../../functions/crates/lpe-jmap/src/tasks/task_to_value.md)
- [task_matches_filter](../../../../functions/crates/lpe-jmap/src/tasks/task_matches_filter.md)
- [task_sort_key](../../../../functions/crates/lpe-jmap/src/tasks/task_sort_key.md)
- [parse_task_input](../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)
- [parse_task_list_create](../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_create.md)
- [parse_task_list_update](../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_update.md)
- [reject_unknown_task_properties](../../../../functions/crates/lpe-jmap/src/tasks/reject_unknown_task_properties.md)
- [reject_unknown_task_list_properties](../../../../functions/crates/lpe-jmap/src/tasks/reject_unknown_task_list_properties.md)
- [validate_task_list_id](../../../../functions/crates/lpe-jmap/src/tasks/validate_task_list_id.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_storage::{
    AuthenticatedAccount, ClientTask, ClientTaskList, CreateTaskListInput, UpdateTaskListInput,
    UpsertClientTaskInput,
}`
- `serde_json::{json, Map, Value}`
- `std::collections::{HashMap, HashSet}`
- `uuid::Uuid`
- `crate::{
    convert::insert_if,
    error::set_error,
    parse::{parse_optional_string, parse_required_string, parse_uuid, parse_uuid_list},
    protocol::{
        ChangesArguments, QueryChangesArguments, TaskGetArguments, TaskListGetArguments,
        TaskListSetArguments, TaskQueryArguments, TaskQueryFilter, TaskQuerySort, TaskSetArguments,
    },
    state::{query_changes_response, query_position},
    validation::{validate_task_filter, validate_task_sort},
    JmapService, DEFAULT_GET_LIMIT, MAX_QUERY_LIMIT,
}`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)