---
type: Rust Function
title: requested_task_list_id
resource: crates/lpe-exchange/src/service/ews/tasks.rs#L179-L186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`fn requested_task_list_id(request: &str) -> Result<Option<Uuid>>`

# Calls

- [requested_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id.md)

# Called by

- [parse_create_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)