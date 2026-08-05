---
type: Rust Method
title: handle_task_list_get
resource: crates/lpe-jmap/src/tasks.rs#L24-L64
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/tasks/task_list_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/tasks/task_list_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_task_list_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [task_list_properties](../../../../../../functions/crates/lpe-jmap/src/tasks/task_list_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [task_list_to_value](../../../../../../functions/crates/lpe-jmap/src/tasks/task_list_to_value.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)