---
type: Rust Method
title: handle_task_list_set
resource: crates/lpe-jmap/src/tasks.rs#L84-L176
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/tasks/task_list_properties
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_create
  - functions/crates/lpe-jmap/src/tasks/task_list_to_value
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_update
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy
---

# Signature

`pub(crate) async fn handle_task_list_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [task_list_properties](../../../../../../functions/crates/lpe-jmap/src/tasks/task_list_properties.md)
- [parse_task_list_create](../../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_create.md)
- [task_list_to_value](../../../../../../functions/crates/lpe-jmap/src/tasks/task_list_to_value.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [parse_task_list_update](../../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_update.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_canonical_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_import_or_copy.md)