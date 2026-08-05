---
type: Rust Function
title: append_table_control_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L80-L216
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/activate_table_notifications_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/normal_inbox_table_lifecycle_details
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(super) fn append_table_control_dispatch_response( principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) -> TableControlFlow`

# Calls

- [append_set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response.md)
- [append_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [activate_table_notifications_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/activate_table_notifications_for_request.md)
- [normal_inbox_table_lifecycle_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/normal_inbox_table_lifecycle_details.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)