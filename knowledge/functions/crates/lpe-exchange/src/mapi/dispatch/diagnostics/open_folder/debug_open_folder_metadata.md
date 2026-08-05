---
type: Rust Function
title: debug_open_folder_metadata
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder.rs#L29-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/open_folder_debug_metadata_uses_real_dynamic_mailbox_values
---

# Signature

`pub(in crate::mapi::dispatch) fn debug_open_folder_metadata( folder_id: u64, mailboxes: &[JmapMailbox], ) -> (String, String, String)`

# Calls

- [folder_row_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [mapi_mailbox_display_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name.md)
- [folder_message_class](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)
- [post_hierarchy_probe_folder_name](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name.md)
- [debug_role_for_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [debug_container_class_for_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id.md)

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_open_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [open_folder_debug_metadata_uses_real_dynamic_mailbox_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/open_folder_debug_metadata_uses_real_dynamic_mailbox_values.md)