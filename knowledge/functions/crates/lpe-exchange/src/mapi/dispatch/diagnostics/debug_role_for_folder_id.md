---
type: Rust Function
title: debug_role_for_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L171-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi) fn debug_role_for_folder_id(folder_id: u64) -> &'static str`

# Calls

- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [post_hierarchy_probe_folder_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy_probe_folder_name.md)

# Called by

- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)