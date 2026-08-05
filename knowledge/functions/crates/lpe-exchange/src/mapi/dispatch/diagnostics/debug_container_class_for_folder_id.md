---
type: Rust Function
title: debug_container_class_for_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L175-L197
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/expected_special_folder_container_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
---

# Signature

`pub(in crate::mapi) fn debug_container_class_for_folder_id(folder_id: u64) -> &'static str`

# Calls

- [expected_special_folder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/expected_special_folder_container_class.md)

# Called by

- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)