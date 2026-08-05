---
type: Rust Method
title: allocate_output_handle_avoiding
resource: crates/lpe-exchange/src/mapi/session.rs#L1034-L1059
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle
---

# Signature

`pub(in crate::mapi) fn allocate_output_handle_avoiding( &mut self, output_handle_index: Option<u8>, object: MapiObject, reserved_handles: &HashSet<u32>, ) -> u32`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [allocate_output_handle_skips_reserved_same_execute_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/allocate_output_handle_skips_reserved_same_execute_handle.md)