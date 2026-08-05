---
type: Rust Function
title: rop_buffer_is_store_independent_special_folder_getprops_probe
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L77-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/read_handle_table
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_special_folder
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe
---

# Signature

`pub(super) fn rop_buffer_is_store_independent_special_folder_getprops_probe( rop_buffer: &[u8], session: &MapiSession, ) -> bool`

# Calls

- [read_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/read_handle_table.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [resolve_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [is_store_independent_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_special_folder.md)
- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [is_store_independent_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/is_store_independent_folder_getprops_probe.md)