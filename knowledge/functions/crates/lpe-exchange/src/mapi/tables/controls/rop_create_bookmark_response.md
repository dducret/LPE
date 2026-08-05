---
type: Rust Function
title: rop_create_bookmark_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L147-L180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_position
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/create_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables
---

# Signature

`pub(in crate::mapi) fn rop_create_bookmark_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [table_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_position.md)
- [table_bookmark_state_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut.md)

# Called by

- [create_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/create_bookmark_response.md)
- [bookmark_seek_preserves_global_position_for_windowed_content_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables.md)