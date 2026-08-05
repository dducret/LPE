---
type: Rust Function
title: rop_seek_row_bookmark_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L182-L240
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_bookmark_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_row_count
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_want_row_moved_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_bookmark_request_validation_matches_microsoft_boolean_values
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_seek_row_bookmark_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [seek_row_bookmark_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_bookmark_request_is_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_columns_are_available](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [table_bookmark_state_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [bookmark](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [bookmark_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_row_count.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [bookmark_want_row_moved_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark_want_row_moved_count.md)

# Called by

- [seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_bookmark_response.md)
- [bookmark_seek_preserves_global_position_for_windowed_content_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_preserves_global_position_for_windowed_content_tables.md)
- [bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted.md)
- [seek_row_bookmark_request_validation_matches_microsoft_boolean_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_bookmark_request_validation_matches_microsoft_boolean_values.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)