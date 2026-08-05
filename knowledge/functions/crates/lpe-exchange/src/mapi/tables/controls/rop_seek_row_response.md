---
type: Rust Function
title: rop_seek_row_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L102-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_position_mut
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_row_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_origin
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_row_moved_count
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_clamps_stale_current_position_to_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_request_validation_matches_microsoft_bookmark_and_boolean_values
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset
---

# Signature

`pub(in crate::mapi) fn rop_seek_row_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [seek_row_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_request_is_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [table_position_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_position_mut.md)
- [seek_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_row_count.md)
- [seek_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/seek_origin.md)
- [want_row_moved_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/want_row_moved_count.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [seek_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_response.md)
- [seek_row_clamps_stale_current_position_to_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_clamps_stale_current_position_to_row_count.md)
- [seek_row_request_validation_matches_microsoft_bookmark_and_boolean_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/seek_row_request_validation_matches_microsoft_bookmark_and_boolean_values.md)
- [inbox_associated_find_row_followup_uses_the_original_rowset](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset.md)