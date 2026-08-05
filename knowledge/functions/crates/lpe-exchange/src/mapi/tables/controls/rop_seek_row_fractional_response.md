---
type: Rust Function
title: rop_seek_row_fractional_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L257-L287
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_fractional_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_position_mut
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fractional_position
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_fractional_response
---

# Signature

`pub(in crate::mapi) fn rop_seek_row_fractional_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [seek_row_fractional_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/seek_row_fractional_request_is_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [table_position_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_position_mut.md)
- [fractional_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fractional_position.md)

# Called by

- [seek_row_fractional_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_fractional_response.md)