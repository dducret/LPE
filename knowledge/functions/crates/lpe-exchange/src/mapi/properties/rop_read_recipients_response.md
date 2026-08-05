---
type: Rust Function
title: rop_read_recipients_response
resource: crates/lpe-exchange/src/mapi/properties.rs#L69-L153
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/row_id
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_success_response_includes_row_count
  - functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_uses_row_id_value_not_vector_index
  - functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_row_zero_on_empty_message_returns_not_found
---

# Signature

`pub(in crate::mapi) fn rop_read_recipients_response( request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [row_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/row_id.md)
- [search_folder_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [rop_error_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [message_recipients](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)
- [serialize_recipient_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row.md)
- [serialize_pending_recipient_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_pending_recipient_row.md)
- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_read_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)
- [read_recipients_success_response_includes_row_count](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_success_response_includes_row_count.md)
- [read_recipients_uses_row_id_value_not_vector_index](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_uses_row_id_value_not_vector_index.md)
- [read_recipients_row_zero_on_empty_message_returns_not_found](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/read_recipients_row_zero_on_empty_message_returns_not_found.md)