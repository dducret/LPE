---
type: Rust Function
title: rop_reload_cached_information_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L607-L710
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_reload_cached_information_matches_open_message_shape
---

# Signature

`pub(in crate::mapi) fn rop_reload_cached_information_response( request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)
- [message_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)

# Called by

- [append_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response.md)
- [microsoft_reload_cached_information_matches_open_message_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_reload_cached_information_matches_open_message_shape.md)