---
type: Rust Function
title: pending_html_property
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L551-L557
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
---

# Signature

`pub(in crate::mapi) fn pending_html_property( properties: &HashMap<u32, MapiValue>, ) -> Option<String>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [pending_html_binary_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)