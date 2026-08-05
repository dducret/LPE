---
type: Rust Function
title: rop_get_properties_list_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L712-L754
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/default_folder_property_tags_with_identity
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response
---

# Signature

`pub(in crate::mapi) fn rop_get_properties_list_response( request: &RopRequest, object: Option<&MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [default_store_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags.md)
- [default_folder_property_tags_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/default_folder_property_tags_with_identity.md)
- [default_attachment_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns.md)
- [default_contact_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags.md)
- [default_event_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags.md)
- [default_task_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags.md)
- [default_note_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags.md)
- [default_journal_entry_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags.md)
- [default_conversation_action_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags.md)
- [default_message_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)
- [default_folder_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)

# Called by

- [append_get_properties_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response.md)