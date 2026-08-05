---
type: Rust Function
title: get_properties_specific_candidate_tags
resource: crates/lpe-exchange/src/mapi/rop.rs#L888-L922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag
---

# Signature

`fn get_properties_specific_candidate_tags(object: Option<&MapiObject>) -> Vec<u32>`

# Calls

- [default_store_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags.md)
- [default_contact_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags.md)
- [default_event_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags.md)
- [default_task_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags.md)
- [default_note_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags.md)
- [default_journal_entry_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags.md)
- [default_attachment_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns.md)
- [default_message_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)
- [default_conversation_action_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags.md)
- [default_folder_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)

# Called by

- [get_properties_specific_typed_value_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_typed_value_tag.md)