---
type: Rust Function
title: field_deleted
resource: crates/lpe-exchange/src/service/ews/fields.rs#L31-L35
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/fields/field_block_matches
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json
  - functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json
  - functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags
  - functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`pub(in crate::service) fn field_deleted(request: &str, field_uri: &str) -> bool`

# Calls

- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [field_block_matches](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_block_matches.md)

# Called by

- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [deleted_or_updated_contact_entry](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/deleted_or_updated_contact_entry.md)
- [ews_updated_contact_emails_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_emails_json.md)
- [ews_updated_contact_urls_json](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/ews_updated_contact_urls_json.md)
- [deleted_or_updated_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/deleted_or_updated_text.md)
- [parse_update_message_flags](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags.md)
- [parse_update_public_folder_item_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)