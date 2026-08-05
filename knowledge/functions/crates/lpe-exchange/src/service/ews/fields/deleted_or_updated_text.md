---
type: Rust Function
title: deleted_or_updated_text
resource: crates/lpe-exchange/src/service/ews/fields.rs#L17-L29
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
---

# Signature

`pub(in crate::service) fn deleted_or_updated_text( request: &str, container: &str, field_uri: &str, local_name: &str, existing: &str, ) -> String`

# Calls

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_update_contact_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [parse_update_public_folder_item_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input.md)
- [parse_update_task_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)