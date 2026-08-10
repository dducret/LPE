---
type: Rust Function
title: validate_mapi_event_create_input
resource: crates/lpe-storage/src/mapi_events.rs#L795-L803
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_fields
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_reminder
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_custom_properties
  - functions/crates/lpe-storage/src/attachments/validate_mapi_event_attachment_changes
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
---

# Signature

`fn validate_mapi_event_create_input(input: &MapiEventCreateInput) -> Result<()>`

# Calls

- [validate_mapi_event_fields](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_fields.md)
- [validate_mapi_event_reminder](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_reminder.md)
- [validate_mapi_event_custom_properties](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_custom_properties.md)
- [validate_mapi_event_attachment_changes](../../../../../functions/crates/lpe-storage/src/attachments/validate_mapi_event_attachment_changes.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)