---
type: Rust Function
title: validate_mapi_event_commit_input
resource: crates/lpe-storage/src/mapi_events.rs#L796-L812
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
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`fn validate_mapi_event_commit_input(input: &MapiEventCommitInput) -> Result<()>`

# Calls

- [validate_mapi_event_fields](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_fields.md)
- [validate_mapi_event_reminder](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_reminder.md)
- [validate_mapi_event_custom_properties](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_custom_properties.md)
- [validate_mapi_event_attachment_changes](../../../../../functions/crates/lpe-storage/src/attachments/validate_mapi_event_attachment_changes.md)

# Called by

- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)