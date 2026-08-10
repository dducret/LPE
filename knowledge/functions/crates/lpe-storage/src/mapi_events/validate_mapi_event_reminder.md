---
type: Rust Function
title: validate_mapi_event_reminder
resource: crates/lpe-storage/src/mapi_events.rs#L838-L851
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_create_input
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input
---

# Signature

`fn validate_mapi_event_reminder(reminder: &MapiEventReminderPatch) -> Result<()>`

# Called by

- [validate_mapi_event_create_input](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_create_input.md)
- [validate_mapi_event_commit_input](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input.md)