---
type: Rust Function
title: validate_mapi_event_attachment_changes
resource: crates/lpe-storage/src/attachments.rs#L1089-L1122
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_create_input
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input
---

# Signature

`pub(crate) fn validate_mapi_event_attachment_changes( changes: &MapiEventAttachmentChanges, ) -> Result<()>`

# Called by

- [validate_mapi_event_create_input](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_create_input.md)
- [validate_mapi_event_commit_input](../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input.md)