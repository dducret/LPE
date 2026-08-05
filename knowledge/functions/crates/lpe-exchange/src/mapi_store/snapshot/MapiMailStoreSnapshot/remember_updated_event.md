---
type: Rust Method
title: remember_updated_event
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L580-L598
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`pub(crate) fn remember_updated_event( &mut self, folder_id: u64, event_id: u64, event: AccessibleEvent, version: MapiEventVersion, attachments: Vec<CalendarEventAttachment>, )`

# Calls

- [calendar_mapi_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments.md)

# Called by

- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)