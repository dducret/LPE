---
type: Rust Method
title: remember_event_reminder_state
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L501-L527
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`pub(crate) fn remember_event_reminder_state( &mut self, event_id: Uuid, state: MapiEventReminderState, )`

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)