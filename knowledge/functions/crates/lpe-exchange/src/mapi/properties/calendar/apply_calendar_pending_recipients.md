---
type: Rust Function
title: apply_calendar_pending_recipients
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L797-L829
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
---

# Signature

`pub(in crate::mapi) fn apply_calendar_pending_recipients( input: &mut UpsertClientEventInput, existing: &AccessibleEvent, recipients: &[PendingRecipient], )`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [organizer_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)