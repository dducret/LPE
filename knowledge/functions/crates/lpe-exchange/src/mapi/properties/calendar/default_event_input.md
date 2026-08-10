---
type: Rust Function
title: default_event_input
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L639-L667
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
---

# Signature

`pub(in crate::mapi) fn default_event_input( account_id: Uuid, id: Option<Uuid>, ) -> UpsertClientEventInput`

# Calls

- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)

# Called by

- [serialize_pending_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)