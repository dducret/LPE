---
type: Rust Function
title: calendar_pending_recipients
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L771-L795
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
---

# Signature

`pub(in crate::mapi) fn calendar_pending_recipients( event: &AccessibleEvent, ) -> Vec<PendingRecipient>`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)

# Called by

- [append_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)
- [append_modify_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)