---
type: Rust Function
title: event_open_mode_after_save
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L516-L522
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/remember_saved_contact_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle
---

# Signature

`pub(super) fn event_open_mode_after_save(disposition: SaveDisposition) -> Option<u8>`

# Called by

- [remember_saved_contact_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/remember_saved_contact_handle.md)
- [remember_saved_event_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle.md)