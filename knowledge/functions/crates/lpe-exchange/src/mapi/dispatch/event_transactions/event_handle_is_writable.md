---
type: Rust Function
title: event_handle_is_writable
resource: crates/lpe-exchange/src/mapi/dispatch/event_transactions.rs#L512-L514
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
---

# Signature

`pub(super) fn event_handle_is_writable(open_mode_flags: u8, may_write: bool) -> bool`

# Called by

- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [save_existing_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_save/save_existing_contact.md)
- [stage_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values.md)
- [stage_contact_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_deletions.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)