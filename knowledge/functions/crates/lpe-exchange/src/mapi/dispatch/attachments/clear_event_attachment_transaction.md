---
type: Rust Function
title: clear_event_attachment_transaction
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1152-L1159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/abandon_event_attachment_transaction
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`pub(super) fn clear_event_attachment_transaction(session: &mut MapiSession, parent_handle: u32)`

# Calls

- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [abandon_event_attachment_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/abandon_event_attachment_transaction.md)
- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)