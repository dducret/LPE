---
type: Rust Function
title: remember_saved_event_handle
resource: crates/lpe-exchange/src/mapi/dispatch/event_save.rs#L334-L355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_open_mode_after_save
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
---

# Signature

`fn remember_saved_event_handle( session: &mut MapiSession, handle: u32, folder_id: u64, event_id: u64, disposition: SaveDisposition, canonical_modseq: i64, )`

# Calls

- [event_open_mode_after_save](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_open_mode_after_save.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)