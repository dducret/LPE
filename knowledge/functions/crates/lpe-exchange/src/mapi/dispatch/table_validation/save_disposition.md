---
type: Rust Function
title: save_disposition
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L135-L147
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported
---

# Signature

`pub(in crate::mapi::dispatch) fn save_disposition(request: &RopRequest) -> Option<SaveDisposition>`

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [save_flags_are_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_flags_are_supported.md)