---
type: Rust Method
title: event_for_uid
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1032-L1036
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
---

# Signature

`pub(crate) fn event_for_uid(&self, folder_id: u64, uid: &str) -> Option<&MapiEvent>`

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)