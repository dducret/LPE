---
type: Rust Function
title: imported_event_content_properties
resource: crates/lpe-exchange/src/mapi/dispatch/event_save.rs#L227-L243
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
---

# Signature

`fn imported_event_content_properties( properties: &HashMap<u32, MapiValue>, ) -> HashMap<u32, MapiValue>`

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)