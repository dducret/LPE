---
type: Rust Function
title: imported_event_global_object_id
resource: crates/lpe-exchange/src/mapi/dispatch/event_save.rs#L214-L225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
---

# Signature

`fn imported_event_global_object_id(properties: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)