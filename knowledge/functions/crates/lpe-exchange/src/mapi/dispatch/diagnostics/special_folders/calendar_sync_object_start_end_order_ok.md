---
type: Rust Function
title: calendar_sync_object_start_end_order_ok
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L611-L624
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/special_i64_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
---

# Signature

`fn calendar_sync_object_start_end_order_ok( object: &mapi_mailstore::SpecialMessageSyncFact, ) -> bool`

# Calls

- [special_i64_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/special_i64_property.md)

# Called by

- [log_calendar_special_sync_objects](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)