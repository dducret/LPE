---
type: Rust Function
title: special_binary_property_len
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L494-L505
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
---

# Signature

`fn special_binary_property_len( object: &mapi_mailstore::SpecialMessageSyncFact, property_tag: u32, ) -> Option<usize>`

# Called by

- [log_calendar_special_sync_objects](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)