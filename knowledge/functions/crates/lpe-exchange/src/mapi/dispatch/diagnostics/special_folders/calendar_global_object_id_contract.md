---
type: Rust Function
title: calendar_global_object_id_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L668-L681
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/third_party_global_object_id_contract
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects
---

# Signature

`fn calendar_global_object_id_contract( object: &mapi_mailstore::SpecialMessageSyncFact, property_tag: u32, ) -> (&'static str, bool)`

# Calls

- [third_party_global_object_id_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/third_party_global_object_id_contract.md)

# Called by

- [log_calendar_special_sync_objects](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_calendar_special_sync_objects.md)