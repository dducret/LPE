---
type: Rust Function
title: log_special_sync_objects
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L475-L563
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_special_sync_objects( principal: &AccountPrincipal, folder_id: u64, sync_type: u8, objects: &[mapi_mailstore::SpecialMessageSyncFact], )`

# Calls

- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)