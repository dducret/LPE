---
type: Rust Method
title: folder_versions
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L739-L741
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
---

# Signature

`pub(crate) fn folder_versions(&self) -> Vec<MapiFolderVersion>`

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)