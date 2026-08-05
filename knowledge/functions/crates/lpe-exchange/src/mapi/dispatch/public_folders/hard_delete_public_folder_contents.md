---
type: Rust Function
title: hard_delete_public_folder_contents
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L6-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder
  - functions/crates/lpe-exchange/src/mapi/record_mapi_folder_purge_metrics
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
---

# Signature

`pub(super) async fn hard_delete_public_folder_contents<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Result<(Vec<u64>, bool), u32>`

# Calls

- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [public_folder_items_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_items_for_folder.md)
- [record_mapi_folder_purge_metrics](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_folder_purge_metrics.md)

# Called by

- [append_empty_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)