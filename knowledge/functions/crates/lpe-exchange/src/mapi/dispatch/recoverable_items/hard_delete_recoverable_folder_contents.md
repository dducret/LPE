---
type: Rust Function
title: hard_delete_recoverable_folder_contents
resource: crates/lpe-exchange/src/mapi/dispatch/recoverable_items.rs#L3-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
---

# Signature

`pub(super) async fn hard_delete_recoverable_folder_contents<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Result<(Vec<u64>, bool), u32>`

# Calls

- [recoverable_items_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/recoverable_items_for_folder.md)
- [recoverable_storage_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_storage_folder.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_empty_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)