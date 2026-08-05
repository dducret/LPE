---
type: Rust Function
title: hard_delete_mailbox_tree_contents
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L830-L943
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
  - functions/crates/lpe-exchange/src/mapi/record_mapi_folder_purge_metrics
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
---

# Signature

`pub(super) async fn hard_delete_mailbox_tree_contents<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Result<(Vec<u64>, bool), u32>`

# Calls

- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [email_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)
- [record_mapi_folder_purge_metrics](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_folder_purge_metrics.md)

# Called by

- [append_empty_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)