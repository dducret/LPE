---
type: Rust Function
title: log_special_folder_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders.rs#L3-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_rca_special_contract_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/expected_special_folder_parent_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_special_folder_contract( principal: &AccountPrincipal, request_id: &str, folder_id: u64, mailbox_folder_found: bool, collaboration_folder_found: bool, advertised_special_folder: bool, snapshot: &MapiMailStoreSnapshot, mailboxes: &[JmapMailbox], emails: &[JmapEmail], )`

# Calls

- [is_rca_special_contract_folder](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/is_rca_special_contract_folder.md)
- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [expected_special_folder_parent_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/expected_special_folder_parent_id.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [folder_access_for_principal](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)