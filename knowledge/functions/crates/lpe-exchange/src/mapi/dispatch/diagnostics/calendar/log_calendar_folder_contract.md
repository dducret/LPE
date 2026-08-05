---
type: Rust Function
title: log_calendar_folder_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar.rs#L3-L84
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_calendar_folder_contract( principal: &AccountPrincipal, folder_id: u64, mailbox_folder_found: bool, collaboration_folder_found: bool, advertised_special_folder: bool, snapshot: &MapiMailStoreSnapshot, mailboxes: &[JmapMailbox], emails: &[JmapEmail], )`

# Calls

- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folders](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [folder_access_for_principal](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [log_calendar_identity_chain](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_identity_chain.md)

# Called by

- [append_open_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)