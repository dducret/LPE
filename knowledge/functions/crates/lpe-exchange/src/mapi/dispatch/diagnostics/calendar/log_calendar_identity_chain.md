---
type: Rust Function
title: log_calendar_identity_chain
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar.rs#L165-L247
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_calendar_identity_chain( principal: &AccountPrincipal, stage: &str, observed_folder_id: u64, checkpoint_mailbox_id: Option<Uuid>, sync_type: Option<u8>, snapshot: Option<&MapiMailStoreSnapshot>, )`

# Calls

- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [virtual_special_mailbox](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [events_for_folder](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)

# Called by

- [log_calendar_folder_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract.md)
- [append_synchronization_configure_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)