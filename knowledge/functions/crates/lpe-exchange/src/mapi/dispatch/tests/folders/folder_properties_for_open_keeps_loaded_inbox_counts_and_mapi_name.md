---
type: Rust Function
title: folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name
resource: crates/lpe-exchange/src/mapi/dispatch/tests/folders.rs#L227-L319
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
---

# Signature

`fn folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name()`

# Calls

- [remember_mapi_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [folder_properties_for_open_from_mailboxes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [empty](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)