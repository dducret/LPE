---
type: Rust Function
title: mapi_message_folder_id
resource: crates/lpe-exchange/src/mapi_store.rs#L916-L922
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`fn mapi_message_folder_id(email: &JmapEmail, folders: &[MapiFolder]) -> u64`

# Calls

- [mapi_folder_id_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id_for_role.md)

# Called by

- [build](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)