---
type: Rust Function
title: is_virtual_special_mailbox
resource: crates/lpe-exchange/src/mapi_store.rs#L914-L921
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests
---

# Signature

`pub(crate) fn is_virtual_special_mailbox(mailbox: &JmapMailbox) -> bool`

# Calls

- [reserved_folder_counter_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)
- [virtual_special_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)

# Called by

- [mapi_folder_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests.md)