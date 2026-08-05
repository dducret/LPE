---
type: Rust Method
title: advertised_special_folder_was_deleted
resource: crates/lpe-exchange/src/mapi/session.rs#L1022-L1024
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request
---

# Signature

`pub(in crate::mapi) fn advertised_special_folder_was_deleted(&self, folder_id: u64) -> bool`

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [create_folder_existing_mailbox_satisfies_deleted_advertised_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request.md)