---
type: Rust Function
title: create_folder_existing_mailbox_satisfies_deleted_advertised_request
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L990-L998
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_special_folder_was_deleted
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
---

# Signature

`pub(super) fn create_folder_existing_mailbox_satisfies_deleted_advertised_request( session: &MapiSession, parent_folder_id: u64, display_name: &str, ) -> bool`

# Calls

- [advertised_special_folder_id_for_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create.md)
- [advertised_special_folder_was_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_special_folder_was_deleted.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)