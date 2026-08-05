---
type: Rust Function
title: advertised_special_folder_id_for_create
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L193-L248
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder
---

# Signature

`pub(in crate::mapi) fn advertised_special_folder_id_for_create( parent_folder_id: u64, display_name: &str, ) -> Option<u64>`

# Calls

- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [create_folder_existing_mailbox_satisfies_deleted_advertised_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/create_folder_existing_mailbox_satisfies_deleted_advertised_request.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [mailbox_advertised_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_advertised_special_folder_id.md)
- [mailbox_shadowed_by_active_outlook_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/mailbox_shadowed_by_active_outlook_special_folder.md)