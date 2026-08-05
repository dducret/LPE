---
type: Rust Function
title: try_mapi_folder_id
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L65-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id_for_role
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id
---

# Signature

`pub(in crate::mapi) fn try_mapi_folder_id(mailbox: &JmapMailbox) -> Option<u64>`

# Calls

- [try_mapi_folder_id_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id_for_role.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [hidden_configuration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/hidden_configuration_folder_message_class.md)
- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id.md)