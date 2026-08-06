---
type: Rust Function
title: collaboration_folder_identity_canonical_id
resource: crates/lpe-exchange/src/mapi_store.rs#L1020-L1025
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
  - functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants
  - functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests
  - functions/crates/lpe-exchange/src/mapi_store/tests/dynamic_contact_folder_exposes_only_persisted_associated_config
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded
---

# Signature

`pub(crate) fn collaboration_folder_identity_canonical_id( kind: MapiCollaborationFolderKind, collection: &CollaborationCollection, ) -> Option<Uuid>`

# Calls

- [collaboration_folder_identity_canonical_id_for_collection](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)

# Called by

- [sync_mailboxes_with_collaboration_counts](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants](../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/custom_collaboration_folders_are_ipm_subtree_children_and_root_depth_descendants.md)
- [dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config](../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/dynamic_contacts_associated_find_row_does_not_invent_osc_contact_sync_config.md)
- [collaboration_folder_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests.md)
- [dynamic_contact_folder_exposes_only_persisted_associated_config](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/dynamic_contact_folder_exposes_only_persisted_associated_config.md)
- [snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders.md)
- [snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_falls_back_when_custom_collaboration_identity_is_not_loaded.md)