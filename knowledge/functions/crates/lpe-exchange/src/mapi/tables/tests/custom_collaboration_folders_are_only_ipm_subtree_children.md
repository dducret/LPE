---
type: Rust Function
title: custom_collaboration_folders_are_only_ipm_subtree_children
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L3416-L3478
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows
---

# Signature

`fn custom_collaboration_folders_are_only_ipm_subtree_children()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [collaboration_folder_identity_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes.md)
- [hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows.md)