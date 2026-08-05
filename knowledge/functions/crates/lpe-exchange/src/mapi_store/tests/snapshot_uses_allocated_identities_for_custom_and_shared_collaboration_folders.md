---
type: Rust Function
title: snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L2444-L2533
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
---

# Signature

`fn snapshot_uses_allocated_identities_for_custom_and_shared_collaboration_folders()`

# Calls

- [collaboration_folder_identity_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [collaboration_folders](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)