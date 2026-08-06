---
type: Rust Function
title: mapi_collaboration_folder_id
resource: crates/lpe-exchange/src/mapi_store.rs#L980-L986
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`fn mapi_collaboration_folder_id( kind: MapiCollaborationFolderKind, collection: &CollaborationCollection, ) -> u64`

# Calls

- [mapi_collaboration_folder_id_for_collection](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id_for_collection.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [build](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)