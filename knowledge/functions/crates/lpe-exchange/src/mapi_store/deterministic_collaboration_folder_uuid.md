---
type: Rust Function
title: deterministic_collaboration_folder_uuid
resource: crates/lpe-exchange/src/mapi_store.rs#L1035-L1055
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
---

# Signature

`fn deterministic_collaboration_folder_uuid( kind: MapiCollaborationFolderKind, collection_id: &str, ) -> Uuid`

# Called by

- [collaboration_folder_identity_canonical_id_for_collection](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)