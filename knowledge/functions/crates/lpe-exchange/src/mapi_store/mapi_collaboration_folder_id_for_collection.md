---
type: Rust Function
title: mapi_collaboration_folder_id_for_collection
resource: crates/lpe-exchange/src/mapi_store.rs#L981-L1011
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id
  - functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id
---

# Signature

`pub(crate) fn mapi_collaboration_folder_id_for_collection( kind: MapiCollaborationFolderKind, collection_id: &str, ) -> Option<u64>`

# Calls

- [collaboration_folder_identity_canonical_id_for_collection](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id_for_collection.md)
- [mapped_mapi_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [legacy_migration_object_id](../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)

# Called by

- [mapi_collaboration_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id.md)
- [mapi_calendar_notification_folder_id](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/address_helpers/mapi_calendar_notification_folder_id.md)