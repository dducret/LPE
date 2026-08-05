---
type: Rust Function
title: folder_version_for_snapshot
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy.rs#L359-L368
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`fn folder_version_for_snapshot( snapshot: &MapiMailStoreSnapshot, mut version: MapiFolderVersion, ) -> MapiFolderVersion`

# Calls

- [identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)