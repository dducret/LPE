---
type: Rust Method
title: logical_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L279-L284
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_ids_from_message_entry_id
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/from_identity_records
---

# Signature

`pub(crate) fn logical_object_id(&self, object_id: u64) -> Option<u64>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [is_logical_special_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/is_logical_special_folder_id.md)

# Called by

- [folder_version_for_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot.md)
- [seed_from_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [object_id_from_wire_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id.md)
- [object_id_from_trailing_replid_wire_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_trailing_replid_wire_id.md)
- [object_id_from_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_source_key.md)
- [object_id_from_long_term_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_long_term_id.md)
- [object_id_from_folder_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_folder_entry_id.md)
- [object_ids_from_message_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_ids_from_message_entry_id.md)
- [from_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/MapiFolderVersions/from_identity_records.md)