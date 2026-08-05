---
type: Rust Function
title: folder_local_commit_time_max_property_value
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L13-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
---

# Signature

`pub(in crate::mapi) fn folder_local_commit_time_max_property_value( snapshot: &crate::mapi_store::MapiMailStoreSnapshot, folder_id: u64, mailboxes: &[JmapMailbox], property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [folder_local_commit_time_max](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)

# Called by

- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)