---
type: Rust Function
title: persisted_default_folder_view_entry_id
resource: crates/lpe-exchange/src/mapi/properties/views.rs#L97-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_config
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
---

# Signature

`pub(in crate::mapi) fn persisted_default_folder_view_entry_id( snapshot: &crate::mapi_store::MapiMailStoreSnapshot, mailbox_guid: Uuid, folder_id: u64, ) -> Option<MapiValue>`

# Calls

- [default_folder_named_view_config](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_config.md)

# Called by

- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)