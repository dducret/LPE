---
type: Rust Function
title: folder_entry_id_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L963-L971
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_folder_entry_id_from_object_id
---

# Signature

`pub(crate) fn folder_entry_id_from_object_id( mailbox_guid: Uuid, object_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_folder_entry_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_folder_entry_id_from_object_id.md)