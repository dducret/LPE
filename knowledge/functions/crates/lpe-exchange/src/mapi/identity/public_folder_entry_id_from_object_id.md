---
type: Rust Function
title: public_folder_entry_id_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L983-L986
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_public_folder_entry_id_from_object_id
---

# Signature

`pub(crate) fn public_folder_entry_id_from_object_id(object_id: u64) -> Option<Vec<u8>>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_public_folder_entry_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_public_folder_entry_id_from_object_id.md)