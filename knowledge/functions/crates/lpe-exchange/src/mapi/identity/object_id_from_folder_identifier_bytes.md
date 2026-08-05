---
type: Rust Function
title: object_id_from_folder_identifier_bytes
resource: crates/lpe-exchange/src/mapi/identity.rs#L993-L996
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes
---

# Signature

`pub(crate) fn object_id_from_folder_identifier_bytes(bytes: &[u8]) -> Option<u64>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_object_id_from_folder_identifier_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes.md)