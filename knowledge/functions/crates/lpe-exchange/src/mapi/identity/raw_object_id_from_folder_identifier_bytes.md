---
type: Rust Function
title: raw_object_id_from_folder_identifier_bytes
resource: crates/lpe-exchange/src/mapi/identity.rs#L812-L816
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_identifier_bytes
---

# Signature

`fn raw_object_id_from_folder_identifier_bytes(bytes: &[u8]) -> Option<u64>`

# Calls

- [raw_object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id.md)
- [raw_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id.md)

# Called by

- [object_id_from_folder_identifier_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_folder_identifier_bytes.md)