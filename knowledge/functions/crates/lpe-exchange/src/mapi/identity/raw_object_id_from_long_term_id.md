---
type: Rust Function
title: raw_object_id_from_long_term_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L742-L744
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id
---

# Signature

`fn raw_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64>`

# Calls

- [object_id_from_long_term_id_with_replica_guids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id_with_replica_guids.md)

# Called by

- [raw_object_id_from_folder_identifier_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_identifier_bytes.md)
- [object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_from_long_term_id.md)