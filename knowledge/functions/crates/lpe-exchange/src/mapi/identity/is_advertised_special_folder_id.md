---
type: Rust Function
title: is_advertised_special_folder_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L827-L873
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_trailing_replid_wire_id
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/stale_special_folder_object_id_from_long_term_id
---

# Signature

`fn is_advertised_special_folder_id(object_id: u64) -> bool`

# Called by

- [object_id_from_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_wire_id.md)
- [object_id_from_trailing_replid_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/object_id_from_trailing_replid_wire_id.md)
- [raw_object_id_from_folder_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_folder_entry_id.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/stale_special_folder_object_id_from_long_term_id.md)