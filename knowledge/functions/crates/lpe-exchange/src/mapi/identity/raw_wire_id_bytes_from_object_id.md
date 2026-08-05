---
type: Rust Function
title: raw_wire_id_bytes_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L644-L650
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/wire_id_bytes_from_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_accepts_legacy_logical_special_folder_wire_ids
---

# Signature

`fn raw_wire_id_bytes_from_object_id(object_id: u64) -> Option<[u8; 8]>`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [wire_id_bytes_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/wire_id_bytes_from_object_id.md)
- [wire_id_bytes_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/wire_id_bytes_from_object_id.md)
- [scoped_codec_accepts_legacy_logical_special_folder_wire_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/scoped_codec_accepts_legacy_logical_special_folder_wire_ids.md)