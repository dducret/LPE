---
type: Rust Module
title: object_ids
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L1-L126
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-rop-error-response-write-object-id-write-u32-roprequest
  - external/crate-mapi-identity-self-long-term-id-from-object-id-object-id-from-long-term-id-object-id-from-long-term-id-with-replica-guids
  - external/crate-mapi-tables-is-advertised-special-folder
  - external/crate-mapi-wire-ropid
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rop_long_term_id_from_id_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_long_term_id_from_id_response.md)
- [rop_id_from_long_term_id_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response.md)
- [long_term_source_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id.md)
- [long_term_source_id_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_id_bytes.md)
- [stale_special_folder_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_long_term_id.md)
- [stale_special_folder_object_id_from_short_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)
- [advertised_virtual_object_id_from_bare_little_endian_short_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/advertised_virtual_object_id_from_bare_little_endian_short_id.md)
- [dynamic_object_id_from_bare_little_endian_short_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/dynamic_object_id_from_bare_little_endian_short_id.md)
- [global_counter_from_little_endian_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/global_counter_from_little_endian_globcnt.md)

# Imports

- `super::{rop_error_response, write_object_id, write_u32, RopRequest}`
- `crate::mapi::identity::{
    self, long_term_id_from_object_id, object_id_from_long_term_id,
    object_id_from_long_term_id_with_replica_guids,
}`
- `crate::mapi::tables::is_advertised_special_folder`
- `crate::mapi::wire::RopId`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)