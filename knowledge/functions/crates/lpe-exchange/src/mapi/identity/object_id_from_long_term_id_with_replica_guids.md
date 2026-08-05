---
type: Rust Function
title: object_id_from_long_term_id_with_replica_guids
resource: crates/lpe-exchange/src/mapi/identity.rs#L746-L758
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response
---

# Signature

`pub(crate) fn object_id_from_long_term_id_with_replica_guids( long_term_id: &[u8], replica_guid_aliases: &[[u8; 16]], ) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [raw_object_id_from_long_term_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_id_from_long_term_id.md)
- [rop_id_from_long_term_id_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response.md)