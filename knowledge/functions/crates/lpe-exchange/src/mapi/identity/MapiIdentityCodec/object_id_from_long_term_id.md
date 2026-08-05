---
type: Rust Method
title: object_id_from_long_term_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L329-L339
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
---

# Signature

`pub(crate) fn object_id_from_long_term_id(&self, long_term_id: &[u8]) -> Option<u64>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)