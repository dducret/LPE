---
type: Rust Method
title: object_ids_from_message_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L421-L445
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
---

# Signature

`pub(crate) fn object_ids_from_message_entry_id( &self, mailbox_guid: Uuid, entry_id: &[u8], ) -> Option<(u64, u64)>`

# Calls

- [global_counter_from_globcnt](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)