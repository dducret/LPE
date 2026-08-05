---
type: Rust Function
title: object_ids_from_message_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L1009-L1017
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_object_ids_from_message_entry_id
---

# Signature

`pub(crate) fn object_ids_from_message_entry_id( mailbox_guid: Uuid, entry_id: &[u8], ) -> Option<(u64, u64)>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_object_ids_from_message_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_object_ids_from_message_entry_id.md)