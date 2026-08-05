---
type: Rust Function
title: message_entry_id_from_object_ids
resource: crates/lpe-exchange/src/mapi/identity.rs#L998-L1007
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/raw_message_entry_id_from_object_ids
---

# Signature

`pub(crate) fn message_entry_id_from_object_ids( mailbox_guid: Uuid, folder_id: u64, message_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [current_mapi_identity_codec](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_identity_codec.md)
- [raw_message_entry_id_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/raw_message_entry_id_from_object_ids.md)