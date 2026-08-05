---
type: Rust Method
title: message_entry_id_from_object_ids
resource: crates/lpe-exchange/src/mapi/identity.rs#L398-L419
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
---

# Signature

`pub(crate) fn message_entry_id_from_object_ids( &self, mailbox_guid: Uuid, folder_id: u64, message_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [actual_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/actual_object_id.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)