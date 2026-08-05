---
type: Rust Function
title: raw_message_entry_id_from_object_ids
resource: crates/lpe-exchange/src/mapi/identity.rs#L875-L893
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_from_object_ids
---

# Signature

`fn raw_message_entry_id_from_object_ids( mailbox_guid: Uuid, folder_id: u64, message_id: u64, ) -> Option<Vec<u8>>`

# Calls

- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [message_entry_id_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/message_entry_id_from_object_ids.md)