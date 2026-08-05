---
type: Rust Function
title: raw_object_ids_from_message_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L895-L914
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/object_ids_from_message_entry_id
---

# Signature

`fn raw_object_ids_from_message_entry_id(mailbox_guid: Uuid, entry_id: &[u8]) -> Option<(u64, u64)>`

# Calls

- [global_counter_from_globcnt](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_globcnt.md)

# Called by

- [object_ids_from_message_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_ids_from_message_entry_id.md)