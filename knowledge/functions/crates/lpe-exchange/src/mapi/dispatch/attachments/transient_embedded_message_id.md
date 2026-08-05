---
type: Rust Function
title: transient_embedded_message_id
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1236-L1252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
---

# Signature

`pub(super) fn transient_embedded_message_id( folder_id: u64, message_id: u64, attach_num: u32, ) -> u64`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)