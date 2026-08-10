---
type: Rust Function
title: write_fast_transfer_visible_recipients
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1097-L1120
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children
---

# Signature

`fn write_fast_transfer_visible_recipients(buffer: &mut Vec<u8>, email: &JmapEmail)`

# Calls

- [write_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_utf16_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)

# Called by

- [write_fast_transfer_message_children](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children.md)