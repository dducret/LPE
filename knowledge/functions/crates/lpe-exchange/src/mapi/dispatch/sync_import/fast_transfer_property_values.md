---
type: Rust Function
title: fast_transfer_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L622-L636
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(super) fn fast_transfer_property_values(bytes: &[u8]) -> Result<Vec<(u32, MapiValue)>>`

# Calls

- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_fast_transfer_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)