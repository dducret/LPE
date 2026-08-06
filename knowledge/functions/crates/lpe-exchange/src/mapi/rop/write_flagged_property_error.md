---
type: Rust Function
title: write_flagged_property_error
resource: crates/lpe-exchange/src/mapi/rop.rs#L841-L844
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
---

# Signature

`fn write_flagged_property_error(response: &mut Vec<u8>, error_code: u32)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [write_flagged_property_row](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)