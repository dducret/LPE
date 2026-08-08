---
type: Rust Function
title: push_unique_nspi_entry_id
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1362-L1366
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
---

# Signature

`fn push_unique_nspi_entry_id(ids: &mut Vec<u32>, value: u32)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)