---
type: Rust Method
title: with_opaque
resource: crates/lpe-activesync/src/wbxml.rs#L35-L43
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
---

# Signature

`pub(crate) fn with_opaque(page: u8, name: impl Into<String>, data: Vec<u8>) -> Self`

# Called by

- [handle_item_operations_fetch](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)