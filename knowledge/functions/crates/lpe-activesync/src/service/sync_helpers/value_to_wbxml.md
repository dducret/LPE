---
type: Rust Function
title: value_to_wbxml
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L95-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/value_to_node
  called_by:
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
---

# Signature

`pub(super) fn value_to_wbxml(value: Value) -> WbxmlNode`

# Calls

- [value_to_node](../../../../../../functions/crates/lpe-activesync/src/snapshot/value_to_node.md)

# Called by

- [handle_item_operations_fetch](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [handle_search](../../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)