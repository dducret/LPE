---
type: Rust Function
title: imported_hierarchy_version
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy.rs#L376-L414
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`fn imported_hierarchy_version( hierarchy_values: &[(u32, MapiValue)], ) -> Option<ImportedHierarchyVersion<'_>>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)