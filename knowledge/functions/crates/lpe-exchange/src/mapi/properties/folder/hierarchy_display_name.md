---
type: Rust Function
title: hierarchy_display_name
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L277-L293
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
---

# Signature

`pub(in crate::mapi) fn hierarchy_display_name( hierarchy_values: &[(u32, MapiValue)], property_values: &[(u32, MapiValue)], ) -> Option<String>`

# Calls

- [as_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_text.md)

# Called by

- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)