---
type: Rust Function
title: serialize_pending_task_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L579-L610
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_task_row( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [task_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi.md)
- [default_task_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/default_task_for_mapping.md)
- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)
- [serialize_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)