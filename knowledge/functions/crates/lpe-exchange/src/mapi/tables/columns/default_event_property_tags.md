---
type: Rust Function
title: default_event_property_tags
resource: crates/lpe-exchange/src/mapi/tables/columns.rs#L325-L360
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
---

# Signature

`pub(in crate::mapi) fn default_event_property_tags() -> Vec<u32>`

# Called by

- [get_properties_specific_candidate_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_specific_candidate_tags.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [rop_get_properties_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response.md)
- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)