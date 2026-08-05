---
type: Rust Function
title: property_ids_match
resource: crates/lpe-exchange/src/mapi/dispatch/property_tags.rs#L21-L23
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/property_value_by_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_contract_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_is_backed
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_is_backed
---

# Signature

`pub(super) fn property_ids_match(left: u32, right: u32) -> bool`

# Called by

- [property_value_by_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/property_value_by_id.md)
- [set_property_debug_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_names/set_property_debug_name.md)
- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [format_common_views_wlink_contract_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_contract_summary.md)
- [associated_contents_table_column_is_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/associated_contents_table_column_is_backed.md)
- [normal_message_table_column_is_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_table_column_is_backed.md)