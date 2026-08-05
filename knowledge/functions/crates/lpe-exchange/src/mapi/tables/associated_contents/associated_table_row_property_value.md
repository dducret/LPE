---
type: Rust Function
title: associated_table_row_property_value
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L345-L358
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_table_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
---

# Signature

`pub(super) fn associated_table_row_property_value( message: &AssociatedTableRow, mailbox_guid: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)

# Called by

- [serialize_associated_table_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_table_property_row.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [sort_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_associated_table_rows.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)