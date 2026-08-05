---
type: Rust Function
title: associated_config_visible_in_table
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L234-L242
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
---

# Signature

`pub(in crate::mapi) fn associated_config_visible_in_table( folder_id: u64, restriction: Option<&MapiRestriction>, message: &MapiAssociatedConfigMessage, ) -> bool`

# Calls

- [is_outlook_inbox_virtual_only_associated_config_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_inbox_virtual_only_associated_config_id.md)
- [is_inbox_exact_rule_organizer_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction.md)

# Called by

- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [associated_table_rows_with_lookup_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)