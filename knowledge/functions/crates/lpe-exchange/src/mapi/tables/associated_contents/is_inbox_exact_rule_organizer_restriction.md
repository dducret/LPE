---
type: Rust Function
title: is_inbox_exact_rule_organizer_restriction
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L244-L260
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table
---

# Signature

`fn is_inbox_exact_rule_organizer_restriction( folder_id: u64, restriction: Option<&MapiRestriction>, ) -> bool`

# Called by

- [should_use_associated_config_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table.md)
- [associated_table_rows_with_lookup_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)
- [associated_config_visible_in_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table.md)