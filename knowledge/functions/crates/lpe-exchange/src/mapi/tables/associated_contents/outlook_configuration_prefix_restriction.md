---
type: Rust Function
title: outlook_configuration_prefix_restriction
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L303-L310
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc
---

# Signature

`pub(super) fn outlook_configuration_prefix_restriction() -> MapiRestriction`

# Called by

- [inbox_associated_exact_named_view_find_row_respects_existing_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction.md)
- [calendar_associated_query_rows_prefix_configuration_returns_calendar_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config.md)
- [inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings.md)
- [inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc.md)