---
type: Rust Function
title: format_debug_mapi_value
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L3-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_text_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
---

# Signature

`pub(super) fn format_debug_mapi_value(value: &MapiValue) -> String`

# Calls

- [format_debug_text_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_text_value.md)

# Called by

- [format_normal_message_debug_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value.md)
- [format_ipm_configuration_row_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [format_contact_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)