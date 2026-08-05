---
type: Rust Function
title: standard_property_row_bytes
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L33-L37
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/query_rows_property_row_bytes
---

# Signature

`pub(in crate::mapi) fn standard_property_row_bytes(values: &[u8]) -> Vec<u8>`

# Calls

- [write_standard_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_standard_property_row.md)

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_contact_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [query_rows_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/query_rows_property_row_bytes.md)