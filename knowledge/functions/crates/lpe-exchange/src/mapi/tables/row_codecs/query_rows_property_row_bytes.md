---
type: Rust Function
title: query_rows_property_row_bytes
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L29-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
---

# Signature

`pub(in crate::mapi) fn query_rows_property_row_bytes(_columns: &[u32], values: &[u8]) -> Vec<u8>`

# Calls

- [standard_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)