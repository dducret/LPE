---
type: Rust Function
title: format_debug_restriction
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L248-L256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_parsed_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(super) fn format_debug_restriction(bytes: &[u8]) -> String`

# Calls

- [parse_mapi_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction.md)
- [format_debug_parsed_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_parsed_restriction.md)

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)