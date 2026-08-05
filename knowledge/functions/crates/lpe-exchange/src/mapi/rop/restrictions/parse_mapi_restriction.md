---
type: Rust Function
title: parse_mapi_restriction
resource: crates/lpe-exchange/src/mapi/rop/restrictions.rs#L7-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
---

# Signature

`pub(in crate::mapi) fn parse_mapi_restriction(bytes: &[u8]) -> Result<MapiRestriction>`

# Calls

- [parse_mapi_restriction_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)

# Called by

- [bounded_search_criteria_from_rop](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/bounded_search_criteria_from_rop.md)
- [format_debug_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction.md)
- [microsoft_oxcdata_reminder_restriction_example_parses_and_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches.md)
- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)