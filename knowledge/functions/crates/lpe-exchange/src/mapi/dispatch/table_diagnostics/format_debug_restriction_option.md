---
type: Rust Function
title: format_debug_restriction_option
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L322-L326
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
---

# Signature

`pub(super) fn format_debug_restriction_option(restriction: Option<&MapiRestriction>) -> String`

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)