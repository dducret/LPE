---
type: Rust Function
title: log_calendar_view_contract_fingerprint
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L160-L192
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_calendar_view_contract_fingerprint( principal: &AccountPrincipal, session: &MapiSession, request_id: &str, request_rop_id: &str, stage: &str, object: Option<&MapiObject>, query_position_response: Option<(u32, u32)>, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)