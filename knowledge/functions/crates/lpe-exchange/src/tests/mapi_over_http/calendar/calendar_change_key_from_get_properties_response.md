---
type: Rust Function
title: calendar_change_key_from_get_properties_response
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L2591-L2608
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset
  - functions/crates/lpe-exchange/src/tests/read_rop_binary_u16
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save
---

# Signature

`fn calendar_change_key_from_get_properties_response( response_rops: &[u8], response_handle_index: u8, context: &str, ) -> Vec<u8>`

# Calls

- [mapi_get_properties_specific_standard_row_offset](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_get_properties_specific_standard_row_offset.md)
- [read_rop_binary_u16](../../../../../../../functions/crates/lpe-exchange/src/tests/read_rop_binary_u16.md)

# Called by

- [mapi_over_http_calendar_concurrent_rw_handles_require_force_save](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save.md)