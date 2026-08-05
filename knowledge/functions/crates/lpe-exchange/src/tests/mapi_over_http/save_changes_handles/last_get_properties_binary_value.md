---
type: Rust Function
title: last_get_properties_binary_value
resource: crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles.rs#L3-L25
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
---

# Signature

`fn last_get_properties_binary_value(response_rops: &[u8]) -> Vec<u8>`

# Calls

- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)