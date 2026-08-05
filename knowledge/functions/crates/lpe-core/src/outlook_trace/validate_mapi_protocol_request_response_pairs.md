---
type: Rust Function
title: validate_mapi_protocol_request_response_pairs
resource: crates/lpe-core/src/outlook_trace.rs#L773-L811
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/outlook_trace/json_string_value
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing
---

# Signature

`fn validate_mapi_protocol_request_response_pairs(lines: &[&str])`

# Calls

- [json_string_value](../../../../../functions/crates/lpe-core/src/outlook_trace/json_string_value.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing](../../../../../functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing.md)