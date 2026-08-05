---
type: Rust Function
title: assert_nspi_dn_to_mid_request_rejected
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L163-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/nspi_bound_headers
  - functions/crates/lpe-exchange/src/tests/response_bytes
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_missing_auxiliary_size_without_names
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer
---

# Signature

`async fn assert_nspi_dn_to_mid_request_rejected(request: &[u8])`

# Calls

- [nspi_bound_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/nspi_bound_headers.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)

# Called by

- [mapi_over_http_nspi_dn_to_mid_rejects_missing_auxiliary_size_without_names](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_missing_auxiliary_size_without_names.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_truncated_auxiliary_buffer.md)
- [mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/mapi_over_http_nspi_dn_to_mid_rejects_trailing_bytes_after_auxiliary_buffer.md)