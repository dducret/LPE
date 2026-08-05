---
type: Rust Function
title: mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions
resource: crates/lpe-exchange/src/tests/mapi_over_http/nspi.rs#L1308-L1350
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/tests/nspi_bound_headers
  - functions/crates/lpe-exchange/src/tests/response_bytes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request
---

# Signature

`async fn mapi_over_http_nspi_ids_ignore_generic_mapi_identity_cache_collisions()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [nspi_bound_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/nspi_bound_headers.md)
- [response_bytes](../../../../../../../functions/crates/lpe-exchange/src/tests/response_bytes.md)
- [nspi_dn_to_mid_request](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/nspi/nspi_dn_to_mid_request.md)