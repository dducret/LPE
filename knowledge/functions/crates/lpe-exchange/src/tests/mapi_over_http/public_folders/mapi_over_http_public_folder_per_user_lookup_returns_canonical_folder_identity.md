---
type: Rust Function
title: mapi_over_http_public_folder_per_user_lookup_returns_canonical_folder_identity
resource: crates/lpe-exchange/src/tests/mapi_over_http/public_folders.rs#L3937-L4024
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
---

# Signature

`async fn mapi_over_http_public_folder_per_user_lookup_returns_canonical_folder_identity()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)