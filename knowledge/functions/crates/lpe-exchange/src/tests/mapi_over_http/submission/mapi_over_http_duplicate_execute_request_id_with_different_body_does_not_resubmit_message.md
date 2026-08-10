---
type: Rust Function
title: mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message
resource: crates/lpe-exchange/src/tests/mapi_over_http/submission.rs#L1862-L1928
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
---

# Signature

`async fn mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_submit_execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)