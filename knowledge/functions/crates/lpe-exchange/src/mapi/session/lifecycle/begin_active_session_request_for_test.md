---
type: Rust Function
title: begin_active_session_request_for_test
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L45-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence
---

# Signature

`pub(crate) fn begin_active_session_request_for_test(session_id: &str) -> impl Drop`

# Calls

- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence.md)