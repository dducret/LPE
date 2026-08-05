---
type: Rust Function
title: accepted_response_rotates_the_mapi_sequence_cookie
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L407-L475
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
---

# Signature

`fn accepted_response_rotates_the_mapi_sequence_cookie()`

# Calls

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_response_with_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [session_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [refresh_accepted_session_response_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)