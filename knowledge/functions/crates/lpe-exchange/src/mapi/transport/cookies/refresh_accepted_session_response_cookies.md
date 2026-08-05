---
type: Rust Function
title: refresh_accepted_session_response_cookies
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L296-L340
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie
---

# Signature

`pub(in crate::mapi) fn refresh_accepted_session_response_cookies( response: &mut Response, endpoint: MapiEndpoint, request_type: &str, request_headers: &HeaderMap, )`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [rotate_session_request_sequence_token](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [session_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)