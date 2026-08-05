---
type: Rust Function
title: active_session_ping_failure_returns_current_session_cookies
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L520-L558
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
---

# Signature

`fn active_session_ping_failure_returns_current_session_cookies()`

# Calls

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)