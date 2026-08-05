---
type: Rust Function
title: connect_rejects_an_expired_session_context
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L77-L108
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
---

# Signature

`fn connect_rejects_an_expired_session_context()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [sessions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)