---
type: Rust Function
title: reconnect_session_rejects_active_context
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L18-L48
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
---

# Signature

`fn reconnect_session_rejects_active_context()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)