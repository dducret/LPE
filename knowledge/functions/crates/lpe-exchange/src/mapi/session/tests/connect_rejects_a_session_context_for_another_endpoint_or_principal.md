---
type: Rust Function
title: connect_rejects_a_session_context_for_another_endpoint_or_principal
resource: crates/lpe-exchange/src/mapi/session/tests.rs#L111-L160
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/tests/principal
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
---

# Signature

`fn connect_rejects_a_session_context_for_another_endpoint_or_principal()`

# Calls

- [principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/principal.md)
- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)