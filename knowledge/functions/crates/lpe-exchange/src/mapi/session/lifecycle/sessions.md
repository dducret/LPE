---
type: Rust Function
title: sessions
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L8-L10
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_an_expired_session_context
---

# Signature

`pub(in crate::mapi) fn sessions() -> &'static Mutex<HashMap<String, MapiSession>>`

# Called by

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [rotate_session_request_sequence_token](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [connect_rejects_an_expired_session_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/connect_rejects_an_expired_session_context.md)