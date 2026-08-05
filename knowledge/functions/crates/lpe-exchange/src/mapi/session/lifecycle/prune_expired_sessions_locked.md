---
type: Rust Function
title: prune_expired_sessions_locked
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L332-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_is_expired
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
---

# Signature

`pub(in crate::mapi) fn prune_expired_sessions_locked( sessions: &mut HashMap<String, MapiSession>, now: SystemTime, )`

# Calls

- [session_is_expired](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_is_expired.md)

# Called by

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [rotate_session_request_sequence_token](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/rotate_session_request_sequence_token.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)