---
type: Rust Function
title: sequence_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L210-L226
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/context_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name
---

# Signature

`pub(in crate::mapi) fn sequence_cookie( endpoint: MapiEndpoint, session_id: &str, expired: bool, ) -> String`

# Calls

- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [context_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/context_cookie.md)
- [sequence_cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name.md)