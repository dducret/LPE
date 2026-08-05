---
type: Rust Function
title: session_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L202-L208
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/context_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name
---

# Signature

`pub(in crate::mapi) fn session_cookie( endpoint: MapiEndpoint, session_id: &str, expired: bool, ) -> String`

# Calls

- [context_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/context_cookie.md)
- [cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name.md)