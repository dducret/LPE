---
type: Rust Function
title: cookie_name
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L358-L363
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie
---

# Signature

`pub(in crate::mapi) fn cookie_name(endpoint: MapiEndpoint) -> &'static str`

# Called by

- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [request_cookie_transport_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [session_cookie_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)
- [session_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie.md)