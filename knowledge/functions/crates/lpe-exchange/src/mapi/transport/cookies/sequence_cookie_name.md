---
type: Rust Function
title: sequence_cookie_name
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L365-L370
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie
---

# Signature

`pub(in crate::mapi) fn sequence_cookie_name(endpoint: MapiEndpoint) -> &'static str`

# Called by

- [request_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie.md)
- [request_cookie_transport_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [session_cookie_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)
- [sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie.md)