---
type: Rust Function
title: request_named_cookie_candidates
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L50-L65
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug
---

# Signature

`fn request_named_cookie_candidates(name: &str, headers: &HeaderMap) -> Vec<String>`

# Called by

- [request_named_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie.md)
- [request_cookie_transport_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [session_cookie_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)