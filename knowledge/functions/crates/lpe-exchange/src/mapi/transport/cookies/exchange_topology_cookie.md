---
type: Rust Function
title: exchange_topology_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L283-L292
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/routing_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/backend_cookie
---

# Signature

`fn exchange_topology_cookie(name: &str, path: &str, session_id: &str, expired: bool) -> String`

# Called by

- [routing_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/routing_cookie.md)
- [backend_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/backend_cookie.md)