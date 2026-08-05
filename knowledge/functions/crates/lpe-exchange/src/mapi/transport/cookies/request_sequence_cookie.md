---
type: Rust Function
title: request_sequence_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L25-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches
---

# Signature

`pub(in crate::mapi) fn request_sequence_cookie( endpoint: MapiEndpoint, headers: &HeaderMap, ) -> Option<String>`

# Calls

- [request_named_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie.md)
- [sequence_cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name.md)

# Called by

- [request_sequence_cookie_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches.md)