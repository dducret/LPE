---
type: Rust Function
title: request_named_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L44-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie
---

# Signature

`pub(in crate::mapi) fn request_named_cookie(name: &str, headers: &HeaderMap) -> Option<String>`

# Calls

- [request_named_cookie_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates.md)

# Called by

- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [request_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie.md)