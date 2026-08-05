---
type: Rust Function
title: context_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L342-L356
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_path
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie
---

# Signature

`pub(in crate::mapi) fn context_cookie( endpoint: MapiEndpoint, name: &str, session_id: &str, expired: bool, ) -> String`

# Calls

- [cookie_path](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_path.md)

# Called by

- [session_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie.md)
- [sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie.md)