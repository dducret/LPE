---
type: Rust Function
title: ping_accepts_missing_or_prior_mapi_sequence_cookie
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L478-L517
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
---

# Signature

`fn ping_accepts_missing_or_prior_mapi_sequence_cookie()`

# Calls

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)