---
type: Rust Function
title: insert_header
resource: crates/lpe-exchange/src/mapi/transport.rs#L1168-L1172
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
---

# Signature

`pub(in crate::mapi) fn insert_header(response: &mut Response, name: &'static str, value: &str)`

# Calls

- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [finalize_mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response.md)
- [decorate_notification_wait_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response.md)
- [notification_wait_empty_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)