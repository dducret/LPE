---
type: Rust Function
title: finalize_mapi_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L800-L820
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/insert_header
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait
  - functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_responses_advertise_the_default_pending_period
---

# Signature

`pub(in crate::mapi) fn finalize_mapi_response( mut response: Response, request_headers: &HeaderMap, ) -> Response`

# Calls

- [insert_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/insert_header.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [complete_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)
- [mapi_responses_advertise_the_default_pending_period](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/mapi_responses_advertise_the_default_pending_period.md)