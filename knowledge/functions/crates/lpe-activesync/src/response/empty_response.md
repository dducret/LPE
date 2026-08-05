---
type: Rust Function
title: empty_response
resource: crates/lpe-activesync/src/response.rs#L16-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-activesync/src/response/add_common_headers
  called_by:
  - functions/crates/lpe-activesync/src/app/options_response_for_store
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
---

# Signature

`pub(crate) fn empty_response() -> Response`

# Calls

- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [add_common_headers](../../../../../functions/crates/lpe-activesync/src/response/add_common_headers.md)

# Called by

- [options_response_for_store](../../../../../functions/crates/lpe-activesync/src/app/options_response_for_store.md)
- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)