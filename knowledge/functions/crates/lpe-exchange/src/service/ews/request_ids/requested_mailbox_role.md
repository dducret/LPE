---
type: Rust Function
title: requested_mailbox_role
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L139-L141
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kinds
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker
---

# Signature

`pub(in crate::service) fn requested_mailbox_role(request: &str) -> Option<&'static str>`

# Calls

- [requested_distinguished_folder_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id.md)

# Called by

- [requested_mailbox_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/requested_mailbox_folder_ids.md)
- [requested_folder_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)
- [requested_folder_kinds](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kinds.md)
- [notification_request_folder_marker](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker.md)