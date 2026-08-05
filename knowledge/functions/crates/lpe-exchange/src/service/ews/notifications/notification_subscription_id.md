---
type: Rust Function
title: notification_subscription_id
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L401-L421
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription
---

# Signature

`pub(in crate::service) fn notification_subscription_id(account_id: Uuid, request: &str) -> String`

# Calls

- [requested_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_folder_ids.md)
- [requested_distinguished_folder_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_distinguished_folder_id.md)

# Called by

- [get_events](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events.md)
- [get_streaming_events](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events.md)
- [register_pull_subscription](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/register_pull_subscription.md)