---
type: Rust Method
title: register_pull_subscription
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L171-L192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/subscribe
---

# Signature

`async fn register_pull_subscription( &self, principal: &AccountPrincipal, request: &str, ) -> Result<(String, String)>`

# Calls

- [notification_subscription_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_subscription_id.md)
- [notification_request_folder_marker](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/notification_request_folder_marker.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [fetch_mapi_notification_cursor](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_notification_cursor.md)
- [notification_watermark](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark.md)

# Called by

- [subscribe](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/subscribe.md)