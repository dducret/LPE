---
type: Rust Method
title: durable_events_response
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L66-L156
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_sequence
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_folder_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_message_id
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_cursor
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_kind
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_queued_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_status_response
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events
  - functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events
---

# Signature

`pub(in crate::service) async fn durable_events_response( &self, operation: &str, principal: &AccountPrincipal, subscription_id: &str, previous_watermark: &str, ) -> Result<String>`

# Calls

- [notification_watermark_sequence](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/notification_watermark_sequence.md)
- [poll_mapi_notifications](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [canonical_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_folder_id.md)
- [canonical_message_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/canonical_message_id.md)
- [change_cursor](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_cursor.md)
- [change_kind](../../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/change_kind.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [get_streaming_events_queued_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_queued_response.md)
- [get_events_queued_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_queued_response.md)
- [get_streaming_events_status_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_streaming_events_status_response.md)
- [get_events_status_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response.md)

# Called by

- [get_events](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_events.md)
- [get_streaming_events](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/ExchangeService/get_streaming_events.md)