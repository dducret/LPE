---
type: Rust Function
title: notification_metrics_record_wait_completion_and_new_mail_delivery
resource: crates/lpe-exchange/src/mapi/notification_metrics.rs#L76-L91
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/mapi_notification_metrics
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_notification_wait_completion
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_new_mail_notification_deliveries
---

# Signature

`fn notification_metrics_record_wait_completion_and_new_mail_delivery()`

# Calls

- [mapi_notification_metrics](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/mapi_notification_metrics.md)
- [record_mapi_notification_wait_completion](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_notification_wait_completion.md)
- [record_mapi_new_mail_notification_deliveries](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_new_mail_notification_deliveries.md)