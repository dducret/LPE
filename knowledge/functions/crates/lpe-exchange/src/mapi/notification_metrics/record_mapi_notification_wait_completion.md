---
type: Rust Function
title: record_mapi_notification_wait_completion
resource: crates/lpe-exchange/src/mapi/notification_metrics.rs#L30-L50
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
---

# Signature

`pub(crate) fn record_mapi_notification_wait_completion( outcome: MapiNotificationWaitOutcome, elapsed: Duration, )`

# Called by

- [notification_metrics_record_wait_completion_and_new_mail_delivery](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery.md)
- [run_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)