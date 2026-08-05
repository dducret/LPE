---
type: Rust Function
title: record_mapi_new_mail_notification_deliveries
resource: crates/lpe-exchange/src/mapi/notification_metrics.rs#L52-L54
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery
---

# Signature

`pub(crate) fn record_mapi_new_mail_notification_deliveries(delivery_count: usize)`

# Called by

- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [notification_metrics_record_wait_completion_and_new_mail_delivery](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery.md)