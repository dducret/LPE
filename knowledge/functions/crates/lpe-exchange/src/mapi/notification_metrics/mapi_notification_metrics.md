---
type: Rust Function
title: mapi_notification_metrics
resource: crates/lpe-exchange/src/mapi/notification_metrics.rs#L56-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/load
  called_by:
  - functions/crates/lpe-admin-api/src/observability/render_metrics
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery
---

# Signature

`pub fn mapi_notification_metrics() -> MapiNotificationMetrics`

# Calls

- [load](../../../../../../functions/LPE-CT/web/app/load.md)

# Called by

- [render_metrics](../../../../../../functions/crates/lpe-admin-api/src/observability/render_metrics.md)
- [notification_metrics_record_wait_completion_and_new_mail_delivery](../../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery.md)