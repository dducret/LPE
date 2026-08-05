---
type: Rust Module
title: notification_metrics
resource: crates/lpe-exchange/src/mapi/notification_metrics.rs#L1-L92
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-sync-atomic-atomicu64-ordering-time-duration
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiNotificationMetrics](../../../../../classes/crates/lpe-exchange/src/mapi/notification_metrics/MapiNotificationMetrics.md)
- [MapiNotificationWaitOutcome](../../../../../classes/crates/lpe-exchange/src/mapi/notification_metrics/MapiNotificationWaitOutcome.md)
- [record_mapi_notification_wait_completion](../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_notification_wait_completion.md)
- [record_mapi_new_mail_notification_deliveries](../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/record_mapi_new_mail_notification_deliveries.md)
- [mapi_notification_metrics](../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/mapi_notification_metrics.md)
- [notification_metrics_record_wait_completion_and_new_mail_delivery](../../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/notification_metrics_record_wait_completion_and_new_mail_delivery.md)

# Imports

- `std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
}`
- `super::*`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)