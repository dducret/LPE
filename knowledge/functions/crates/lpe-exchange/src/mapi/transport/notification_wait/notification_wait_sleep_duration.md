---
type: Rust Function
title: notification_wait_sleep_duration
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L259-L267
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
---

# Signature

`pub(super) fn notification_wait_sleep_duration( now: tokio::time::Instant, deadline: tokio::time::Instant, next_pending_at: tokio::time::Instant, ) -> Duration`

# Called by

- [run_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)