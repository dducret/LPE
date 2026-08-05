---
type: Rust Function
title: notification_watermark_folder_marker
resource: crates/lpe-exchange/src/service/ews/notifications.rs#L440-L454
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response
---

# Signature

`pub(in crate::service) fn notification_watermark_folder_marker(watermark: &str) -> Option<String>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [get_events_status_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/notifications/get_events_status_response.md)