---
type: Rust Function
title: notification_wait_final_frame
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L302-L322
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait
---

# Signature

`fn notification_wait_final_frame( response_code: u16, body: &[u8], started_at: std::time::Instant, start_time: std::time::SystemTime, ) -> Bytes`

# Called by

- [complete_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)